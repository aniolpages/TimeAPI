mod ntp;

use axum::extract::State;
use axum::http::{HeaderValue, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router, routing::get};
use serde::Serialize;
use std::env;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tower_http::cors::{Any, CorsLayer};
use tower_http::set_header::SetResponseHeaderLayer;

#[derive(Clone)]
struct AppState {
    clock: Arc<RwLock<Option<Clock>>>,
    max_age: Duration,
}

struct Clock {
    anchor: Instant,
    unix_ms: f64,
    target_ms: f64,
    last_sync: Instant,
    uncertainty_ms: f64,
    server: String,
}

impl Clock {
    fn now(&self, now: Instant) -> f64 {
        let elapsed = now.duration_since(self.anchor).as_secs_f64() * 1000.0;
        let correction = (self.target_ms - self.unix_ms).clamp(-elapsed / 1000.0, elapsed / 1000.0);
        self.unix_ms + elapsed + correction
    }
}

#[derive(Serialize)]
struct TimeResponse {
    unix_ms: u64,
    synchronized: bool,
    ntp_server: String,
    uncertainty_ms: f64,
    sync_age_ms: u64,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: &'static str,
}

async fn now(State(state): State<AppState>) -> Response {
    let clock = state.clock.read().unwrap();
    let instant = Instant::now();
    let Some(clock) = clock.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "NTP not synchronized",
            }),
        )
            .into_response();
    };
    let age = instant.duration_since(clock.last_sync);
    if age > state.max_age {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "NTP synchronization expired",
            }),
        )
            .into_response();
    }
    let unix_ms = clock.now(instant);
    let target = clock.target_ms + instant.duration_since(clock.anchor).as_secs_f64() * 1000.0;
    Json(TimeResponse {
        unix_ms: unix_ms.floor() as u64,
        synchronized: true,
        ntp_server: clock.server.clone(),
        uncertainty_ms: clock.uncertainty_ms + age.as_secs_f64() * 0.05 + (target - unix_ms).abs(),
        sync_age_ms: age.as_millis() as u64,
    })
    .into_response()
}

fn app(state: AppState) -> Router {
    Router::new()
        .route("/api/now", get(now))
        .route("/health", get(now))
        .with_state(state)
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods([Method::GET, Method::HEAD, Method::OPTIONS])
                .allow_headers(Any)
                .expose_headers([header::CACHE_CONTROL, header::AGE]),
        )
        .layer(SetResponseHeaderLayer::overriding(
            header::CACHE_CONTROL,
            HeaderValue::from_static("no-store, no-cache, max-age=0, must-revalidate"),
        ))
        .layer(SetResponseHeaderLayer::overriding(
            header::PRAGMA,
            HeaderValue::from_static("no-cache"),
        ))
        .layer(SetResponseHeaderLayer::overriding(
            header::EXPIRES,
            HeaderValue::from_static("0"),
        ))
}

async fn synchronize(state: AppState, servers: Vec<String>, poll: Duration) {
    loop {
        let mut samples = Vec::new();
        for server in &servers {
            match ntp::query(server).await {
                Ok(sample) => samples.push(sample),
                Err(error) => eprintln!("NTP {server}: {error}"),
            }
        }
        let success = !samples.is_empty();
        if let Some(sample) = samples
            .into_iter()
            .min_by(|a, b| a.delay_ms.total_cmp(&b.delay_ms))
        {
            let instant = Instant::now();
            let target_ms =
                sample.unix_ms + instant.duration_since(sample.received).as_secs_f64() * 1000.0;
            let mut clock = state.clock.write().unwrap();
            let current = clock
                .as_ref()
                .filter(|c| instant.duration_since(c.last_sync) <= state.max_age)
                .map(|c| c.now(instant))
                .unwrap_or(target_ms);
            let unix_ms = if (target_ms - current).abs() > 100.0 {
                target_ms
            } else {
                current
            };
            eprintln!(
                "NTP synchronized: {} delay={:.3}ms uncertainty={:.3}ms correction={:.3}ms",
                sample.server,
                sample.delay_ms,
                sample.uncertainty_ms,
                target_ms - current
            );
            *clock = Some(Clock {
                anchor: instant,
                unix_ms,
                target_ms,
                last_sync: instant,
                uncertainty_ms: sample.uncertainty_ms,
                server: sample.server,
            });
        }
        tokio::time::sleep(if success {
            poll
        } else {
            Duration::from_secs(15)
        })
        .await;
    }
}

fn seconds(
    name: &str,
    default: u64,
    min: u64,
    max: u64,
) -> Result<Duration, Box<dyn std::error::Error>> {
    let value = env::var(name)
        .map(|v| v.parse::<u64>())
        .unwrap_or(Ok(default))?;
    if !(min..=max).contains(&value) {
        return Err(format!("{name} must be between {min} and {max}").into());
    }
    Ok(Duration::from_secs(value))
}

async fn shutdown() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let servers: Vec<String> = env::var("NTP_SERVERS")
        .unwrap_or_else(|_| {
            "0.europe.pool.ntp.org:123,1.europe.pool.ntp.org:123,2.europe.pool.ntp.org:123"
                .to_owned()
        })
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect();
    if servers.is_empty() {
        return Err("NTP_SERVERS is empty".into());
    }
    let poll = seconds("NTP_POLL_SECONDS", 64, 16, 3600)?;
    let max_age = seconds("NTP_MAX_AGE_SECONDS", 300, 60, 86400)?;
    if max_age < poll {
        return Err("NTP_MAX_AGE_SECONDS must be at least NTP_POLL_SECONDS".into());
    }
    let state = AppState {
        clock: Arc::new(RwLock::new(None)),
        max_age,
    };
    let listener =
        TcpListener::bind(env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:3123".to_owned()))
            .await?;
    eprintln!("TimeAPI listening on {}", listener.local_addr()?);
    let sync = tokio::spawn(synchronize(state.clone(), servers, poll));
    let result = axum::serve(listener, app(state))
        .with_graceful_shutdown(shutdown())
        .await;
    sync.abort();
    result?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use tower::ServiceExt;

    fn state(age: Duration) -> AppState {
        let instant = Instant::now();
        AppState {
            clock: Arc::new(RwLock::new(Some(Clock {
                anchor: instant,
                unix_ms: 1_790_990_000_000.0,
                target_ms: 1_790_990_000_000.0,
                last_sync: instant - age,
                uncertainty_ms: 1.0,
                server: "test:123".to_owned(),
            }))),
            max_age: Duration::from_secs(300),
        }
    }

    #[tokio::test]
    async fn returns_uncached_cors_time_for_any_origin() {
        let response = app(state(Duration::ZERO))
            .oneshot(
                Request::builder()
                    .uri("/api/now?_=unique")
                    .header("origin", "https://any.example")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["access-control-allow-origin"], "*");
        assert!(
            response.headers()["cache-control"]
                .to_str()
                .unwrap()
                .contains("no-store")
        );
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["unix_ms"].as_u64().unwrap() >= 1_790_990_000_000);
        assert_eq!(json["synchronized"], true);
    }

    #[tokio::test]
    async fn supports_cache_control_cors_preflight() {
        let response = app(state(Duration::ZERO))
            .oneshot(
                Request::builder()
                    .method("OPTIONS")
                    .uri("/api/now")
                    .header("origin", "https://any.example")
                    .header("access-control-request-method", "GET")
                    .header("access-control-request-headers", "cache-control")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(response.status().is_success());
        assert_eq!(response.headers()["access-control-allow-origin"], "*");
        assert_eq!(response.headers()["access-control-allow-headers"], "*");
    }

    #[tokio::test]
    async fn refuses_unsynchronized_or_expired_time() {
        let empty = AppState {
            clock: Arc::new(RwLock::new(None)),
            max_age: Duration::from_secs(300),
        };
        for state in [empty, state(Duration::from_secs(301))] {
            let response = app(state)
                .oneshot(
                    Request::builder()
                        .uri("/api/now")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(response.headers()["access-control-allow-origin"], "*");
        }
    }

    #[test]
    fn disciplines_clock_monotonically_in_both_directions() {
        let instant = Instant::now();
        let mut clock = Clock {
            anchor: instant,
            unix_ms: 1000.0,
            target_ms: 1010.0,
            last_sync: instant,
            uncertainty_ms: 1.0,
            server: "test".to_owned(),
        };
        assert_eq!(clock.now(instant + Duration::from_secs(5)), 6005.0);
        assert_eq!(clock.now(instant + Duration::from_secs(20)), 21010.0);
        clock.target_ms = 990.0;
        assert_eq!(clock.now(instant + Duration::from_secs(5)), 5995.0);
        assert_eq!(clock.now(instant + Duration::from_secs(20)), 20990.0);
    }
}
