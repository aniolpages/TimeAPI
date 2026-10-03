mod ntp;

use axum::Extension;
use axum::extract::Request;
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{State, WebSocketUpgrade};
use axum::http::{HeaderValue, Method, StatusCode, header};
use axum::middleware::{self, Next};
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
    rate: f64,
}

impl Clock {
    fn now(&self, now: Instant) -> f64 {
        let elapsed = if now >= self.anchor {
            now.duration_since(self.anchor).as_secs_f64() * 1000.0
        } else {
            -self.anchor.duration_since(now).as_secs_f64() * 1000.0
        };
        let correction = (self.target_ms - self.unix_ms)
            .clamp(-elapsed.max(0.0) / 1000.0, elapsed.max(0.0) / 1000.0);
        self.unix_ms + elapsed * self.rate + correction
    }
}

#[derive(Serialize)]
struct TimeResponse {
    unix_ms: f64,
    receive_unix_ms: f64,
    transmit_unix_ms: f64,
    protocol_version: u8,
    drift_ppm: f64,
    synchronized: bool,
    ntp_server: String,
    uncertainty_ms: f64,
    sync_age_ms: u64,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: &'static str,
}

fn snapshot(state: &AppState, received: Instant) -> Result<TimeResponse, &'static str> {
    let lock = state.clock.read().unwrap();
    let instant = Instant::now();
    let clock = lock.as_ref().ok_or("NTP not synchronized")?;
    let age = instant.duration_since(clock.last_sync);
    if age > state.max_age {
        return Err("NTP synchronization expired");
    }
    let unix_ms = clock.now(instant);
    let target =
        clock.target_ms + instant.duration_since(clock.anchor).as_secs_f64() * 1000.0 * clock.rate;
    Ok(TimeResponse {
        unix_ms,
        receive_unix_ms: clock.now(received),
        transmit_unix_ms: unix_ms,
        protocol_version: 1,
        drift_ppm: (clock.rate - 1.0) * 1e6,
        synchronized: true,
        ntp_server: clock.server.clone(),
        uncertainty_ms: clock.uncertainty_ms + age.as_secs_f64() * 0.05 + (target - unix_ms).abs(),
        sync_age_ms: age.as_millis() as u64,
    })
}

async fn stamp(mut request: Request, next: Next) -> Response {
    request.extensions_mut().insert(Instant::now());
    next.run(request).await
}

async fn now(State(state): State<AppState>, Extension(received): Extension<Instant>) -> Response {
    match snapshot(&state, received) {
        Ok(time) => Json(time).into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse { error }),
        )
            .into_response(),
    }
}

fn frame(id: f64, time: Option<TimeResponse>) -> Vec<u8> {
    let values = if let Some(t) = time {
        [
            id,
            t.receive_unix_ms,
            t.transmit_unix_ms,
            t.uncertainty_ms,
            t.sync_age_ms as f64,
            1.0,
        ]
    } else {
        [id, 0.0, 0.0, 0.0, 0.0, 0.0]
    };
    values.into_iter().flat_map(f64::to_le_bytes).collect()
}

async fn websocket(mut socket: WebSocket, state: AppState) {
    let mut window = Instant::now();
    let mut count = 0;
    loop {
        let Ok(Some(Ok(message))) =
            tokio::time::timeout(Duration::from_secs(90), socket.recv()).await
        else {
            break;
        };
        let received = Instant::now();
        if received.duration_since(window) >= Duration::from_secs(1) {
            window = received;
            count = 0;
        }
        count += 1;
        if count > 32 {
            break;
        }
        match message {
            Message::Binary(bytes) if bytes.len() == 8 => {
                let id = f64::from_le_bytes(bytes[..].try_into().unwrap());
                if !id.is_finite() {
                    break;
                }
                let data = frame(id, snapshot(&state, received).ok());
                if socket.send(Message::Binary(data.into())).await.is_err() {
                    break;
                }
            }
            Message::Ping(bytes) => {
                if socket.send(Message::Pong(bytes)).await.is_err() {
                    break;
                }
            }
            Message::Close(_) => break,
            _ => break,
        }
    }
    let _ = socket.send(Message::Close(None)).await;
}

async fn sync_socket(State(state): State<AppState>, upgrade: WebSocketUpgrade) -> Response {
    upgrade
        .max_message_size(8)
        .max_frame_size(128)
        .on_upgrade(move |socket| websocket(socket, state))
}

fn app(state: AppState) -> Router {
    Router::new()
        .route("/api/now", get(now))
        .route("/api/sync", get(sync_socket))
        .route("/health", get(now))
        .with_state(state)
        .layer(middleware::from_fn(stamp))
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
    let mut history: Vec<(Instant, f64)> = Vec::new();
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
            if let Some((previous, unix)) = history.last() {
                let elapsed = instant.duration_since(*previous).as_secs_f64() * 1000.0;
                if (target_ms - unix - elapsed).abs() > 20.0 {
                    history.clear();
                }
            }
            history.push((instant, target_ms));
            history.retain(|(t, _)| instant.duration_since(*t) < Duration::from_secs(300));
            if history.len() > 150 {
                history.remove(0);
            }
            let rate = estimate_rate(&history);
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
                rate,
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

fn estimate_rate(history: &[(Instant, f64)]) -> f64 {
    if history.len() < 6 {
        return 1.0;
    }
    let (base, unix) = history[0];
    let span = history.last().unwrap().0.duration_since(base).as_secs_f64();
    if span < 30.0 {
        return 1.0;
    }
    let points: Vec<_> = history
        .iter()
        .map(|(t, u)| {
            let x = t.duration_since(base).as_secs_f64() * 1000.0;
            (x, u - unix - x)
        })
        .collect();
    fn fit(points: &[(f64, f64)]) -> (f64, f64, f64) {
        let n = points.len() as f64;
        let x = points.iter().map(|p| p.0).sum::<f64>() / n;
        let y = points.iter().map(|p| p.1).sum::<f64>() / n;
        let xx = points.iter().map(|p| (p.0 - x).powi(2)).sum::<f64>();
        let xy = points.iter().map(|p| (p.0 - x) * (p.1 - y)).sum::<f64>();
        (x, y, (xy / xx).clamp(-0.0002, 0.0002))
    }
    let (x, y, slope) = fit(&points);
    let mut residuals: Vec<f64> = points
        .iter()
        .map(|p| (p.1 - y - slope * (p.0 - x)).abs())
        .collect();
    residuals.sort_by(f64::total_cmp);
    let threshold = (residuals[residuals.len() / 2] * 4.0).max(0.1);
    let good: Vec<_> = points
        .iter()
        .copied()
        .filter(|p| (p.1 - y - slope * (p.0 - x)).abs() <= threshold)
        .collect();
    1.0 + if good.len() >= 6 { fit(&good).2 } else { slope }
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
        .unwrap_or_else(|_| "127.0.0.1:123".to_owned())
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect();
    if servers.is_empty() {
        return Err("NTP_SERVERS is empty".into());
    }
    let poll = seconds("NTP_POLL_SECONDS", 2, 1, 3600)?;
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
                rate: 1.0,
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
        assert!(json["unix_ms"].as_f64().unwrap() >= 1_790_990_000_000.0);
        assert_eq!(json["synchronized"], true);
        assert_eq!(json["protocol_version"], 1);
        assert!(
            json["transmit_unix_ms"].as_f64().unwrap() >= json["receive_unix_ms"].as_f64().unwrap()
        );
        assert_eq!(json["unix_ms"], json["transmit_unix_ms"]);
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
    fn binary_protocol_preserves_submillisecond_precision_and_reports_unsynced() {
        let instant = Instant::now();
        let state = state(Duration::ZERO);
        let bytes = frame(123.0, Some(snapshot(&state, instant).unwrap()));
        assert_eq!(bytes.len(), 48);
        let values: Vec<_> = bytes
            .as_chunks::<8>()
            .0
            .iter()
            .map(|b| f64::from_le_bytes(*b))
            .collect();
        assert_eq!(values[0], 123.0);
        assert!(values[2] >= values[1]);
        assert_eq!(values[5], 1.0);
        let unavailable = frame(123.0, None);
        assert_eq!(
            f64::from_le_bytes(unavailable[40..48].try_into().unwrap()),
            0.0
        );
    }

    #[test]
    fn learns_frequency_and_rejects_a_phase_outlier() {
        let base = Instant::now();
        let mut history: Vec<_> = (0..20)
            .map(|i| {
                (
                    base + Duration::from_secs(i * 5),
                    1000.0 + i as f64 * 5000.0 * 1.000035,
                )
            })
            .collect();
        history[8].1 += 50.0;
        assert!((estimate_rate(&history) - 1.000035).abs() < 1e-9);
        assert_eq!(estimate_rate(&history[..3]), 1.0);
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
            rate: 1.0,
        };
        assert_eq!(clock.now(instant + Duration::from_secs(5)), 6005.0);
        assert_eq!(clock.now(instant + Duration::from_secs(20)), 21010.0);
        clock.target_ms = 990.0;
        assert_eq!(clock.now(instant + Duration::from_secs(5)), 5995.0);
        assert_eq!(clock.now(instant + Duration::from_secs(20)), 20990.0);
    }
}
