use std::io::{self, ErrorKind};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::{UdpSocket, lookup_host};
use tokio::time::timeout;

const EPOCH: f64 = 2_208_988_800.0;
const ERA: f64 = 4_294_967_296.0;

#[derive(Debug)]
pub struct Sample {
    pub unix_ms: f64,
    pub received: Instant,
    pub delay_ms: f64,
    pub uncertainty_ms: f64,
    pub server: String,
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, message)
}

fn encode_timestamp(unix_seconds: f64) -> [u8; 8] {
    let ntp = unix_seconds + EPOCH;
    let seconds = ntp.floor().rem_euclid(ERA) as u32;
    let fraction = (ntp.fract() * ERA) as u32;
    let mut bytes = [0; 8];
    bytes[..4].copy_from_slice(&seconds.to_be_bytes());
    bytes[4..].copy_from_slice(&fraction.to_be_bytes());
    bytes
}

fn decode_timestamp(bytes: &[u8], near_unix: f64) -> f64 {
    let seconds = u32::from_be_bytes(bytes[..4].try_into().unwrap()) as f64;
    let fraction = u32::from_be_bytes(bytes[4..8].try_into().unwrap()) as f64 / ERA;
    let era = ((near_unix + EPOCH - seconds) / ERA).round();
    seconds + era * ERA + fraction - EPOCH
}

fn decode_response(
    packet: &[u8],
    origin: &[u8; 8],
    t1: f64,
    t4: f64,
) -> io::Result<(f64, f64, f64)> {
    if packet.len() < 48 {
        return Err(invalid("truncated NTP response"));
    }
    let version = (packet[0] >> 3) & 7;
    if packet[0] >> 6 == 3 || !matches!(version, 3 | 4) || packet[0] & 7 != 4 {
        return Err(invalid("invalid NTP mode, version or leap indicator"));
    }
    if !(1..=15).contains(&packet[1]) {
        return Err(invalid("unsynchronized NTP server or kiss-of-death"));
    }
    if packet[24..32] != *origin {
        return Err(invalid("NTP origin mismatch"));
    }
    if packet[32..40] == [0; 8] || packet[40..48] == [0; 8] {
        return Err(invalid("missing NTP timestamps"));
    }
    let t2 = decode_timestamp(&packet[32..40], t1);
    let t3 = decode_timestamp(&packet[40..48], t1);
    let processing = t3 - t2;
    let delay = (t4 - t1) - processing;
    if processing < 0.0 || delay < -0.001 || delay > 2.0 {
        return Err(invalid("invalid NTP delay"));
    }
    let offset = ((t2 - t1) + (t3 - t4)) / 2.0;
    let root_delay = i32::from_be_bytes(packet[4..8].try_into().unwrap()) as f64 / 65_536.0;
    let dispersion = u32::from_be_bytes(packet[8..12].try_into().unwrap()) as f64 / 65_536.0;
    let uncertainty = delay.max(0.0) / 2.0 + root_delay.max(0.0) / 2.0 + dispersion;
    if uncertainty > 1.0 {
        return Err(invalid("NTP uncertainty exceeds one second"));
    }
    Ok((
        (t4 + offset) * 1000.0,
        delay.max(0.0) * 1000.0,
        uncertainty * 1000.0,
    ))
}

pub async fn query(server: &str) -> io::Result<Sample> {
    timeout(Duration::from_secs(3), async {
        let addresses: Vec<_> = lookup_host(server).await?.collect();
        let mut last_error = io::Error::new(ErrorKind::NotFound, "no NTP addresses");
        for address in addresses.into_iter().take(3) {
            let bind = if address.is_ipv4() {
                "0.0.0.0:0"
            } else {
                "[::]:0"
            };
            let result = async {
                let socket = UdpSocket::bind(bind).await?;
                socket.connect(address).await?;
                let t1 = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|_| invalid("local clock predates Unix epoch"))?
                    .as_secs_f64();
                let sent = Instant::now();
                let origin = encode_timestamp(t1);
                let mut request = [0; 48];
                request[0] = 0x23;
                request[40..48].copy_from_slice(&origin);
                socket.send(&request).await?;
                let mut packet = [0; 512];
                let length =
                    timeout(Duration::from_millis(800), socket.recv(&mut packet)).await??;
                let received = Instant::now();
                let t4 = t1 + received.duration_since(sent).as_secs_f64();
                let (unix_ms, delay_ms, uncertainty_ms) =
                    decode_response(&packet[..length], &origin, t1, t4)?;
                Ok(Sample {
                    unix_ms,
                    received,
                    delay_ms,
                    uncertainty_ms,
                    server: server.to_owned(),
                })
            }
            .await;
            match result {
                Ok(sample) => return Ok(sample),
                Err(error) => last_error = error,
            }
        }
        Err(last_error)
    })
    .await?
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(t1: f64, t2: f64, t3: f64) -> [u8; 48] {
        let mut packet = [0; 48];
        packet[0] = 0x24;
        packet[1] = 2;
        packet[24..32].copy_from_slice(&encode_timestamp(t1));
        packet[32..40].copy_from_slice(&encode_timestamp(t2));
        packet[40..48].copy_from_slice(&encode_timestamp(t3));
        packet
    }

    #[test]
    fn four_timestamp_offset_accounts_for_server_processing() {
        let t1 = 1_790_990_000.0;
        let packet = response(t1, t1 + 0.030, t1 + 0.032);
        let (unix, delay, uncertainty) =
            decode_response(&packet, &encode_timestamp(t1), t1, t1 + 0.042).unwrap();
        assert!((unix - (t1 + 0.052) * 1000.0).abs() < 0.001);
        assert!((delay - 40.0).abs() < 0.001);
        assert!((uncertainty - 20.0).abs() < 0.001);
    }

    #[test]
    fn timestamp_roundtrip_handles_2036_rollover() {
        for unix in [1_790_990_000.125, 2_085_978_496.25, 2_300_000_000.75] {
            assert!((decode_timestamp(&encode_timestamp(unix), unix) - unix).abs() < 0.000001);
        }
    }

    #[test]
    fn rejects_unsynchronized_malformed_and_unrelated_packets() {
        let t1 = 1_790_990_000.0;
        let origin = encode_timestamp(t1);
        let packet = response(t1, t1 + 0.01, t1 + 0.011);
        assert!(decode_response(&packet[..47], &origin, t1, t1 + 0.02).is_err());
        for (index, value) in [(0, 0xe4), (0, 0x23), (1, 0), (1, 16), (24, 255)] {
            let mut bad = packet;
            bad[index] = value;
            assert!(decode_response(&bad, &origin, t1, t1 + 0.02).is_err());
        }
        assert!(decode_response(&packet, &origin, t1, t1 + 5.0).is_err());
    }

    #[tokio::test]
    async fn queries_udp_server_and_matches_request_origin() {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut request = [0; 48];
            let (_, peer) = socket.recv_from(&mut request).await.unwrap();
            let t1 = decode_timestamp(&request[40..48], 1_790_990_000.0);
            let packet = response(t1, t1 + 0.010, t1 + 0.010);
            socket.send_to(&packet, peer).await.unwrap();
        });
        let sample = query(&address.to_string()).await.unwrap();
        assert_eq!(sample.server, address.to_string());
        assert!(sample.delay_ms.is_finite() && sample.delay_ms >= 0.0);
        assert!((sample.uncertainty_ms - sample.delay_ms / 2.0).abs() < 0.001);
        assert!(sample.unix_ms.is_finite());
        server.await.unwrap();
    }
}
