//! Standalone sig-trace web UI + search HTTP server. Carved out of
//! `central-service/src/ws.rs` so it can run on any box that hosts a bot
//! writing sig-trace files — most relevant for **LT**, which writes
//! sig-trace locally but has no central-service co-located to serve them.
//!
//! Three endpoints (open to any IP — same posture as the central handlers):
//!   GET /                          — embedded HTML UI (same as FR's)
//!   GET /sig/:sig                  — grep trace files for `sig`
//!   GET /time/HH:MM:SS?window=N    — return lines within ±N seconds of t
//!                          ?filter=triggers (default) | all
//!
//! Env:
//!   SIG_TRACE_DIR  — directory containing `sig_trace.jsonl` +
//!                    rotated `sig_trace.{1..9}.jsonl`. Default
//!                    `/home/ubuntu/sig_trace`.
//!   SIG_TRACE_BIND — bind address. Default `0.0.0.0:9001`.
//!
//! Run (on the LT box):
//!   ~/supra-stop-take/target/release/sig_trace_server &
//! Then visit `http://<lt-box-ip>:9001/` in the browser.

use axum::{
    body::Body,
    extract::{Path, Query},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use tokio::net::TcpListener;

const SIG_UI_HTML: &str = include_str!("../sig_ui.html");

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let bind = std::env::var("SIG_TRACE_BIND").unwrap_or_else(|_| "0.0.0.0:9001".to_owned());
    let dir = std::env::var("SIG_TRACE_DIR").unwrap_or_else(|_| "/home/ubuntu/sig_trace".to_owned());
    tracing::info!(bind = %bind, sig_trace_dir = %dir, "sig_trace_server starting");

    let app = Router::new()
        .route("/", get(serve_sig_ui))
        .route("/sig/:sig", get(serve_sig_search))
        .route("/time/:time", get(serve_time_search));

    let listener = TcpListener::bind(&bind)
        .await
        .map_err(|e| anyhow::anyhow!("bind {bind}: {e}"))?;
    axum::serve(listener, app).await?;
    Ok(())
}

// =============================================================================
// Handlers — verbatim port from central-service/src/ws.rs, kept self-
// contained so this binary doesn't pull in the rest of the central-service
// runtime (broadcast channels, Mongo, etc.).
// =============================================================================

async fn serve_sig_ui() -> Response {
    Response::builder()
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .body(Body::from(SIG_UI_HTML))
        .expect("build sig_ui body")
}

async fn serve_sig_search(Path(sig): Path<String>) -> Response {
    if sig.len() < 8 {
        return (StatusCode::BAD_REQUEST, "sig must be at least 8 chars").into_response();
    }
    let dir = std::env::var("SIG_TRACE_DIR").unwrap_or_else(|_| "/home/ubuntu/sig_trace".to_owned());
    let dir = std::path::PathBuf::from(dir);
    if !dir.exists() {
        return (StatusCode::NOT_FOUND, "sig_trace dir not found").into_response();
    }

    // Files in oldest-first order: .9, .8, …, .1, then active. Lets us
    // return events oldest-first by reading rolled descending then active.
    let mut files: Vec<std::path::PathBuf> = Vec::new();
    for i in (1..10).rev() {
        let p = dir.join(format!("sig_trace.{}.jsonl", i));
        if p.exists() {
            files.push(p);
        }
    }
    let active = dir.join("sig_trace.jsonl");
    if active.exists() {
        files.push(active);
    }
    if files.is_empty() {
        return (StatusCode::NOT_FOUND, "no trace files yet").into_response();
    }

    const MAX_BODY_BYTES: usize = 10 * 1024 * 1024;
    let needle = sig.clone();
    let body_result = tokio::task::spawn_blocking(move || -> std::io::Result<String> {
        use std::io::{BufRead, BufReader};
        let mut out = String::new();
        for path in files {
            let f = std::fs::File::open(&path)?;
            let reader = BufReader::new(f);
            for line in reader.lines().map_while(Result::ok) {
                if line.contains(&needle) {
                    out.push_str(&line);
                    out.push('\n');
                    if out.len() >= MAX_BODY_BYTES {
                        out.push_str("...[truncated at 10 MB]\n");
                        return Ok(out);
                    }
                }
            }
        }
        Ok(out)
    })
    .await;

    match body_result {
        Ok(Ok(body)) => {
            if body.is_empty() {
                (StatusCode::NOT_FOUND, format!("no matches for sig={sig}")).into_response()
            } else {
                Response::builder()
                    .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
                    .body(Body::from(body))
                    .expect("build sig response")
            }
        }
        Ok(Err(e)) => {
            tracing::error!("sig search io error: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, format!("io: {e}")).into_response()
        }
        Err(e) => {
            tracing::error!("sig search join error: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, "join failed").into_response()
        }
    }
}

#[derive(Debug, serde::Deserialize, Default)]
struct TimeQuery {
    window: Option<u64>,
    filter: Option<String>,
}

async fn serve_time_search(
    Path(time_str): Path<String>,
    Query(q): Query<TimeQuery>,
) -> Response {
    let parts: Vec<&str> = time_str.split(':').collect();
    let (h, m, s) = match parts.as_slice() {
        [h, m] => match (h.parse::<u32>(), m.parse::<u32>()) {
            (Ok(h), Ok(m)) => (h, m, 0u32),
            _ => return (StatusCode::BAD_REQUEST, "expect HH:MM or HH:MM:SS").into_response(),
        },
        [h, m, s] => match (h.parse::<u32>(), m.parse::<u32>(), s.parse::<u32>()) {
            (Ok(h), Ok(m), Ok(s)) => (h, m, s),
            _ => return (StatusCode::BAD_REQUEST, "expect HH:MM or HH:MM:SS").into_response(),
        },
        _ => return (StatusCode::BAD_REQUEST, "expect HH:MM or HH:MM:SS").into_response(),
    };
    if h >= 24 || m >= 60 || s >= 60 {
        return (StatusCode::BAD_REQUEST, "out of range").into_response();
    }

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let day_ms = 86_400_000u64;
    let today_midnight_ms = (now_ms / day_ms) * day_ms;
    let target_ms = today_midnight_ms
        + (h as u64 * 3_600_000)
        + (m as u64 * 60_000)
        + (s as u64 * 1_000);

    let window_secs = q.window.unwrap_or(6).min(300);
    let lo = target_ms.saturating_sub(window_secs * 1000);
    let hi = target_ms.saturating_add(window_secs * 1000);

    let triggers_only = !matches!(q.filter.as_deref(), Some("all"));

    let dir = std::env::var("SIG_TRACE_DIR").unwrap_or_else(|_| "/home/ubuntu/sig_trace".to_owned());
    let dir = std::path::PathBuf::from(dir);
    if !dir.exists() {
        return (StatusCode::NOT_FOUND, "sig_trace dir not found").into_response();
    }

    let mut files: Vec<std::path::PathBuf> = Vec::new();
    for i in (1..10).rev() {
        let p = dir.join(format!("sig_trace.{}.jsonl", i));
        if p.exists() {
            files.push(p);
        }
    }
    let active = dir.join("sig_trace.jsonl");
    if active.exists() {
        files.push(active);
    }
    if files.is_empty() {
        return (StatusCode::NOT_FOUND, "no trace files yet").into_response();
    }

    const MAX_BODY_BYTES: usize = 10 * 1024 * 1024;
    let body_result = tokio::task::spawn_blocking(move || -> std::io::Result<String> {
        use std::io::{BufRead, BufReader};
        let mut out = String::new();
        for path in files {
            let f = std::fs::File::open(&path)?;
            let reader = BufReader::new(f);
            for line in reader.lines().map_while(Result::ok) {
                let Some(rest) = line.strip_prefix("ts_ms=") else {
                    continue;
                };
                let Some(sp) = rest.find(' ') else { continue };
                let Ok(ts) = rest[..sp].parse::<u64>() else {
                    continue;
                };
                if ts >= lo && ts <= hi {
                    if triggers_only && !line.contains("trigger") {
                        continue;
                    }
                    out.push_str(&line);
                    out.push('\n');
                    if out.len() >= MAX_BODY_BYTES {
                        out.push_str("...[truncated at 10 MB]\n");
                        return Ok(out);
                    }
                }
            }
        }
        Ok(out)
    })
    .await;

    match body_result {
        Ok(Ok(body)) => {
            if body.is_empty() {
                (
                    StatusCode::NOT_FOUND,
                    format!(
                        "no lines in [{} \u{00b1} {}s] (target_ms={target_ms})",
                        time_str, window_secs
                    ),
                )
                    .into_response()
            } else {
                Response::builder()
                    .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
                    .body(Body::from(body))
                    .expect("build time response")
            }
        }
        Ok(Err(e)) => {
            tracing::error!("time search io error: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, format!("io: {e}")).into_response()
        }
        Err(e) => {
            tracing::error!("time search join error: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, "join failed").into_response()
        }
    }
}
