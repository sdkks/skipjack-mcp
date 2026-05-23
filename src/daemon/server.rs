//! Connection handler for the daemon's Unix domain socket.
//!
//! Each incoming connection is handled by [`handle_connection`], which
//! reads newline-delimited JSON frames, deserializes them as [`Request`],
//! dispatches to the appropriate handler, and writes the [`Response`] back
//! as a single JSON line.
//!
//! # Framing
//!
//! The wire format is newline-delimited JSON (one message per line).
//! Frame boundaries are determined by the `\n` byte; there are no
//! content-length headers. Messages over [`MAX_MESSAGE_SIZE`] (1 MB) are
//! rejected before deserialization.
//!
//! # Connection lifecycle
//!
//! A single connection can handle multiple requests in sequence. The handler
//! loops until the client disconnects, a read error occurs, or a message
//! exceeds the size limit. After a Shutdown request the daemon signals
//! shutdown externally; the connection itself just returns the ShutdownAck.

use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use super::protocol::{Request, Response, MAX_MESSAGE_SIZE};
use crate::config::Config;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Handle a single client connection on the Unix domain socket.
///
/// Reads newline-delimited JSON requests in a loop until the client
/// disconnects or an unrecoverable framing error occurs. Each request
/// produces exactly one response.
///
/// # Errors
///
/// - Returns an error if the initial read fails (socket closed, etc.).
/// - Framing errors (message over 1 MB) result in an Error response and
///   connection close.
/// - Malformed JSON results in an Error response but the connection stays
///   open for subsequent requests.
pub async fn handle_connection(stream: UnixStream, config: Arc<Config>) -> anyhow::Result<()> {
    let (reader, mut writer) = tokio::io::split(stream);
    let mut buf_reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();

        // Read until newline, enforcing the 1 MB size limit.
        let bytes_read = read_line_limited(&mut buf_reader, &mut line, MAX_MESSAGE_SIZE).await;

        match bytes_read {
            Ok(0) => {
                // EOF — client disconnected cleanly.
                tracing::debug!("connection closed by client");
                return Ok(());
            }
            Ok(_) => {
                // Got a line; parse and dispatch.
                match serde_json::from_str::<Request>(line.trim()) {
                    Ok(request) => {
                        let response = dispatch_request(request, &config);
                        write_response(&mut writer, &response).await?;
                    }
                    Err(e) => {
                        // Malformed JSON — respond with parse error and continue.
                        tracing::warn!(error = %e, "failed to parse request JSON");
                        let response = Response::Error {
                            code: -32700,
                            message: format!("Parse error: {}", e),
                            data: None,
                        };
                        // Writing the error response may fail if the client has
                        // disconnected; log and continue.
                        if let Err(write_err) = write_response(&mut writer, &response).await {
                            tracing::debug!(error = %write_err, "failed to write parse error response");
                            return Ok(());
                        }
                    }
                }
            }
            Err(ReadError::Io(e)) => {
                // I/O error (client disconnect, etc.) — drop silently.
                tracing::debug!(error = %e, "I/O error reading from socket");
                return Ok(());
            }
            Err(ReadError::MaxSizeExceeded) => {
                // Message exceeded size limit — respond with error and close.
                tracing::warn!("request body exceeded 1 MB limit, closing connection");
                let response = Response::Error {
                    code: -32600,
                    message: "Request too large: maximum message size is 1 MB".into(),
                    data: None,
                };
                let _ = write_response(&mut writer, &response).await;
                return Ok(());
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Read a line from `reader` into `buf`, enforcing a maximum byte count.
///
/// Returns `Ok(0)` on EOF. Returns `Err(ReadError::MaxSizeExceeded)` if the line
/// (including delimiter) exceeds `max_size` bytes, or `Err(ReadError::Io(...))`
/// for I/O errors. The `buf` is cleared before reading.
async fn read_line_limited<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
    buf: &mut String,
    max_size: usize,
) -> Result<usize, ReadError> {
    buf.clear();

    // Track total bytes read including the eventual newline.
    let mut total: usize = 0;

    loop {
        let bytes_in_buffer = reader.buffer().len();
        if bytes_in_buffer == 0 {
            // Refill the internal buffer.
            reader.fill_buf().await.map_err(|e| {
                tracing::debug!(error = %e, "read error on socket");
                ReadError::Io(e)
            })?;
            if reader.buffer().is_empty() {
                return Ok(0); // EOF
            }
        }

        // Search for newline in current buffer contents.
        let buffer = reader.buffer();
        if let Some(pos) = buffer.iter().position(|&b| b == b'\n') {
            let chunk = &buffer[..pos];
            let chunk_str = std::str::from_utf8(chunk).unwrap_or("");
            buf.push_str(chunk_str);
            total += pos + 1; // include the newline

            if total > max_size + 1 {
                return Err(ReadError::MaxSizeExceeded);
            }

            // Consume past the newline.
            reader.consume(pos + 1);
            return Ok(total);
        }

        // No newline yet — check if we've exceeded the limit.
        let remaining = buffer.len();
        if total + remaining > max_size + 1 {
            return Err(ReadError::MaxSizeExceeded);
        }

        // Append current buffer contents to buf.
        let chunk_str = std::str::from_utf8(buffer).unwrap_or("");
        buf.push_str(chunk_str);
        total += remaining;
        reader.consume(remaining);
    }
}

/// Error type for `read_line_limited`, distinguishing I/O errors from
/// size-limit breaches.
#[derive(Debug)]
enum ReadError {
    MaxSizeExceeded,
    Io(std::io::Error),
}

/// Dispatch a [`Request`] to the appropriate handler and produce a [`Response`].
///
/// This function is the central routing point for all daemon requests.
/// As daemon subsystems (cache, provider manager, etc.) are implemented,
/// the placeholder stubs here are replaced with real implementations.
fn dispatch_request(request: Request, _config: &Config) -> Response {
    match request {
        Request::Search(_search_req) => Response::Error {
            code: -32601,
            message: "Search: not yet implemented".into(),
            data: None,
        },
        Request::ListProviders => Response::Error {
            code: -32601,
            message: "ListProviders: not yet implemented".into(),
            data: None,
        },
        Request::CacheStats => Response::Error {
            code: -32601,
            message: "CacheStats: not yet implemented".into(),
            data: None,
        },
        Request::ProviderStatus => Response::Error {
            code: -32601,
            message: "ProviderStatus: not yet implemented".into(),
            data: None,
        },
        Request::CacheClear { .. } => Response::Error {
            code: -32601,
            message: "CacheClear: not yet implemented".into(),
            data: None,
        },
        Request::Health => Response::Health {
            status: "ok".into(),
            uptime_secs: 0, // populated by daemon when wired up
            version: env!("CARGO_PKG_VERSION").into(),
        },
        Request::Shutdown => {
            tracing::info!("shutdown requested via socket");
            // The daemon accept loop should watch for a shutdown signal.
            // For now, just acknowledge.
            Response::ShutdownAck
        }
    }
}

/// Serialize a [`Response`] as a single JSON line and write it to the socket.
async fn write_response<W: AsyncWrite + Unpin>(
    writer: &mut W,
    response: &Response,
) -> std::io::Result<()> {
    let mut json = serde_json::to_string(response).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("serialization error: {}", e),
        )
    })?;
    json.push('\n');
    writer.write_all(json.as_bytes()).await?;
    writer.flush().await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::SearchRequest;
    use tokio::io::AsyncWriteExt;
    use tokio::net::UnixStream;

    /// Helper: spawn a daemon-side handler on a socket pair and return the
    /// client-side stream.
    async fn spawn_handler(
        config: Arc<Config>,
    ) -> (UnixStream, tokio::task::JoinHandle<anyhow::Result<()>>) {
        let (server, client) = UnixStream::pair().expect("create socket pair");
        let config_clone = Arc::clone(&config);
        let handle = tokio::spawn(async move { handle_connection(server, config_clone).await });
        (client, handle)
    }

    /// The Health request should return a valid Health response.
    #[tokio::test]
    async fn health_request_returns_ok() {
        let config = Arc::new(Config::default());
        let (mut client, _handle) = spawn_handler(config).await;

        let req_json = r#"{"type":"Health"}"#;
        client.write_all(req_json.as_bytes()).await.unwrap();
        client.write_all(b"\n").await.unwrap();

        let mut reader = BufReader::new(&mut client);
        let mut response_line = String::new();
        read_line_limited(&mut reader, &mut response_line, MAX_MESSAGE_SIZE)
            .await
            .expect("read response line");

        let response: Response =
            serde_json::from_str(response_line.trim()).expect("parse response");
        match response {
            Response::Health { status, .. } => assert_eq!(status, "ok"),
            other => panic!("expected Health response, got: {:?}", other),
        }
    }

    /// An unknown or malformed request should return an Error response.
    #[tokio::test]
    async fn malformed_request_returns_error() {
        let config = Arc::new(Config::default());
        let (mut client, _handle) = spawn_handler(config).await;

        // Send invalid JSON
        client.write_all(b"{bad json\n").await.unwrap();

        let mut reader = BufReader::new(&mut client);
        let mut response_line = String::new();
        read_line_limited(&mut reader, &mut response_line, MAX_MESSAGE_SIZE)
            .await
            .expect("read response line");

        let response: Response =
            serde_json::from_str(response_line.trim()).expect("parse response");
        match response {
            Response::Error { code, .. } => {
                assert_eq!(code, -32700, "expected parse error code");
            }
            other => panic!("expected Error response, got: {:?}", other),
        }
    }

    /// A valid Search request (unimplemented) should return an Error with -32601.
    #[tokio::test]
    async fn unimplemented_request_returns_method_not_found() {
        let config = Arc::new(Config::default());
        let (mut client, _handle) = spawn_handler(config).await;

        let request = Request::Search(SearchRequest {
            request_id: "test".into(),
            query: "rust".into(),
            limit: 5,
            providers: vec![],
            language: None,
            country: None,
            safe_search: true,
            freshness: None,
            dispatch_mode: None,
        });

        let mut req_json = serde_json::to_string(&request).unwrap();
        req_json.push('\n');
        client.write_all(req_json.as_bytes()).await.unwrap();

        let mut reader = BufReader::new(&mut client);
        let mut response_line = String::new();
        read_line_limited(&mut reader, &mut response_line, MAX_MESSAGE_SIZE)
            .await
            .expect("read response line");

        let response: Response =
            serde_json::from_str(response_line.trim()).expect("parse response");
        match response {
            Response::Error { code, .. } => {
                assert_eq!(code, -32601);
            }
            other => panic!("expected Error response, got: {:?}", other),
        }
    }

    /// Sending zero bytes (immediate EOF) should be handled cleanly.
    #[tokio::test]
    async fn client_disconnect_without_data() {
        let config = Arc::new(Config::default());
        let (client, handle) = spawn_handler(config).await;

        // Drop the client immediately, signaling EOF.
        drop(client);

        let result = handle
            .await
            .expect("handler task panicked")
            .expect("handle_connection failed");
        // handle_connection should return Ok on clean EOF
        assert!(matches!(result, ()));
    }

    /// Multiple requests on the same connection should all receive responses.
    #[tokio::test]
    async fn multiple_requests_on_same_connection() {
        let config = Arc::new(Config::default());
        let (mut client, _handle) = spawn_handler(config).await;

        for _ in 0..3 {
            client.write_all(b"{\"type\":\"Health\"}\n").await.unwrap();

            let mut reader = BufReader::new(&mut client);
            let mut response_line = String::new();
            read_line_limited(&mut reader, &mut response_line, MAX_MESSAGE_SIZE)
                .await
                .expect("read response line");

            let response: Response =
                serde_json::from_str(response_line.trim()).expect("parse response");
            assert!(matches!(response, Response::Health { .. }));
        }
    }

    /// Shutdown request should return ShutdownAck.
    #[tokio::test]
    async fn shutdown_returns_ack() {
        let config = Arc::new(Config::default());
        let (mut client, _handle) = spawn_handler(config).await;

        client
            .write_all(b"{\"type\":\"Shutdown\"}\n")
            .await
            .unwrap();

        let mut reader = BufReader::new(&mut client);
        let mut response_line = String::new();
        read_line_limited(&mut reader, &mut response_line, MAX_MESSAGE_SIZE)
            .await
            .expect("read response line");

        let response: Response =
            serde_json::from_str(response_line.trim()).expect("parse response");
        assert!(matches!(response, Response::ShutdownAck));
    }

    /// Sending a message over 1MB should return an Error with code -32600.
    #[tokio::test]
    async fn oversized_message_returns_error() {
        let config = Arc::new(Config::default());
        let (mut client, _handle) = spawn_handler(config).await;

        // Build a JSON message where the body exceeds 1MB.
        let padding = "A".repeat(MAX_MESSAGE_SIZE + 1024);
        let req_json = format!("{{\"type\":\"Health\",\"pad\":\"{}\"}}\n", padding);
        client.write_all(req_json.as_bytes()).await.unwrap();

        let mut reader = BufReader::new(&mut client);
        let mut response_line = String::new();
        read_line_limited(&mut reader, &mut response_line, MAX_MESSAGE_SIZE)
            .await
            .expect("read response line");

        let response: Response =
            serde_json::from_str(response_line.trim()).expect("parse response");
        match response {
            Response::Error { code, message, .. } => {
                assert_eq!(code, -32600, "expected invalid request code -32600");
                assert!(
                    message.to_lowercase().contains("too large"),
                    "expected 'too large' message, got: {}",
                    message
                );
            }
            other => panic!("expected Error response, got: {:?}", other),
        }
    }
}
