//! Daemon lifecycle management: PID files, Unix domain socket, signal handling,
//! and the accept loop that dispatches client connections.
//!
//! # Architecture
//!
//! The [`Daemon`] struct owns the daemon lifecycle:
//!
//! 1. **PID file check** -- On startup, checks whether a PID file already exists
//!    and whether the referenced process is alive. Refuses to start if the
//!    daemon is already running.
//! 2. **Socket creation** -- Creates a Unix domain socket with `0600` permissions
//!    using `socket2` for fine-grained control.
//! 3. **Accept loop** -- Spawns each incoming connection into a task that
//!    reads newline-delimited JSON requests and writes responses.
//! 4. **Signal handling** -- SIGTERM/SIGINT trigger graceful shutdown
//!    (remove PID file, remove socket, wait for in-flight requests).
//!    SIGHUP triggers configuration reload without restart.
//!
//! # Example
//!
//! ```ignore
//! use skipjackd::config::Config;
//! use skipjackd::daemon::Daemon;
//!
//! let config = Config::load(None)?.freeze();
//! Daemon::start(config, None).await?;
//! ```

use anyhow::Context;
use socket2::{Domain, SockAddr, Socket, Type};
use std::os::fd::IntoRawFd;
use std::os::unix::io::FromRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::net::UnixListener;
use tokio::sync::{Notify, RwLock};
use tracing;

use crate::config::Config;
use crate::daemon::manager::Manager;

pub mod manager;
pub mod protocol;
pub mod server;

// Re-export protocol types for convenience.
pub use protocol::{ProviderHealth, ProviderInfo, Request, Response};

// ---------------------------------------------------------------------------
// Daemon struct
// ---------------------------------------------------------------------------

/// The daemon process controller.
///
/// Owns the lifecycle of the metasearch daemon: PID file, Unix socket,
/// signal handling, and the connection accept loop.
#[derive(Debug)]
pub struct Daemon {
    /// Shared configuration, atomically swappable on SIGHUP reload.
    config: Arc<RwLock<Arc<Config>>>,
    /// Path to the socket file (e.g., `/tmp/skipjackd.sock`).
    socket_path: PathBuf,
    /// Path to the PID file (e.g., `/tmp/skipjackd.pid`).
    pid_path: PathBuf,
    /// Instant the daemon was started (for uptime reporting).
    started_at: Instant,
    /// Signaled when a shutdown is requested (SIGTERM, SIGINT, or Shutdown request).
    shutdown_notify: Arc<Notify>,
    /// Set to `true` when config reload is requested (SIGHUP).
    reload_requested: Arc<AtomicBool>,
    /// Optional handle to the running daemon task.
    handle: Option<tokio::task::JoinHandle<anyhow::Result<()>>>,
}

impl Daemon {
    /// Start the daemon: check PID file, create socket, write PID, enter accept loop.
    ///
    /// # Arguments
    ///
    /// * `config` -- Frozen configuration. The daemon holds an `Arc` clone.
    /// * `config_path` -- Optional explicit path to the config file, used when
    ///   reloading configuration on SIGHUP. If `None`, the default path is used.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A PID file exists and the referenced process is still alive (duplicate start).
    /// - The socket directory does not exist or cannot be created.
    /// - The socket cannot be created or bound.
    /// - The PID file cannot be written.
    /// - Signal handlers cannot be registered.
    pub async fn start(config: Arc<Config>, config_path: Option<String>) -> anyhow::Result<Daemon> {
        let socket_path =
            PathBuf::from(&config.daemon.socket_dir).join(format!("{}.sock", config.daemon.name));
        let pid_path =
            PathBuf::from(&config.daemon.pid_dir).join(format!("{}.pid", config.daemon.name));

        // 1. Check for existing daemon.
        check_pid_file(&pid_path)?;

        // 2. Create the Unix domain socket with 0600 permissions.
        let listener = create_socket(&socket_path)?;

        // 3. Write the PID file.
        write_pid_file(&pid_path)?;

        tracing::info!(
            pid = %std::process::id(),
            socket = %socket_path.display(),
            "daemon started"
        );

        // 4. Set up signal handlers and shared state.
        let config_lock = Arc::new(RwLock::new(Arc::clone(&config)));
        let shutdown_notify = Arc::new(Notify::new());
        let reload_requested = Arc::new(AtomicBool::new(false));
        let started_at = Instant::now();

        // 5. Spawn the signal handler.
        let shutdown_signal = Arc::clone(&shutdown_notify);
        let reload_flag = Arc::clone(&reload_requested);
        let config_for_signal = Arc::clone(&config_lock);
        tokio::spawn(signal_handler(
            shutdown_signal,
            reload_flag,
            config_for_signal,
            config_path,
        ));

        // 6. Create the provider manager.
        let manager = Arc::new(
            Manager::new(&config)
                .await
                .context("Failed to create provider manager")?,
        );

        // 7. Spawn the accept loop.
        let config_for_accept = Arc::clone(&config_lock);
        let manager_for_accept = Arc::clone(&manager);
        let shutdown_for_accept = Arc::clone(&shutdown_notify);
        let reload_for_accept = Arc::clone(&reload_requested);
        let handle = tokio::spawn(accept_loop(
            listener,
            config_for_accept,
            manager_for_accept,
            reload_for_accept,
            shutdown_for_accept,
        ));

        Ok(Daemon {
            config: config_lock,
            socket_path,
            pid_path,
            started_at,
            shutdown_notify,
            reload_requested,
            handle: Some(handle),
        })
    }

    /// Wait for the daemon to shut down, consuming `self`.
    ///
    /// Returns the result of the accept loop task.
    pub async fn wait(mut self) -> anyhow::Result<()> {
        if let Some(handle) = self.handle.take() {
            match handle.await {
                Ok(result) => result,
                Err(join_err) => {
                    anyhow::bail!("daemon accept loop panicked: {}", join_err);
                }
            }
        } else {
            Ok(())
        }
    }

    /// Signal the daemon to shut down gracefully.
    ///
    /// This is called from the Shutdown request handler or from external
    /// code (e.g., the `stop` CLI subcommand). It triggers the same shutdown
    /// sequence as SIGTERM: remove PID file, remove socket, drain connections.
    pub fn request_shutdown(&self) {
        self.shutdown_notify.notify_one();
    }

    /// Return the daemon's uptime in seconds.
    pub fn uptime_secs(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }

    /// Check whether a config reload was requested (by SIGHUP) and return the
    /// latest configuration.
    ///
    /// Returns the current `Arc<Config>`; if a reload was requested since the
    /// last call, the fresh config is included.
    pub async fn config(&self) -> Arc<Config> {
        if self.reload_requested.swap(false, Ordering::SeqCst) {
            let config = self.config.read().await.clone();
            tracing::debug!("config reload applied");
            config
        } else {
            self.config.read().await.clone()
        }
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        cleanup(&self.pid_path, &self.socket_path);
    }
}

// ---------------------------------------------------------------------------
// PID file operations
// ---------------------------------------------------------------------------

/// Check whether a PID file exists and whether the referenced process is alive.
///
/// # Errors
///
/// Returns an error if the PID file exists and the process is still running,
/// indicating that the daemon is already active.
fn check_pid_file(pid_path: &Path) -> anyhow::Result<()> {
    if !pid_path.exists() {
        return Ok(());
    }

    let contents = std::fs::read_to_string(pid_path)
        .with_context(|| format!("Failed to read PID file: {}", pid_path.display()))?;

    let pid: u32 = contents
        .trim()
        .parse()
        .with_context(|| format!("Invalid PID in pid file: {}", contents.trim()))?;

    // Check if the process is still alive.
    if is_process_alive(pid) {
        anyhow::bail!("daemon already running (pid {})", pid);
    }

    // Stale PID file — remove it.
    tracing::warn!(
        pid = pid,
        path = %pid_path.display(),
        "stale PID file found (process not alive), removing"
    );
    let _ = std::fs::remove_file(pid_path);

    Ok(())
}

/// Write the current process ID to the PID file.
fn write_pid_file(pid_path: &Path) -> anyhow::Result<()> {
    // Ensure the parent directory exists.
    if let Some(parent) = pid_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create PID directory: {}", parent.display()))?;
    }

    let pid = std::process::id();
    std::fs::write(pid_path, format!("{}\n", pid))
        .with_context(|| format!("Failed to write PID file: {}", pid_path.display()))?;

    tracing::debug!(pid = pid, path = %pid_path.display(), "PID file written");
    Ok(())
}

/// Check whether a process with the given PID is alive.
///
/// Uses `kill(pid, 0)` which sends no signal but checks for process existence.
/// Distinguishes ESRCH (process not found) from EPERM (permission denied) — if we
/// can't determine liveness because of permissions, we assume alive to be safe.
fn is_process_alive(pid: u32) -> bool {
    // SAFETY: kill(pid, 0) is safe — it sends no signal, only checks
    // whether the process exists and we have permission to signal it.
    unsafe {
        let ret = libc::kill(pid as i32, 0);
        if ret == 0 {
            return true;
        }
        let err = std::io::Error::last_os_error();
        match err.raw_os_error() {
            Some(libc::ESRCH) => false,
            _ => true, // EPERM or other — assume alive to be safe
        }
    }
}

// ---------------------------------------------------------------------------
// Socket creation
// ---------------------------------------------------------------------------

/// Create a Unix domain socket with `0600` permissions using `socket2`.
///
/// 1. Removes any pre-existing socket file at `socket_path`.
/// 2. Creates the socket, binds, and listens via `socket2`.
/// 3. Sets file permissions to `0600`.
/// 4. Converts to a `tokio::net::UnixListener` for async I/O.
fn create_socket(socket_path: &Path) -> anyhow::Result<UnixListener> {
    // Remove any stale socket file.
    if socket_path.exists() {
        std::fs::remove_file(socket_path).with_context(|| {
            format!(
                "Failed to remove existing socket: {}",
                socket_path.display()
            )
        })?;
    }

    // Ensure the parent directory exists.
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create socket directory: {}", parent.display()))?;
    }

    // Create the socket using socket2 for explicit control over the bind.
    let addr = SockAddr::unix(socket_path)
        .with_context(|| format!("Invalid socket path: {}", socket_path.display()))?;

    let socket =
        Socket::new(Domain::UNIX, Type::STREAM, None).context("Failed to create Unix socket")?;

    socket
        .bind(&addr)
        .with_context(|| format!("Failed to bind socket: {}", socket_path.display()))?;

    socket.listen(128).context("Failed to listen on socket")?;

    // Tokio requires non-blocking file descriptors. socket2 creates blocking
    // sockets by default; we must set non-blocking mode before converting to
    // a tokio UnixListener.
    socket
        .set_nonblocking(true)
        .context("Failed to set socket to non-blocking")?;

    // Set 0600 permissions on the socket file BEFORE any connections are accepted.
    std::fs::set_permissions(
        socket_path,
        <std::fs::Permissions as std::os::unix::fs::PermissionsExt>::from_mode(0o600),
    )
    .with_context(|| {
        format!(
            "Failed to set socket permissions: {}",
            socket_path.display()
        )
    })?;

    tracing::debug!(
        path = %socket_path.display(),
        mode = "0600",
        "socket created and bound"
    );

    // Convert to async tokio listener using the raw file descriptor.
    // SAFETY: `socket` owns the fd and has been bound+listened above.
    // We transfer ownership by consuming `socket` via `into_raw_fd()`.
    let raw_fd = socket.into_raw_fd();
    let std_listener = unsafe { std::os::unix::net::UnixListener::from_raw_fd(raw_fd) };
    let listener = UnixListener::from_std(std_listener)
        .context("Failed to convert socket to async listener")?;

    Ok(listener)
}

// ---------------------------------------------------------------------------
// Accept loop
// ---------------------------------------------------------------------------

/// The main accept loop: accept incoming connections and spawn per-connection handlers.
///
/// Runs until `shutdown_notify` is signaled (by SIGTERM, SIGINT, or programmatic
/// shutdown request). On shutdown, the socket is dropped (which stops new accepts),
/// and the function returns.
///
/// Checks the reload flag on each accept iteration; when set, reads the fresh
/// config from the shared `RwLock` so that new connections see updated settings.
async fn accept_loop(
    listener: UnixListener,
    config: Arc<RwLock<Arc<Config>>>,
    manager: Arc<Manager>,
    reload_requested: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
) -> anyhow::Result<()> {
    loop {
        // Check for config reload before accepting.
        let current_config = if reload_requested.swap(false, Ordering::SeqCst) {
            tracing::debug!("accept loop applying config reload");
            config.read().await.clone()
        } else {
            config.read().await.clone()
        };

        let shutdown_grace = current_config.daemon.shutdown_grace_period_secs;

        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, peer_addr)) => {
                        tracing::debug!(
                            peer = ?peer_addr,
                            "accepted connection"
                        );
                        let mgr = Arc::clone(&manager);
                        tokio::spawn(async move {
                            if let Err(e) = server::handle_connection(stream, current_config, mgr).await {
                                tracing::warn!(error = %e, "connection handler error");
                            }
                        });
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "accept error");
                        // Continue accepting; a single accept error should not
                        // crash the daemon.
                    }
                }
            }
            _ = shutdown_notify.notified() => {
                tracing::info!(
                    grace_period_secs = shutdown_grace,
                    "shutdown signal received, draining connections"
                );

                // Drop the listener to stop accepting new connections.
                drop(listener);

                // Wait for in-flight requests to complete up to the grace period.
                // For now, we just sleep the grace period. Once the daemon tracks
                // in-flight request counts, replace with a proper drain.
                tokio::time::sleep(std::time::Duration::from_secs(shutdown_grace)).await;

                tracing::info!("shutdown complete");
                return Ok(());
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Signal handling
// ---------------------------------------------------------------------------

/// Handle OS signals for the lifetime of the daemon.
///
/// - **SIGTERM / SIGINT**: Notifies the shutdown signal, causing the accept
///   loop to drain and return.
/// - **SIGHUP**: Sets a reload flag so the daemon can re-read its configuration
///   file without restarting.
async fn signal_handler(
    shutdown_notify: Arc<Notify>,
    reload_requested: Arc<AtomicBool>,
    config: Arc<RwLock<Arc<Config>>>,
    config_path: Option<String>,
) {
    let mut sigterm = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
    {
        Ok(signal) => signal,
        Err(e) => {
            tracing::error!(error = %e, "failed to register SIGTERM handler");
            return;
        }
    };

    let mut sigint = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
    {
        Ok(signal) => signal,
        Err(e) => {
            tracing::error!(error = %e, "failed to register SIGINT handler");
            return;
        }
    };

    let mut sighup = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()) {
        Ok(signal) => signal,
        Err(e) => {
            tracing::error!(error = %e, "failed to register SIGHUP handler");
            return;
        }
    };

    loop {
        tokio::select! {
            _ = sigterm.recv() => {
                tracing::info!("SIGTERM received, initiating graceful shutdown");
                shutdown_notify.notify_one();
                return;
            }
            _ = sigint.recv() => {
                tracing::info!("SIGINT received, initiating graceful shutdown");
                shutdown_notify.notify_one();
                return;
            }
            _ = sighup.recv() => {
                tracing::info!("SIGHUP received, reloading configuration");
                reload_requested.store(true, Ordering::SeqCst);

                // Reload config from the same path used at startup.
                match Config::load(config_path.as_deref()) {
                    Ok(new_config) => {
                        tracing::info!(
                            path = config_path.as_deref().unwrap_or("default"),
                            "configuration reloaded"
                        );
                        *config.write().await = Arc::new(new_config);
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "failed to reload configuration, keeping current config");
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Cleanup
// ---------------------------------------------------------------------------

/// Remove the PID file and socket file on shutdown.
///
/// This is called from the [`Daemon`]'s `Drop` implementation and from the
/// accept loop after draining connections.
fn cleanup(pid_path: &Path, socket_path: &Path) {
    if pid_path.exists() {
        if let Err(e) = std::fs::remove_file(pid_path) {
            tracing::warn!(
                error = %e,
                path = %pid_path.display(),
                "failed to remove PID file during cleanup"
            );
        } else {
            tracing::debug!(path = %pid_path.display(), "PID file removed");
        }
    }

    if socket_path.exists() {
        if let Err(e) = std::fs::remove_file(socket_path) {
            tracing::warn!(
                error = %e,
                path = %socket_path.display(),
                "failed to remove socket file during cleanup"
            );
        } else {
            tracing::debug!(path = %socket_path.display(), "socket file removed");
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use tempfile::TempDir;

    // -----------------------------------------------------------------------
    // PID file tests
    // -----------------------------------------------------------------------

    /// check_pid_file returns Ok when no PID file exists.
    #[test]
    fn check_pid_file_no_file() {
        let dir = TempDir::new().expect("create temp dir");
        let pid_path = dir.path().join("test.pid");
        assert!(!pid_path.exists());
        check_pid_file(&pid_path).expect("should succeed when no PID file exists");
    }

    /// check_pid_file should error when the PID file references the current process.
    #[test]
    fn check_pid_file_current_process_alive() {
        let dir = TempDir::new().expect("create temp dir");
        let pid_path = dir.path().join("test.pid");
        std::fs::write(&pid_path, format!("{}\n", std::process::id())).expect("write pid");
        let result = check_pid_file(&pid_path);
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("already running"),
            "expected 'already running' error"
        );
    }

    /// check_pid_file should clean up a stale PID file (bogus PID that is not alive).
    #[test]
    fn check_pid_file_stale_removes_file() {
        let dir = TempDir::new().expect("create temp dir");
        let pid_path = dir.path().join("test.pid");

        // PID 99999 is very unlikely to exist.
        std::fs::write(&pid_path, "99999\n").expect("write pid");
        check_pid_file(&pid_path).expect("stale pid file should be cleaned up");
        assert!(
            !pid_path.exists(),
            "stale pid file should have been removed"
        );
    }

    /// write_pid_file should create the PID file with the current PID.
    #[test]
    fn write_pid_file_creates_file() {
        let dir = TempDir::new().expect("create temp dir");
        let pid_path = dir.path().join("test.pid");
        write_pid_file(&pid_path).expect("write pid file");
        assert!(pid_path.exists());

        let contents = std::fs::read_to_string(&pid_path).expect("read pid file");
        let pid: u32 = contents.trim().parse().expect("parse pid");
        assert_eq!(pid, std::process::id());
    }

    // -----------------------------------------------------------------------
    // Socket tests
    // -----------------------------------------------------------------------

    /// create_socket should create a socket file with 0600 permissions.
    #[tokio::test]
    async fn create_socket_creates_with_correct_permissions() {
        let dir = TempDir::new().expect("create temp dir");
        let socket_path = dir.path().join("test.sock");

        let listener = create_socket(&socket_path).expect("create socket");
        assert!(socket_path.exists());

        // Check permissions.
        let metadata = std::fs::metadata(&socket_path).expect("metadata");
        let mode = metadata.permissions().mode();
        // Unix socket files have mode bits set; check that only owner bits are set.
        assert_eq!(
            mode & 0o777,
            0o600,
            "socket permissions should be 0600, got {:o}",
            mode & 0o777
        );

        // Socket should be usable.
        assert!(listener.local_addr().is_ok());

        drop(listener);
    }

    /// create_socket removes a pre-existing socket before creating a new one.
    #[tokio::test]
    async fn create_socket_replaces_existing() {
        let dir = TempDir::new().expect("create temp dir");
        let socket_path = dir.path().join("test.sock");

        // Create a dummy file first.
        {
            let _f = std::fs::File::create(&socket_path).expect("create dummy file");
        }
        assert!(socket_path.exists());

        let listener = create_socket(&socket_path).expect("should replace existing file");
        assert!(socket_path.exists());

        // Should still be a socket.
        let metadata = std::fs::metadata(&socket_path).expect("metadata");
        let mode = metadata.permissions().mode();
        assert_eq!(mode & 0o777, 0o600);

        drop(listener);
    }

    // -----------------------------------------------------------------------
    // Integration tests (daemon lifecycle)
    // -----------------------------------------------------------------------

    /// A basic round-trip: start a daemon on temp dirs, send a Health request,
    /// shut down, and verify cleanup.
    #[tokio::test]
    async fn daemon_lifecycle_health_and_cleanup() {
        let dir = TempDir::new().expect("create temp dir");

        let mut config = Config::default();
        config.daemon.name = "test-lifecycle".into();
        config.daemon.socket_dir = dir.path().to_string_lossy().to_string();
        config.daemon.pid_dir = dir.path().to_string_lossy().to_string();
        config.daemon.shutdown_grace_period_secs = 1;
        let config = config.freeze();

        let daemon = Daemon::start(config, None).await.expect("start daemon");

        // Verify PID file exists.
        let pid_path = dir.path().join("test-lifecycle.pid");
        assert!(pid_path.exists(), "PID file should exist after start");

        // Verify socket file exists.
        let socket_path = dir.path().join("test-lifecycle.sock");
        assert!(socket_path.exists(), "socket file should exist after start");

        // Connect and send a Health request.
        let mut stream = tokio::net::UnixStream::connect(&socket_path)
            .await
            .expect("connect to daemon");
        use tokio::io::AsyncWriteExt;
        stream
            .write_all(b"{\"type\":\"Health\"}\n")
            .await
            .expect("write health request");
        stream.flush().await.expect("flush");

        // Read response.
        use tokio::io::{AsyncBufReadExt, BufReader};
        let mut reader = BufReader::new(&mut stream);
        let mut line = String::new();
        // Read until we get the response line.
        reader
            .read_line(&mut line)
            .await
            .expect("read response line");

        let response: Response = serde_json::from_str(line.trim()).expect("parse response");
        assert!(
            matches!(response, Response::Health { .. }),
            "expected Health response, got: {:?}",
            response
        );

        // Request shutdown.
        daemon.request_shutdown();

        // Wait for daemon to exit.
        daemon.wait().await.expect("daemon should exit cleanly");

        // Verify cleanup: PID file and socket file removed.
        assert!(
            !pid_path.exists(),
            "PID file should be removed after shutdown"
        );
        assert!(
            !socket_path.exists(),
            "socket file should be removed after shutdown"
        );
    }

    /// Starting a second daemon should fail with "already running".
    #[tokio::test]
    async fn duplicate_daemon_start_fails() {
        let dir = TempDir::new().expect("create temp dir");

        let mut config = Config::default();
        config.daemon.name = "test-dup".into();
        config.daemon.socket_dir = dir.path().to_string_lossy().to_string();
        config.daemon.pid_dir = dir.path().to_string_lossy().to_string();
        config.daemon.shutdown_grace_period_secs = 1;
        let config = config.freeze();
        let config2 = config.clone();

        let daemon = Daemon::start(config, None)
            .await
            .expect("start first daemon");

        // Second start should fail.
        let result = Daemon::start(config2, None).await;
        assert!(result.is_err(), "second daemon start should fail");
        assert!(
            result.unwrap_err().to_string().contains("already running"),
            "error should mention 'already running'"
        );

        // Clean up.
        daemon.request_shutdown();
        daemon
            .wait()
            .await
            .expect("first daemon should exit cleanly");
    }

    /// The socket path must be computed correctly from config values.
    #[test]
    fn socket_path_from_config() {
        let mut config = Config::default();
        config.daemon.name = "my-daemon".into();
        config.daemon.socket_dir = "/var/run".into();

        let socket_path =
            PathBuf::from(&config.daemon.socket_dir).join(format!("{}.sock", config.daemon.name));
        assert_eq!(socket_path, PathBuf::from("/var/run/my-daemon.sock"));
    }

    /// The PID path must be computed correctly from config values.
    #[test]
    fn pid_path_from_config() {
        let mut config = Config::default();
        config.daemon.name = "my-daemon".into();
        config.daemon.pid_dir = "/var/run".into();

        let pid_path =
            PathBuf::from(&config.daemon.pid_dir).join(format!("{}.pid", config.daemon.name));
        assert_eq!(pid_path, PathBuf::from("/var/run/my-daemon.pid"));
    }
}
