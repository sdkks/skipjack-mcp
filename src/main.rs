use std::env;
use std::process;

use metasearchd::config::Config;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() <= 1 {
        // No arguments — MCP server mode (FR-2.1).
        if let Err(e) = run_mcp().await {
            eprintln!("Fatal MCP error: {}", e);
            process::exit(1);
        }
        process::exit(0);
    }

    // Extract --config/-c value before daemon dispatch so it can be passed
    // to both Config::load and Daemon::start (for SIGHUP reload).
    let config_path = extract_config_arg(&args);

    // Check for --daemon as the first positional arg only (not anywhere in argv).
    if args.get(1).map_or(false, |a| a == "--daemon") {
        // Daemon mode (FR-1.1).
        eprintln!(
            "metasearchd {} ({}) starting daemon...",
            env!("CARGO_PKG_VERSION"),
            env!("GIT_SHA")
        );
        let config = Config::load(config_path.as_deref()).unwrap_or_else(|e| {
            eprintln!("Failed to load config: {}", e);
            process::exit(1);
        });
        let frozen = config.freeze();
        match metasearchd::daemon::Daemon::start(frozen, config_path).await {
            Ok(daemon) => {
                if let Err(e) = daemon.wait().await {
                    eprintln!("Daemon exited with error: {}", e);
                    process::exit(1);
                }
            }
            Err(e) => {
                eprintln!("Failed to start daemon: {}", e);
                process::exit(1);
            }
        }
        process::exit(0);
    }

    // CLI mode — dispatch to the CLI module.
    if let Err(e) = metasearchd::cli::run().await {
        let err_msg = e.to_string();
        // Match the context string from send_request() in cli.rs.
        let is_daemon_unreachable = err_msg.contains("daemon not running");

        eprintln!("error: {}", err_msg);

        if is_daemon_unreachable {
            process::exit(2);
        } else {
            process::exit(1);
        }
    }
}

/// Extract the value of `--config <path>` or `-c <path>` from raw args.
///
/// Returns `None` if no config flag is present. This is used before clap
/// parsing in daemon mode so the config path can be passed to both
/// `Config::load` and `Daemon::start` (for SIGHUP reload).
fn extract_config_arg(args: &[String]) -> Option<String> {
    for (i, arg) in args.iter().enumerate() {
        if (arg == "--config" || arg == "-c") && i + 1 < args.len() {
            return Some(args[i + 1].clone());
        }
    }
    None
}

/// Load config, set up tracing, and start the MCP server on stdio.
async fn run_mcp() -> anyhow::Result<()> {
    // Initialize tracing subscriber for MCP server logging.
    // We log to stderr since stdout is the MCP transport channel.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = Config::load(None)?;
    let frozen = config.freeze();
    metasearchd::mcp::run(frozen).await
}
