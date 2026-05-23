# metasearchd

Multi-provider web search daemon with anti-blocking countermeasures, local caching, and MCP integration for AI agents.

## Quick start

```bash
# Build from source (requires Rust 1.86+)
cargo build --release

# Copy the example config and add your API keys
mkdir -p ~/.config/metasearchd
cp config.toml ~/.config/metasearchd/config.toml
# Edit to set: JINA_API_KEY, BRAVE_API_KEY (env vars or inline)

# Start the daemon
./target/release/metasearchd --daemon

# Run a search
./target/release/metasearchd search "rust async patterns" --limit 5

# Or run as an MCP server for AI agents (reads/writes on stdio)
./target/release/metasearchd
```

## Modes of operation

metasearchd is a single binary with three personalities:

| Invocation | Mode | Description |
|---|---|---|
| `metasearchd` (no args) | **MCP server** | Listens on stdio for MCP JSON-RPC requests from AI agents |
| `metasearchd --daemon` | **Daemon** | Background process listening on a Unix domain socket |
| `metasearchd <subcommand>` | **CLI** | One-shot commands that talk to the daemon over the socket |

## CLI subcommands

```
metasearchd search <query>     Run a web search
  -n, --limit <N>              Max results (default: 10)
  -p, --providers <list>       Comma-separated provider names
  -l, --language <code>        ISO 639-1 language code (e.g. en, fr)
  --country <code>             ISO 3166-1 alpha-2 country code
  --freshness <filter>         past-day | past-week | past-month
  --format <fmt>               pretty (default) or json
  --dispatch-mode <mode>       concurrent (default) or tiered

metasearchd status             Show daemon health and connected providers
metasearchd stop               Gracefully shut down the daemon
metasearchd providers          List configured providers
metasearchd cache-clear        Clear the SQLite cache
metasearchd usage              Show available commands
metasearchd --version          Print version and git SHA
```

## MCP tools

When running in MCP mode, three tools are exposed to the AI agent:

| Tool | Description |
|---|---|
| `search` | Execute a multi-provider search with optional filters, limit, and dispatch mode |
| `list_providers` | List all configured providers with their current health status |
| `cache_stats` | Retrieve cache hit/miss/eviction statistics |

## Search providers

| Provider | Method | Auth | Notes |
|---|---|---|---|
| DuckDuckGo | HTML scraping | None | Privacy-respecting, no API key needed |
| Jina AI | `s.jina.ai` API | `JINA_API_KEY` env var | Returns clean markdown |
| Brave Search | API | `BRAVE_API_KEY` env var | Index-based, fast |
| SearXNG | JSON API | Optional | Self-hosted metasearch instance |

## Anti-blocking

Each provider can be configured with:

- **User-Agent rotation** — pool of modern browser UAs, rotated per request
- **Rate limiting** — sliding-window RPM cap per provider
- **Exponential backoff** — configurable base delay, max attempts, and cap
- **TLS cipher shuffling** — randomizes cipher order to avoid JA3 fingerprinting
- **IP rotation** — static, per-request, IPv6 pool rotation strategies
- **Page delays** — random jitter between requests (500ms–2s default)

## Dispatch modes

- **Concurrent** (`dispatch.mode = "concurrent"`) — fire all enabled providers at once, merge results as they arrive. Fastest, but consumes more resources.
- **Tiered** (`dispatch.mode = "tiered"`) — execute provider tiers sequentially. Each tier runs in parallel internally. Stop when enough results are collected. Lower tiers only run if higher tiers fail or return insufficient results.

## Configuration

Configuration is loaded from `~/.config/metasearchd/config.toml` (or the path passed via `--config` / `-c`). Environment variables with the `METASEARCHD_` prefix override config keys using double-underscore nesting:

```bash
METASEARCHD_CACHE__DEFAULT_TTL_SECS=7200  # overrides cache.default_ttl_secs
METASEARCHD_DAEMON__LOG_LEVEL=debug       # overrides daemon.log_level
```

See `config.toml` in the repo for all options with defaults and documentation.

## Caching

SQLite-based cache with WAL mode. Configurable TTL per provider or globally. Tracks hits, misses, and evictions. Query cache is keyed on `(query, provider, language, country)` so parameter variations are cached independently.

## Daemon lifecycle

```
metasearchd --daemon              Start in background
metasearchd status                Check health
metasearchd stop                  Graceful shutdown (SIGTERM)
kill -SIGHUP <pid>               Reload config without restart
kill -SIGINT <pid>               Graceful shutdown (30s drain period)
```

The daemon writes a PID file to `/tmp/metasearchd.pid` and listens on `/tmp/metasearchd.sock` (paths configurable).

## Docker

A development Docker image is available. Not intended for production — the daemon runs as a standalone binary on the host.

```bash
docker build -t metasearchd:dev .
docker run --rm -it metasearchd:dev cargo build --release
```

## Install from releases

```bash
curl -sSL https://raw.githubusercontent.com/said/metasearchd/main/install.sh | sh
# Or specify a version:
VERSION=0.1.0 sh install.sh
```

Binaries are installed to `~/.local/bin`.

## Build

```bash
cargo build --release    # release build
cargo test               # run tests
make lint                # format + clippy
make fix                 # auto-fix formatting and clippy suggestions
```

## License

MIT
