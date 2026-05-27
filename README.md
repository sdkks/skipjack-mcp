# skipjackd

Multi-provider web search daemon with anti-blocking countermeasures, local caching, and MCP integration for AI agents.

## Quick start

```bash
# Build from source (requires Rust 1.86+)
cargo build --release

# Copy the example config and add your API keys
mkdir -p ~/.config/skipjackd
cp config.toml ~/.config/skipjackd/config.toml
# Set your API keys in the config (see "Search providers" below)
# vim ~/.config/skipjackd/config.toml

# Start the daemon
./target/release/skipjackd --daemon

# Run a search
./target/release/skipjackd search "rust async patterns" --limit 5

# Or run as an MCP server for AI agents (reads/writes on stdio)
./target/release/skipjackd
```

## Modes of operation

skipjackd is a single binary with three personalities:

| Invocation               | Mode           | Description                                               |
| ------------------------ | -------------- | --------------------------------------------------------- |
| `skipjackd` (no args)    | **MCP server** | Listens on stdio for MCP JSON-RPC requests from AI agents |
| `skipjackd --daemon`     | **Daemon**     | Background process listening on a Unix domain socket      |
| `skipjackd <subcommand>` | **CLI**        | One-shot commands that talk to the daemon over the socket |

## CLI subcommands

```
skipjackd search <query>     Run a web search
  -n, --limit <N>              Max results (default: 10)
  -p, --providers <list>       Comma-separated provider names
  -l, --language <code>        ISO 639-1 language code (e.g. en, fr)
  --country <code>             ISO 3166-1 alpha-2 country code
  --freshness <filter>         past-day | past-week | past-month
  --format <fmt>               pretty (default) or json
  --dispatch-mode <mode>       concurrent (default) or tiered

skipjackd fetch <url>        Fetch a URL and print the page as markdown
  -t, --timeout <secs>         Per-request timeout (default: 15, max: 120)
  --json                       Output raw JSON instead of markdown

skipjackd status             Show daemon health and connected providers
skipjackd stop               Gracefully shut down the daemon
skipjackd providers          List configured providers
skipjackd cache-clear        Clear the SQLite cache
skipjackd usage              Show available commands
skipjackd --version          Print version and git SHA
```

## MCP tools

When running in MCP mode, four tools are exposed to the AI agent:

| Tool             | Description                                                                                                          |
| ---------------- | -------------------------------------------------------------------------------------------------------------------- |
| `search`         | Execute a multi-provider search with optional filters, limit, and dispatch mode                                      |
| `fetch`          | Fetch a web page and return its content as markdown, with TLS/UA rotation to avoid blocking                          |
| `list_providers` | List all configured providers with their current health status                                                       |
| `cache_stats`    | Retrieve cache hit/miss/eviction statistics                                                                          |

### Fetch tool

The `fetch` tool reuses skipjackd's anti-blocking infrastructure to convert web pages to markdown. The intended workflow is `search` → pick relevant URLs → `fetch` each one for reading.

| Parameter      | Required | Default | Description                         |
| -------------- | -------- | ------- | ----------------------------------- |
| `url`          | yes      | —       | The URL to fetch                    |
| `timeout_secs` | no       | 15      | Per-request timeout (max 120)       |

| Response field  | Type    | Description                                                      |
| --------------- | ------- | ---------------------------------------------------------------- |
| `url`           | string  | The fetched URL (may differ after redirects)                     |
| `markdown`      | string  | Page content converted to markdown                               |
| `status`        | string  | `"ok"`, `"error"`, or `"spa_detected"`                          |
| `spa_detected`  | boolean | Whether the page appears to require JavaScript rendering         |
| `http_status`   | integer | HTTP status code                                                 |
| `error`         | string? | Error message (only present when status is `"error"`)            |

**SPA detection:** if the page looks JavaScript-rendered (e.g., React, Vue, Next.js), the markdown will end with a machine-readable `<!-- SKIPJACKD_SPA_DETECTED: ... -->` comment block so the agent knows the content may be incomplete and can fall back to a browser-based tool (e.g., Playwright MCP).

**Security:** SSRF protection blocks private IPs (RFC 1918, loopback, link-local), cloud metadata endpoints, and known internal hostnames. Redirects are re-validated at every hop. Response body is capped at 5 MB.

Wire it into your MCP client by adding to `mcp.json`:

```json
{
  "mcpServers": {
    "skipjackd": {
      "command": "skipjackd",
      "args": []
    }
  }
}
```

Or install globally with a single command:

```bash
claude mcp add skipjackd -- skipjackd # project scope
claude mcp add skipjackd --scope user -- skipjackd # user scope (global)
```

This registers `skipjackd` as an MCP server in Claude Code.

**The MCP server is a proxy to the daemon** — you must have `skipjackd --daemon` running in the background. The MCP server connects to the daemon's Unix socket internally, so the daemon handles all provider communication, caching, and health tracking.

See `mcp.json.example` in the repo.

## Search providers

| Provider     | Method          | Auth    | Configuration                                        |
| ------------ | --------------- | ------- | ---------------------------------------------------- |
| DuckDuckGo   | HTML scraping   | None    | No key needed — works out of the box                 |
| Jina AI      | `s.jina.ai` API | API key | `api_key` or `api_key_env = "JINA_API_KEY"`          |
| Brave Search | API             | API key | `api_key` or `api_key_env = "BRAVE_API_KEY"`         |
| SearXNG      | JSON API        | None    | `base_url = "http://localhost:8080"` (your instance) |

### API keys

Providers that require authentication support two config fields:

```toml
[[providers]]
name = "brave"
enabled = true
api_key = "BSA-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"   # put the key directly in config

[[providers]]
name = "jina"
enabled = true
api_key_env = "JINA_API_KEY"                       # or read from an env var
```

Use `api_key` for the literal value, or `api_key_env` to reference an environment variable. `api_key_env` is safer if you keep your config in version control — put the secret in your shell profile or a `.env` file instead.

### SearXNG

SearXNG needs no API key — just point `base_url` at your instance. Works with both HTTP (local) and HTTPS (public):

```toml
[[providers]]
name = "searxng"
enabled = true
base_url = "http://localhost:8080"       # local instance
# base_url = "https://search.example.com" # public instance
rate_limit_rpm = 20
tags = ["web", "privacy"]
```

If `base_url` is missing, the provider is marked unavailable and skipped during search.

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

Configuration is loaded from `~/.config/skipjackd/config.toml` (or the path passed via `--config` / `-c`). Environment variables with the `SKIPJACKD_` prefix override config keys using double-underscore nesting:

```bash
SKIPJACKD_CACHE__DEFAULT_TTL_SECS=7200  # overrides cache.default_ttl_secs
SKIPJACKD_DAEMON__LOG_LEVEL=debug       # overrides daemon.log_level
```

See `config.toml` in the repo for all options with defaults and documentation.

## Caching

SQLite-based cache with WAL mode. Configurable TTL per provider or globally. Tracks hits, misses, and evictions. Query cache is keyed on `(query, provider, language, country)` so parameter variations are cached independently.

## Daemon lifecycle

```
skipjackd --daemon              Start in background
skipjackd status                Check health
skipjackd stop                  Graceful shutdown (SIGTERM)
kill -SIGHUP <pid>               Reload config without restart
kill -SIGINT <pid>               Graceful shutdown (30s drain period)
```

The daemon writes a PID file to `/tmp/skipjackd.pid` and listens on `/tmp/skipjackd.sock` (paths configurable).

### Launch at boot (macOS)

```bash
make install-daemon-macos   # installs a launchd plist to ~/Library/LaunchAgents/
```

This creates a launchd service that starts `skipjackd --daemon` at login and keeps it alive. Logs go to `~/Library/Logs/skipjackd.log`. See `init/macos/` for the plist template.

## Docker

A development Docker image is available. Not intended for production — the daemon runs as a standalone binary on the host.

```bash
docker build -t skipjackd:dev .
docker run --rm -it skipjackd:dev cargo build --release
```

## Install from releases

```bash
curl -sSL https://raw.githubusercontent.com/said/skipjackd/main/install.sh | sh
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
make install             # install to ~/.cargo/bin
```

## License

MIT
