# Development sandbox for metasearchd.
# This image provides a Rust toolchain and all build dependencies.
# It is NOT intended for production deployment — the daemon runs as a
# standalone binary on the host.

FROM rust:1.86-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    musl-tools \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Add cross-compilation targets for release builds.
RUN rustup target add \
    x86_64-unknown-linux-musl \
    aarch64-unknown-linux-musl

WORKDIR /workspace

# Layer dependencies first for caching.
COPY Cargo.toml Cargo.lock* ./
RUN mkdir -p src && \
    echo 'fn main() {}' > src/main.rs && \
    echo '' > src/lib.rs && \
    cargo build --release 2>/dev/null || true && \
    rm -rf src

COPY . .

CMD ["/bin/bash"]
