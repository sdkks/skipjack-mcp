.PHONY: build test lint fix install-hooks install-pre-commit-hook install-daemon-macos check-daemon-macos restart-daemon-macos clean tap-push install-release-deps bump-version release

PREFIX ?= $(HOME)/.cargo

install:
	cargo install --path .

install-daemon-macos:
	@mkdir -p $(HOME)/Library/LaunchAgents $(HOME)/Library/Logs
	sed -e 's|__HOME__|$(HOME)|g' \
	    -e 's|__PREFIX__|$(PREFIX)|g' \
	    init/macos/com.skipjackd.daemon.plist \
	    > $(HOME)/Library/LaunchAgents/com.skipjackd.daemon.plist
	plutil -lint $(HOME)/Library/LaunchAgents/com.skipjackd.daemon.plist
	@echo "installed ~/Library/LaunchAgents/com.skipjackd.daemon.plist"
	@echo ""
	@echo "To load now:"
	@echo "  launchctl bootstrap gui/$$(id -u) ~/Library/LaunchAgents/com.skipjackd.daemon.plist"
	@echo ""
	@echo "To stop:"
	@echo "  launchctl bootout gui/$$(id -u)/com.skipjackd.daemon"
	@echo ""
	@echo "Logs: ~/Library/Logs/skipjackd.log ~/Library/Logs/skipjackd.err"

check-daemon-macos:
	@echo "=== launchd ==="
	@launchctl print gui/$$(id -u)/com.skipjackd.daemon 2>/dev/null || echo "  not loaded in launchd"
	@echo ""
	@echo "=== process ==="
	@pgrep -fl skipjackd || echo "  not running"
	@echo ""
	@echo "=== socket ==="
	@if [ -S /tmp/skipjackd.sock ]; then \
		echo "  /tmp/skipjackd.sock exists"; \
		skipjackd status 2>/dev/null || echo "  daemon not responding"; \
	else \
		echo "  /tmp/skipjackd.sock missing"; \
	fi

restart-daemon-macos:
	@echo "stopping..."
	@launchctl bootout gui/$$(id -u)/com.skipjackd.daemon 2>/dev/null || true
	@echo "waiting for daemon to exit..."
	@for i in $$(seq 1 20); do \
		pgrep -f skipjackd >/dev/null 2>&1 || break; \
		sleep 0.5; \
	done
	@rm -f /tmp/skipjackd.sock /tmp/skipjackd.pid
	@echo "starting..."
	@launchctl bootstrap gui/$$(id -u) $(HOME)/Library/LaunchAgents/com.skipjackd.daemon.plist
	@echo "waiting for daemon to be ready..."
	@until skipjackd status >/dev/null 2>&1; do sleep 0.5; done
	@echo ""
	@$(MAKE) --no-print-directory check-daemon-macos

build:
	cargo build --release

test:
	cargo test

lint:
	cargo fmt --check
	cargo clippy -- -D warnings

fix:
	cargo fmt
	cargo clippy --fix --allow-dirty

install-hooks: install-pre-commit-hook

install-pre-commit-hook:
	cp pre-commit.sh .git/hooks/pre-commit
	cp commit-msg.sh .git/hooks/commit-msg
	chmod +x .git/hooks/pre-commit .git/hooks/commit-msg

clean:
	cargo clean

tap-push: lint test build

install-release-deps:
	cargo install cargo-edit 2>/dev/null || true
	cargo install cross 2>/dev/null || true
	rustup toolchain add stable-x86_64-unknown-linux-gnu --profile minimal --force-non-host 2>/dev/null || true
	rustup toolchain add stable-aarch64-unknown-linux-gnu --profile minimal --force-non-host 2>/dev/null || true
	rustup target add aarch64-apple-darwin 2>/dev/null || true

bump-version:
	@NEXT=$$(bash scripts/version.sh); \
	if [ -z "$$NEXT" ]; then \
		echo "No semver-significant commits since last tag. Nothing to bump."; \
		exit 0; \
	fi; \
	echo "=== Bumping version: $$(grep '^version' Cargo.toml | head -1) → $$NEXT ==="; \
	cargo set-version $$NEXT; \
	git add Cargo.toml Cargo.lock; \
	git commit -m "chore: bump to v$$NEXT"; \
	git tag -a "v$$NEXT" -m "v$$NEXT"; \
	echo "=== Tagged v$$NEXT ==="; \
	echo ""; \
	echo "To push:"; \
	echo "  git push origin main"; \
	echo "  git push origin v$$NEXT"

release: install-release-deps
	@if [ -n "$$(git status --porcelain)" ]; then \
		echo "ERROR: working tree is dirty. Commit or stash changes first."; \
		git status --short; \
		exit 1; \
	fi
	@NEXT=$$(bash scripts/version.sh); \
	if [ -z "$$NEXT" ]; then \
		echo "No semver-significant commits since last tag. Nothing to release."; \
		exit 0; \
	fi; \
	echo "=== Releasing v$$NEXT ==="; \
	$(MAKE) bump-version; \
	echo "=== Building for all targets ===" && \
	cross build --release --target x86_64-unknown-linux-musl && \
	cross build --release --target aarch64-unknown-linux-musl && \
	cargo build --release --target aarch64-apple-darwin && \
	mkdir -p dist && \
	for target in x86_64-unknown-linux-musl aarch64-unknown-linux-musl aarch64-apple-darwin; do \
		binary="target/$$target/release/skipjackd"; \
		if [ ! -f "$$binary" ]; then \
			echo "ERROR: binary not found: $$binary"; \
			exit 1; \
		fi; \
		case "$$target" in \
			*-linux-musl) suffix=$$target;; \
			*-apple-darwin) suffix=aarch64-darwin;; \
		esac; \
		tar -czf "dist/skipjackd-v$$NEXT-$$suffix.tar.gz" -C "target/$$target/release" skipjackd; \
		echo "  packaged dist/skipjackd-v$$NEXT-$$suffix.tar.gz"; \
	done && \
	echo "=== Pushing tags ===" && \
	git push origin main && \
	git push origin "v$$NEXT" && \
	echo "=== Creating GitHub release ===" && \
	gh release create "v$$NEXT" \
		--title "v$$NEXT" \
		--notes "Release v$$NEXT" \
		dist/*.tar.gz && \
	rm -rf dist && \
	echo "=== Released v$$NEXT ==="
