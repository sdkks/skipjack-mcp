.PHONY: build test lint fix install-hooks install-pre-commit-hook install-daemon-macos check-daemon-macos restart-daemon-macos clean tap-push

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
	@sleep 2
	@rm -f /tmp/skipjackd.sock /tmp/skipjackd.pid
	@echo "starting..."
	@launchctl bootstrap gui/$$(id -u) $(HOME)/Library/LaunchAgents/com.skipjackd.daemon.plist
	@sleep 2
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
