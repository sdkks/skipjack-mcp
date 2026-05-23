.PHONY: build test lint fix install-hooks install-pre-commit-hook clean tap-push

install:
	cargo install --path .

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
