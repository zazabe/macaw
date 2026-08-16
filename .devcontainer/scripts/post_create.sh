#!/bin/env bash

set -euxo pipefail

rustup component add rustfmt clippy

curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash
cargo binstall --no-confirm tokio-console cargo-insta websocat