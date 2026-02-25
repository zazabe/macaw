#!/bin/env bash

set -euxo pipefail

rustup component add rustfmt clippy
cargo install --locked tokio-console cargo-insta websocat