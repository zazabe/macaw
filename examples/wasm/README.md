# WASM HTTP plugin example

This example runs a real Macaw HTTP recorder with a WebAssembly Component
providing both request transformation and recording redaction. The component
adds `x-wasm-signature: signed-by-wasm` to the upstream request, while Macaw
stores `x-wasm-signature: <redacted>`.

Choose either the Python or Rust guest implementation below. Both implement the
versioned WIT contract in
[`macaw-wasm/wit/http-v1.wit`](../../macaw-wasm/wit/http-v1.wit).

## Python guest

Python 3.10 or newer with `venv` and `pip` is required.

```bash
make -C examples/wasm/python build
```

## Rust guest

Install the WebAssembly target and `wasm-tools`, then build and wrap the guest:

```bash
rustup target add wasm32-unknown-unknown
cargo install wasm-tools

cargo build \
  --manifest-path examples/wasm/rust/Cargo.toml \
  --target wasm32-unknown-unknown \
  --release

mkdir -p examples/wasm/rust/target/component
wasm-tools component new \
  examples/wasm/rust/target/wasm32-unknown-unknown/release/macaw_http_auth_plugin.wasm \
  -o examples/wasm/rust/target/component/http-auth-plugin.wasm
```

## Run the recorder

Pass either component to the project-level example:

```bash
cargo run --release --example wasm --features wasm -- \
  examples/wasm/python/target/http-auth-plugin-python.wasm
```

The example starts its own HTTP echo server and forwards requests to it, so it
does not require external network access. It prints the recorder's dynamically
assigned local address and a ready-to-run `curl` command. Send requests through
that address, then press Ctrl-C to write `data/wasm-record.json`.
