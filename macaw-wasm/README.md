# macaw-wasm

WebAssembly Component adapters for Macaw extension points.

The initial HTTP adapter loads a component implementing
[`wit/http-v1.wit`](wit/http-v1.wit), initializes it with an opaque JSON value,
and exposes its hooks through Macaw's native `HttpTransform` and `HttpRedact`
traits.

```rust
use macaw::http::HttpProxyOptions;
use macaw::wasm::WasmHttpPlugin;

let plugin = WasmHttpPlugin::from_file(
    "payment-auth.wasm",
    serde_json::json!({ "secret": "test-secret" }),
)?;

let options = HttpProxyOptions {
    transform: Box::new(plugin.transform()),
    redact: Box::new(plugin.redact()),
    ..Default::default()
};
```

Enable the root crate's `wasm` feature to use the `macaw::wasm` facade. Native
Rust transform and redaction implementations remain available and do not
depend on this crate.

The host links the synchronous WASI 0.2 (`wasi:cli/imports`) interfaces needed
by language runtimes such as Python. The default WASI context is sandboxed: it
does not inherit host arguments, environment variables, stdio, filesystem
access, or network access.

## Project-wide example

The repository's [`examples/wasm`](../examples/wasm) directory contains:

- a real `Macaw<Recorder>` host using the public `macaw::wasm` facade;
- equivalent Python and Rust HTTP auth plugin projects;
- build and run instructions.

Both guests read a signature from initialization configuration, add it during
`encode-request`, and replace it with `<redacted>` during `redact-request`. The
host creates one `WasmHttpPlugin`, so its transform and redaction adapters share
the same initialized component instance.

Build the Python guest and run the recorder with:

```bash
make -C examples/wasm/python build
cargo run --release --example wasm --features wasm -- \
  examples/wasm/python/target/http-auth-plugin-python.wasm
```

See [`examples/wasm/README.md`](../examples/wasm/README.md) for the Rust guest,
Python binding generation, custom upstream configuration, and complete setup
instructions.
