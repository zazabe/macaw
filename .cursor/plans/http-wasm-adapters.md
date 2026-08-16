# HTTP WASM transform and redact

## Objective

Add optional WebAssembly Component support for Macaw's HTTP transform and
redaction extension points while preserving native Rust implementations.

The first vertical slice proves the language-neutral WIT boundary with a small
Rust guest component. Python/WASI packaging and sidecar configuration are
separate follow-up work.

## Architecture

```text
HttpProxyOptions
├── Box<dyn HttpTransform>
│   ├── native Rust implementation
│   └── WasmHttpTransform
└── Box<dyn HttpRedact>
    ├── native Rust implementation
    └── WasmHttpRedact

WasmHttpTransform ─┐
                   ├── WasmHttpPlugin
WasmHttpRedact ────┘   └── shared Store + component instance
```

- Keep `macaw-http` independent of Wasmtime.
- Add a separate `macaw-wasm` workspace crate depending on `macaw-http`.
- Expose `macaw::wasm` behind an optional root `wasm` feature.
- Use a shared component instance for transform and redaction so initialization
  data and component state are consistent.
- Serialize synchronous component calls through shared interior mutability.

## Implementation plan

### 1. Correct the native HTTP contracts

- Make `HttpRedact::http_redact_request` return
  `Result<HttpRequestEvent, anyhow::Error>`.
- Propagate redaction failures through recorder and replayer processing.
- Prepare the final upstream URI and `Host` before calling `encode_request`.
- Continue recording and redacting the pre-encoded logical request so generated
  signatures are not stored.
- Add native recorder tests for:
  - final upstream URI and `Host` visibility;
  - controlled HTTP 500 responses when a transform fails;
  - no upstream call or recorded event after transform failure.

### 2. Define the HTTP Component ABI

- Add `macaw-wasm/wit/http-v1.wit`.
- Mirror Macaw's current HTTP event fidelity:
  - request ID;
  - method, URI, status, and HTTP version;
  - string header names and values;
  - byte bodies.
- Export:
  - `initialize(config-json)`;
  - `decode-request`;
  - `encode-request`;
  - `decode-response`;
  - `encode-response`;
  - `redact-request`.
- Treat request IDs as host-owned and reject components that change them.
- Validate all HTTP values returned by the component.

This version intentionally does not expand Macaw's current header model.
Duplicate headers and non-UTF-8 header values require a separate event-model
change before a future ABI version.

### 3. Implement the Wasmtime host

- Use the current Wasmtime Component Model API.
- Compile components from files or bytes.
- Initialize each component once with opaque JSON configuration.
- Keep one persistent instance per `WasmHttpPlugin`.
- Derive `WasmHttpTransform` and `WasmHttpRedact` from the shared plugin.
- Reset execution fuel before every call.
- Apply a bounded guest-memory limit.
- Map guest `result::err` values to contextual Macaw processing errors.
- Mark the instance unavailable after a trap; do not retry a potentially
  partially executed call.
- Provide no WASI imports in the first slice.

### 4. Demonstrate usage

- Add a runnable Rust guest component under `examples/wasm/rust`.
- Read a signature from initialization configuration.
- Add the signature during `encode-request`.
- Replace it with `<redacted>` during `redact-request`.
- Add a project-level host example showing both adapters sharing the initialized
  component.
- Document guest compilation, component wrapping, and execution in
  `macaw-wasm/README.md`.

### 5. Verify

- Test request and response conversion through generated WIT types.
- Test invalid request IDs, duplicate headers, guest errors, traps, and invalid
  component bytes.
- Run:
  - formatting checks;
  - focused `macaw-http` and `macaw-wasm` tests;
  - strict workspace Clippy;
  - the complete workspace test suite;
  - the runnable guest/host example end to end.

## Current status

- [x] Fallible HTTP redaction contract.
- [x] Final upstream URI and `Host` visible to `encode_request`.
- [x] Separate optional `macaw-wasm` crate.
- [x] Versioned HTTP WIT ABI.
- [x] Shared stateful transform/redact adapters.
- [x] JSON initialization, fuel and memory limits.
- [x] Event validation and trap handling.
- [x] Native failing-transform recorder test.
- [x] Runnable guest/host signing and redaction example.
- [x] Workspace tests, formatting, and strict Clippy passing.

## Follow-up work

- [x] Add WASI support needed by Python-generated components.
- [x] Add `componentize-py` project templates and build tooling.
- [x] Add an actual Macaw recorder integration test using a compiled component:
  verify the upstream receives a signature while the recording contains its
  redacted form.
- Wire WASM plugin selection and opaque configuration into machine-mode
  sidecar configuration.
- Add WebSocket transform/redaction interfaces.
- Make execution limits configurable.
- Define component recovery or replacement policy after traps.
- Revisit HTTP header fidelity in a future versioned ABI.
