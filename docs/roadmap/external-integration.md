# External integration architecture

## Status

This document describes the proposed architecture for controlling Macaw from
external language runtimes, starting with Python. It is a roadmap, not a
description of the current public API.

## Goals

- Configure record or replay mode directly from application and test code.
- Start and stop Macaw reliably as part of a test lifecycle.
- Discover dynamically allocated proxy addresses without parsing human output.
- Support Macaw's HTTP and WebSocket features without embedding Rust in every
  host language.
- Allow executable redaction and transformation logic, including request
  signing.
- Keep plugins portable, sandboxed, and usable from languages other than
  Python.
- Preserve the existing CLI as a human-facing interface.

## Architecture

External integration is split into three layers:

```text
┌──────────────────────────────────────────────────────┐
│ Python application or test                          │
│                                                      │
│  macaw SDK                                           │
│  - typed configuration                              │
│  - context manager / pytest fixture                  │
│  - sidecar lifecycle                                │
│  - Python-to-WASM plugin tooling                     │
└──────────────────────────┬───────────────────────────┘
                           │ versioned JSON protocol
                           │ stdin / stdout
┌──────────────────────────▼───────────────────────────┐
│ Macaw sidecar                                        │
│                                                      │
│  - record/replay engine                              │
│  - proxy lifecycle and dynamic port binding          │
│  - recording storage                                 │
│  - declarative overrides                             │
│  - WASM component host                               │
└──────────────────────────┬───────────────────────────┘
                           │ versioned WIT interfaces
┌──────────────────────────▼───────────────────────────┐
│ WASM components                                      │
│                                                      │
│  - redact requests                                   │
│  - encode/decode requests and responses              │
│  - request signing and other executable transforms   │
│  - future protocol-specific extension points         │
└──────────────────────────────────────────────────────┘
```

The Python SDK is a controller, not a binding to Macaw's internal actor model.
Macaw runs in a child process and remains responsible for networking,
concurrency, recording, and replay. Custom code is compiled into a WASI
Component and loaded by the sidecar.

This separation avoids coupling Python's interpreter and event loop to Tokio,
while the component boundary provides one plugin model for Python, Rust,
JavaScript, Go, and other supported languages.

## Sidecar protocol

The CLI gains a machine mode in addition to its existing human mode. Machine
mode has the following stream contract:

- stdin receives one versioned JSON configuration document;
- stdout contains only newline-delimited JSON protocol messages;
- stderr contains logs intended for humans and log collectors;
- process exit status reports final success or failure.

The first protocol message identifies the protocol version. Startup completes
with a `ready` message containing the actual address of every proxy:

```json
{
  "type": "ready",
  "protocol_version": 1,
  "mode": "replay",
  "proxies": {
    "api": {
      "protocol": "http",
      "address": "127.0.0.1:43127",
      "url": "http://127.0.0.1:43127"
    }
  }
}
```

Other messages include structured startup errors, runtime diagnostics, and the
recording outcome. Protocol messages use an additive, versioned schema so that
SDK and sidecar compatibility can be checked before a test begins.

Proxy configurations should allow `127.0.0.1:0`. Macaw binds the socket and
returns the OS-selected port in `ready`; SDKs must not reserve a port and later
rebind it.

### Lifecycle

1. The SDK resolves record/replay mode, commonly from `MACAW_MODE`.
2. It starts the bundled or explicitly configured Macaw executable.
3. It writes configuration to stdin and waits for `ready` with a timeout.
4. The test uses proxy URLs returned by the SDK.
5. On context exit, the SDK requests graceful shutdown and waits for the
   sidecar.
6. Record mode flushes the recording before reporting successful shutdown.
7. Startup, plugin, recording, and unexpected process failures are raised as
   SDK exceptions.

The initial implementation may use a termination signal for shutdown. The
protocol should eventually include an explicit shutdown command so it also
works consistently on Windows and can return a structured outcome.

## Configuration model

SDK configuration is typed and serialized to the sidecar schema. Users should
not need to generate TOML or temporary configuration files.

```python
import os

from macaw import HttpProxy, Macaw, WasmRedact, WasmTransform

with Macaw.from_env(
    recording="tests/recordings/payment.json",
    proxies={
        "payment": HttpProxy(
            target="https://payments.example.com",
            bind="127.0.0.1:0",
            overrides=[
                # Declarative rules represented by typed Python values.
            ],
            transform=WasmTransform(
                "plugins/payment_auth.wasm",
                config={"secret": os.environ["PAYMENT_SECRET"]},
            ),
            redact=WasmRedact("plugins/payment_auth.wasm"),
        )
    },
) as macaw:
    client = PaymentClient(base_url=macaw.url("payment"))
    client.pay(...)
```

The same serialized model should be accepted by all SDKs. TOML remains
available for the human-facing CLI but is not the inter-process API.

Declarative overrides should be accepted inline. File references remain useful
for sharing larger rule sets and are resolved relative to a clearly defined
configuration root.

## WASM component model

Macaw hosts plugins with Wasmtime using the WebAssembly Component Model and
versioned WIT interfaces. Components receive stable, language-neutral protocol
records rather than serialized Rust structs or access to Macaw actors.

The WASM interfaces mirror Macaw's native `HttpTransform` and `HttpRedact`
interfaces. A WASM implementation is selected in proxy options in the same way
as a native implementation; the proxy pipeline does not need to distinguish
between them.

The HTTP transform interface exposes:

- `decode-request`
- `encode-request`
- `decode-response`
- `encode-response`

The HTTP redaction interface exposes:

- `redact-request`

Overrides remain declarative by default. An executable override hook can be
added later if a demonstrated use case cannot be represented by the rule
language.

A simplified interface shape is:

```wit
record header {
    name: string,
    value: list<u8>,
}

record http-request {
    method: string,
    uri: string,
    headers: list<header>,
    body: list<u8>,
}

interface http-transform {
    decode-request: func(request: http-request)
        -> result<http-request, transform-error>;

    encode-request: func(request: http-request)
        -> result<http-request, transform-error>;

    decode-response: func(response: http-response)
        -> result<http-response, transform-error>;

    encode-response: func(response: http-response)
        -> result<http-response, transform-error>;
}

interface http-redact {
    redact-request: func(request: http-request)
        -> result<http-request, redact-error>;
}
```

Plugins receive an opaque configuration value when they are instantiated.
Configuration may contain credentials and is available to plugin code so users
can implement arbitrary authentication protocols. Macaw treats components as
trusted user code: it isolates failures and host access, but does not attempt to
protect a credential from the component that uses it.

One component may export both interfaces. It can then sign the real upstream
request in `encode-request` and remove or normalize that signature in
`redact-request`. The two hooks remain separate because they operate on
different representations and at different points in the proxy pipeline.

The production interface must preserve details relevant to matching and wire
fidelity, including duplicate headers, non-UTF-8 values where the protocol
permits them, request identifiers, status codes, and binary bodies. Each
protocol gets its own WIT records and hooks.

### Python plugins

Python plugin code is packaged as a WASI Component, for example with
`componentize-py`. A plugin is compiled or packaged before the sidecar starts;
Macaw does not interpret an arbitrary `.py` file.

```python
class PaymentAuth:
    def __init__(self, config):
        self.secret = config["secret"]

    def encode_request(self, request):
        request.headers["authorization"] = sign_request(request, self.secret)
        return request

    def redact_request(self, request):
        request.headers["authorization"] = "<redacted>"
        return request
```

The signing algorithm, canonicalization, key derivation, and authorization
header format are entirely plugin-defined. Macaw does not need a registry of
authentication protocols or signing algorithms.

The Python SDK should provide project templates, a build command, compatibility
validation, and useful diagnostics around this packaging step. Pure-Python and
WASI-compatible dependencies can be supported. CPython native extensions,
subprocesses, unrestricted threads, and arbitrary host access are not assumed
to work.

## Host capabilities

WASM components are capability-based. They receive no filesystem, network,
environment, clock, or randomness unless explicitly configured. Plugin
configuration is the intentional exception: the complete configured value,
including credentials, is passed directly to the trusted component.

Useful host interfaces include:

- structured logging;
- current time, when nondeterminism is explicitly allowed;
- random bytes;
- optional outbound HTTP with an allowlist.

Capabilities are declared per plugin in sidecar configuration. Macaw must never
echo plugin configuration in readiness messages, diagnostics, or component
error output. Plugin authors are responsible for ensuring transformed secrets
are removed by redaction before events are recorded. Since a trusted component
can place a credential in a request, response, log, or allowed network call,
sandboxing cannot prevent deliberate credential disclosure by that component.

## Execution and failure semantics

Macaw loads and instantiates plugins during startup. The sidecar does not emit
`ready` until every plugin is validated and all proxies are listening.

Each invocation has configurable limits:

- execution fuel or epoch deadline;
- maximum memory;
- maximum request/response body size crossing the boundary;
- maximum log volume;
- allowed host capabilities.

A trap, timeout, invalid returned event, or denied capability becomes a
protocol-specific Macaw processing error. It must not crash the sidecar.
Errors include the proxy and hook names while excluding event bodies and
secrets by default.

The current transform API is synchronous. The first component ABI should also
be synchronous and keep host capabilities local and bounded. Network calls or
remote secret retrieval would require an async design and should not be added
implicitly to the first ABI.

Components should be instantiated per proxy unless measurements demonstrate a
need for pooling. Calls for one proxy are sequential under the current actor
model, which gives stateful plugins predictable ordering. Cross-proxy shared
mutable state is not guaranteed.

## Python SDK

The initial SDK surface consists of:

- `Macaw` context manager;
- typed `HttpProxy` and `WebSocketProxy` configuration;
- explicit `record` and `replay` modes plus `from_env`;
- resolved `address()` and `url()` accessors;
- typed declarative override builders;
- `WasmTransform` and `WasmRedact` implementations with opaque plugin
  configuration;
- a pytest fixture factory;
- structured exceptions carrying sidecar diagnostics;
- plugin build and validation tooling.

An asynchronous context manager can be added without changing the sidecar
protocol. The subprocess remains the isolation boundary in both cases.

Platform-specific Python wheels may bundle the Macaw executable. The SDK also
accepts an explicit executable path for development, unsupported platforms,
and system-managed installations. SDK and sidecar versions are checked during
the startup handshake.

## Delivery plan

The architecture is implemented as one vertical plan, split into reviewable
changes:

1. Define and version the JSON configuration, sidecar messages, and initial
   HTTP WIT interface.
2. Add machine mode, dynamic bindings, structured readiness, graceful
   shutdown, and error reporting to the Macaw executable.
3. Add Wasmtime hosting with `encode-request`, limits, and logging.
4. Add the Python SDK context manager, typed HTTP configuration, subprocess
   lifecycle, and pytest integration.
5. Add Python component templates and packaging.
6. Prove the full path with an end-to-end request-signing test in both record
   and replay modes.
7. Add the remaining HTTP hooks, declarative override parity, WebSocket hooks,
   and distribution artifacts.

The first usable vertical slice is intentionally narrow: one HTTP proxy,
record/replay selection, dynamic port discovery, and a Python-authored
`encode-request` component. It validates every architectural boundary before
the ABI is expanded.

## Non-goals

- Exposing Macaw's actor handles or internal Rust event types to Python.
- Running arbitrary Python source directly inside the Macaw process.
- Guaranteeing compatibility with all PyPI packages inside WASI.
- Giving plugins unrestricted host filesystem or network access. Credentials
  explicitly supplied in plugin configuration are available to the plugin.
- Replacing the existing human CLI and TOML workflow.
- Making remote or asynchronous host calls part of the initial transform ABI.

## Decisions to finalize before implementation

- The exact Component Model and WASI versions supported by the first release.
- The complete wire-faithful HTTP and WebSocket WIT record shapes.
- Whether configuration is a single stdin document or framed protocol command.
- The graceful shutdown command and process signal fallback on each platform.
- Component state, instance reuse, and concurrency guarantees.
- ABI compatibility policy and supported SDK/sidecar version skew.
- Distribution ownership for platform binaries and Python WASM build tooling.
