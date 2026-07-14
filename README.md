# sse

A spec-compliant [Server-Sent Events](https://html.spec.whatwg.org/multipage/server-sent-events.html) library for Nim. **Stdlib only** — no external dependencies, no `std/httpclient`; HTTP is done by hand over sockets.

- **Two clients**: async (`std/asyncdispatch`) and sync (blocking), both implementing the full EventSource lifecycle — automatic reconnection with exponential backoff, `Last-Event-ID` resumption, redirect following, and optional stall detection.
- **Server helpers**: wire-format serialization, auto-incrementing event IDs, a replay buffer for reconnecting clients, and keep-alive tracking — transport-agnostic, bring your own HTTP server.
- **Pure core**: the parser and writer do no I/O. Feed the parser raw bytes from any source; it emits events via callbacks.
- **TLS with real hostname verification** on all platforms, including Windows, where Nim's stdlib silently skips the check.

## Installation

```
nimble install sse
```

or

```
git clone https://github.com/Niminem/sse
```

## Client quickstart

**Which client?** Use the **async client** if your application already runs on `std/asyncdispatch` or you want many concurrent streams on one thread. Use the **sync client** for thread-based designs (one dedicated thread per stream, cancelled via `CancelToken`), for simple scripts and CLI tools where a blocking loop is the whole program, or when you want to stay out of asyncdispatch entirely. Both expose the same callbacks, configuration, and lifecycle — switching later is mostly a rename.

Async:

```nim
import std/asyncdispatch
import sse

let client = newAsyncSseClient("https://example.com/events")

client.onOpen = proc () =
  echo "connected"
client.onEvent = proc (event: SseEvent) =
  echo event.eventType, ": ", event.data
client.onError = proc (msg: string) =
  echo "error: ", msg

waitFor client.connect()  # runs until close() or a fatal error
```

Sync (blocks the calling thread; callbacks fire on that thread):

```nim
import sse

let client = newSyncSseClient("http://localhost:8080/events")
client.onEvent = proc (event: SseEvent) =
  echo event.data
  if event.data == "done":
    client.close()  # observed within one pollInterval

client.connect()
```

Both constructors validate the URL immediately (raising `ValueError`) but do no network activity until `connect`. Configuration is passed at construction:

```nim
let client = newSyncSseClient("http://localhost:8080/events",
  config = SyncSseClientConfig(
    autoReconnect: false,   # single-shot stream
    maxRedirects: 10,
    maxReconnectDelay: 60_000,
    stallTimeout: 30_000,   # give up if the server goes silent (0 = disabled)
    recvSize: 4096,
    pollInterval: 250,      # how quickly close()/cancellation is observed
    connectTimeout: 30_000,
  ))
```

The async config (`AsyncSseClientConfig`) has the same fields minus `pollInterval`/`connectTimeout`. Defaults are `DefaultConfig` (async) and `DefaultSyncConfig` (sync).

### Custom requests: POST, headers, body

The EventSource spec only knows GET with fixed headers, but many modern SSE endpoints — LLM APIs in particular — expect a POST with a JSON payload and auth headers, and stream SSE back. Both configs support this via three new fields — `httpMethod` (default `HttpGet`), `extraHeaders` (default empty), and `body` (default empty) — so an untouched config still sends a spec-compliant GET:

```nim
import std/[json, os]
import sse

var config = DefaultSyncConfig
config.autoReconnect = false      # important — see below
config.httpMethod = HttpPost      # std/httpcore's HttpMethod, re-exported
config.extraHeaders = @{
  "content-type": "application/json",
  "x-api-key": getEnv("ANTHROPIC_API_KEY"),
  "anthropic-version": "2023-06-01",
}
config.body = $ %*{
  "model": "claude-opus-4-8",
  "max_tokens": 1024,
  "stream": true,
  "messages": [{"role": "user", "content": "Hello!"}],
}

let client = newSyncSseClient("https://api.anthropic.com/v1/messages", config)
client.onEvent = proc (event: SseEvent) =
  echo event.eventType, ": ", event.data
  if event.eventType == "message_stop":
    client.close()
client.onError = proc (msg: string) =
  echo "error: ", msg

client.connect()
```

Rules, enforced with `ValueError` at construction:

- Extra header names must not collide (case-insensitively) with the headers the client manages itself: `Host`, `Accept`, `Cache-Control`, `Last-Event-ID`, and `Content-Length` (computed from `body`). No CR/LF anywhere in names or values.
- A non-empty `body` requires a method that allows one (not GET/HEAD). Set `Content-Type` via `extraHeaders`.

**Set `autoReconnect: false` for request/response-style streams.** Reconnection re-sends the entire request — method, headers, and body — so an LLM endpoint would re-run (and re-bill) the whole generation, and `Last-Event-ID` resumption is meaningless there. Auto-reconnect with custom requests is only sensible for endpoints where replaying the request is idempotent.

### Cancellation

Both clients accept an optional `CancelToken`. A token can be shared across any number of clients — sync and async alike — and a single `cancel()` call, safe from any thread (the flag is atomic), stops all of them at their next poll (sync) or yield point (async). Cancellation behaves like `close()`: no error event fires, and events already parsed but not yet delivered are silently dropped.

This is the mechanism for cross-thread shutdown. `close()` itself is same-thread only: call it from a callback on the sync client, or from the event-loop thread on the async client.

```nim
let token = newCancelToken()
let client = newSyncSseClient("http://localhost:8080/events", cancelToken = token)
# ... later, from any thread:
token.cancel()
```

Lifetime rule: create the token before spawning threads and keep a reference alive on the creating thread until all clients using it are done.

## Server quickstart

The server helpers are pure — every proc returns wire bytes for you to write to your transport (raw sockets, `std/asynchttpserver`, anything):

```nim
import sse

var conn = initSseConnection(historyLimit = 100)  # keep last 100 events for replay

# On a new request: send headers, then replay anything the client missed.
var payload = httpHeaders()                        # "HTTP/1.1 200 OK" + SSE headers
payload.add conn.replay(parseLastEventId(rawRequestHeaders))

# Send events (IDs auto-increment; pass id = "..." to override).
payload.add conn.formatEvent("hello world", eventType = "greeting")
payload.add conn.formatEvent("line 1\nline 2")     # multi-line data handled correctly

# Every ~15 s of inactivity, send a heartbeat so proxies don't kill the stream.
if conn.needsKeepAlive(nowMs):
  payload.add keepAliveComment()
```

For lower-level control, `sse/writer` exposes the raw serializers (`serializeEvent`, `serializeComment`, `serializeRetry`, `serializeId`), and `sse/parser` exposes the incremental parser (`initSseParser`, `feed`, `complete`) if you only need the wire format.

## TLS

Build with `-d:ssl` to enable `https://`. Without it, https URLs are rejected at construction time with a clear error.

By default the client verifies the certificate **chain** (`CVerifyPeer` + system CA roots) *and* the **hostname** (RFC 6125 via `X509_check_host`). The hostname check works on every platform — notably Windows, where `std/net` compiles it out and `std/asyncnet` never performs one at all. The HTTP request (including `Last-Event-ID`) is only sent after verification succeeds.

For development against self-signed certificates, disable both checks:

```nim
var config = DefaultSyncConfig
config.verifyHostname = false
let client = newSyncSseClient("https://localhost:8443/events", config)
client.sslContext = newContext(verifyMode = CVerifyNone)
```

`sslContext` also accepts a custom context for a private CA bundle or client certificates; a supplied context is caller-owned and never destroyed by the client.

On Windows, the default context needs a loadable CA store: place a `cacert.pem` where OpenSSL can find it (e.g. next to the executable, or point `SSL_CERT_FILE` at it).

## Module map

| Module | What it is | I/O |
|---|---|---|
| `sse/types` | `SseEvent`, `ReadyState`, `CancelToken`, callback types | none |
| `sse/parser` | Incremental wire-format parser | none |
| `sse/writer` | Wire-format serializer | none |
| `sse/server` | Per-connection server state: IDs, replay buffer, keep-alive | none |
| `sse/http` | URL parsing, request building, response/chunked decoding | none |
| `sse/client_async` | Async client (`std/asyncdispatch`) | sockets |
| `sse/client_sync` | Blocking client (`std/net`) | sockets |
| `sse/tls` | TLS handshake + portable hostname verification (`-d:ssl` only) | sockets |

`import sse` re-exports everything; individual modules can be imported standalone.

## Spec compliance

The parser, dispatch rules, reconnection semantics, `Last-Event-ID` handling, and response validation follow WHATWG HTML §9.2, verified by an extensive test suite (parser edge cases, chunked transfer, redirects, reconnection, TLS — ~6,000 lines of tests). Deliberate deviations:

- **Callbacks instead of the DOM `EventTarget` API** — `onEvent` receives every event type; there is no per-type listener registration.
- **No CORS / `withCredentials`** — meaningless outside a browser.
- **Custom request options** (`httpMethod`/`extraHeaders`/`body`) — the spec is GET-only with fixed headers; these opt-in fields exist for non-spec endpoints that stream SSE in response to a custom request (see "Custom requests" above). The defaults produce a byte-identical spec-compliant request.
- **TLS failures are fatal, not retried.** The spec would reconnect on any non-abort network error, but retrying a deterministic handshake or hostname failure would hammer a misconfigured (or MITM'd) endpoint forever.
- **All common redirect codes handled** (301/302/303/307/308; the spec only names 301/307). Permanent redirects (301/308) update the URL used for reconnection; temporary ones don't.

## Notes & caveats

- **Threading**: callbacks fire on the event loop (async) or the `connect` thread (sync). Client objects are not thread-safe; the only cross-thread operation is `CancelToken.cancel()`.
- **IP-literal https URLs skip the hostname check** (mirrors `std/net`; OpenSSL's IP-check API isn't exposed by the stdlib). Prefer DNS names for TLS endpoints.
- **The sync TLS handshake has no timeout** and isn't cancellable mid-attempt (`connectTimeout` covers only the TCP connect). Stall detection on the sync client rounds up to the next `pollInterval`.
- **`SslContext` is never freed** — one context per client for the process lifetime.

## Testing

```bash
nimble test
```

Tests spin up threaded loopback servers (including TLS ones with embedded self-signed certificates) — no network access or external fixtures required.

## License

MIT
