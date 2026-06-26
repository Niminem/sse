# SSE Library — Implementation Plan

Spec-compliant Server-Sent Events library in Nim. Stdlib only.

Reference: [HTML Living Standard §9.2 — Server-sent events](https://html.spec.whatwg.org/multipage/server-sent-events.html)

---

## Module Structure

```
src/
  sse.nim                # Umbrella module — re-exports public API
  sse/
    types.nim            # Core types, config, errors
    parser.nim           # Incremental event stream parser (spec §9.2.5–6)
    writer.nim           # Event serialization to text/event-stream format
    client.nim           # Sync EventSource client
    asyncclient.nim      # Async EventSource client
    server.nim           # Server-side helpers
tests/
  t_parser.nim           # Parser correctness + edge cases
  t_writer.nim           # Serialization + roundtrip tests
  t_client.nim           # Client integration tests
  t_server.nim           # Server helper tests
```

Users import `sse` to get the full public API, or individual submodules
(e.g. `import sse/parser`) for selective use.

### Stdlib Dependencies

- `std/strutils` — string operations
- `std/parseutils` — integer parsing
- `std/net` — sync sockets (client)
- `std/asyncnet` — async sockets (async client)
- `std/asyncdispatch` — async runtime
- `std/asynchttpserver` — server helpers
- `std/httpcore` — `HttpHeaders`, HTTP status codes
- `std/uri` — URL parsing
- `std/unittest` — tests

---

## 1. Core Types (`types.nim`)

### SseEvent

```nim
type
  SseEvent* = object
    data*: string       ## Event payload. Multi-line data is joined with \n.
    eventType*: string  ## The "event" field value. Empty string = "message".
    id*: string         ## Last event ID set by this event.
    retry*: int         ## Reconnection time in ms. -1 = not set by this event.
```

Plain value type (`object`, not `ref`). Small, cheap to copy and stack-allocate.
`retry` uses `-1` as sentinel rather than `Option[int]` — simpler and avoids
an `std/options` dependency for one field.

### ReadyState

```nim
type
  ReadyState* = enum
    rsConnecting = 0   ## Connection not yet established, or reconnecting.
    rsOpen = 1         ## Connection open, dispatching events.
    rsClosed = 2       ## Connection closed; not reconnecting.
```

Matches the spec's `CONNECTING`, `OPEN`, `CLOSED` constants (§9.2.2).

### Errors

```nim
type
  SseError* = object of CatchableError
    ## Base error type for this library.

  SseLimitError* = object of SseError
    ## Raised when a parser safety limit is exceeded
    ## (maxLineLen, maxEventSize).

  SseConnectionError* = object of SseError
    ## Raised on unrecoverable connection failures
    ## (non-200 status, wrong content type, HTTP 204, etc.).
```

### CancelToken

```nim
type
  CancelToken* = ref object
    cancelled: bool

proc newCancelToken*(): CancelToken
proc cancel*(token: CancelToken)
proc isCancelled*(token: CancelToken): bool
```

Lightweight cancellation signal. Single-threaded (no atomics).
Used by the client to support external cancellation.
Internally paired with socket timeouts so a blocking `recv` doesn't
prevent cancellation from being observed.

### Parser Config

```nim
type
  SseParserConfig* = object
    maxLineLen*: int       ## Max bytes per line. Default: 65_536 (64 KiB).
    maxEventSize*: int     ## Max bytes in accumulated event data. Default: 1_048_576 (1 MiB).

proc initSseParserConfig*(): SseParserConfig =
  SseParserConfig(maxLineLen: 65_536, maxEventSize: 1_048_576)
```

### Client Config

```nim
type
  EventSourceConfig* = object
    reconnectionTime*: int       ## Initial reconnection delay in ms. Default: 3000.
    minReconnectTime*: int       ## Floor for server-sent retry value. Default: 1000 ms.
    maxReconnectTime*: int       ## Ceiling for server-sent retry value. Default: 60_000 ms.
    maxReconnectAttempts*: int   ## Max consecutive reconnects. 0 = unlimited. Default: 0.
    inactivityTimeout*: int     ## Dead-connection detection in ms. 0 = disabled. Default: 0.
    cancelToken*: CancelToken   ## Optional external cancellation signal.
    parserConfig*: SseParserConfig
```

### Server Config

```nim
type
  SseServerConfig* = object
    heartbeatInterval*: int   ## Keep-alive comment interval in ms. Default: 15_000.
                               ## 0 = disabled. Spec §9.2.7 recommends ~15 seconds.
```

---

## 2. Parser (`parser.nim`)

Transport-agnostic, incremental event stream parser implementing spec §9.2.5
(grammar) and §9.2.6 (interpretation).

### ABNF Grammar (spec §9.2.5)

The parser must accept streams conforming to:

```
stream        = [ bom ] *event
event         = *( comment / field ) end-of-line
comment       = colon *any-char end-of-line
field         = 1*name-char [ colon [ space ] *any-char ] end-of-line
end-of-line   = ( cr lf / cr / lf )
```

Where:
- `lf` = U+000A
- `cr` = U+000D
- `colon` = U+003A
- `space` = U+0020
- `bom` = U+FEFF
- `name-char` = any scalar value except LF, CR, or COLON
- `any-char` = any scalar value except LF or CR

MIME type: `text/event-stream`. Encoding: always UTF-8.

### Parser State

```nim
type
  BomState = enum
    bsNeedFirst, bsNeedSecond, bsNeedThird, bsDone

  SseParser* = object
    config: SseParserConfig
    dataBuf: string            # Accumulated "data" field values
    eventTypeBuf: string       # Current "event" field value
    lastEventIdBuf: string     # Current "id" value (persists across events)
    retryValue: int            # Latest "retry" value seen, -1 if none
    lineBuf: string            # Partial line accumulator (across chunks)
    lastCharWasCr: bool        # Tracks CR at chunk boundary for CRLF handling
    bomState: BomState         # Incremental BOM detection state machine

proc initSseParser*(config = initSseParserConfig()): SseParser
```

### Public API

```nim
proc push*(parser: var SseParser, chunk: openArray[char],
           onEvent: proc(event: SseEvent))
  ## Feed raw data into the parser. Calls onEvent for each complete event.
  ## Handles chunks that split mid-line or mid-CRLF.

proc push*(parser: var SseParser, chunk: openArray[char]): seq[SseEvent]
  ## Convenience overload. Returns accumulated events from this chunk.
  ## Thin wrapper over the callback form.

proc reset*(parser: var SseParser)
  ## Reset parser state (buffers, BOM state). Keeps config.
```

### Line Ending Handling (spec §9.2.5–6)

Lines end with CR LF, LF, or CR. The parser must handle:
- A CR at the end of one chunk followed by LF at the start of the next
  (this is a single CRLF line ending, not two line endings).
- Mixed line endings within the same stream.

Implementation: `lastCharWasCr` flag. When a CR is seen at the end of
processing, set the flag. On the next `push`, if the first char is LF,
skip it (it's the LF half of a CRLF). If it's anything else, the CR
was a standalone line ending (already processed).

### BOM Handling (spec §9.2.6)

"Streams must be decoded using the UTF-8 decode algorithm", which strips
one leading UTF-8 BOM (U+FEFF = bytes EF BB BF) if present. Only the
very first bytes of the entire stream are checked.

Implementation: a 4-state machine (`bsNeedFirst` → `bsNeedSecond` →
`bsNeedThird` → `bsDone`) that handles BOM bytes split across multiple
`push` calls. On mismatch, any already-consumed partial BOM bytes are
flushed back into `lineBuf` as regular stream content. Once `bsDone` is
reached, no further BOM checking occurs.

### Line Processing (spec §9.2.6)

Once a complete line is extracted:

1. **Empty line** → dispatch event (see below).
2. **Starts with `:` (U+003A)** → comment; ignore the line entirely.
3. **Contains `:`** → split at first `:`. Left side = field name, right
   side = field value. If field value starts with a single U+0020 SPACE,
   remove that space (only the first one).
4. **No `:`** → entire line is the field name, value is empty string.

### Field Processing (spec §9.2.6)

Field names are compared literally (case-sensitive):

| Field name | Action |
|---|---|
| `data` | Append field value to `dataBuf`, then append U+000A LF to `dataBuf`. |
| `event` | Set `eventTypeBuf` to the field value. |
| `id` | If field value does NOT contain U+0000 NULL, set `lastEventIdBuf` to field value. Otherwise ignore. |
| `retry` | If field value consists of only ASCII digits (0-9), parse as base-10 integer and store as `retryValue`. Otherwise ignore. |
| *(anything else)* | Ignore the field. |

### Event Dispatch (spec §9.2.6)

Triggered by an empty line:

1. If `dataBuf` is empty → reset `eventTypeBuf` and `retryValue`,
   return (no event emitted).
2. If `dataBuf` ends with U+000A LF, remove that trailing LF.
3. Emit `SseEvent` with:
   - `data` = `dataBuf`
   - `eventType` = `eventTypeBuf`
   - `id` = `lastEventIdBuf`
   - `retry` = `retryValue`
4. Set `dataBuf` and `eventTypeBuf` to empty string.
   **Do NOT reset `lastEventIdBuf`** — it persists until changed by a
   subsequent `id` field.
5. Reset `retryValue` to -1.

### End of Stream (spec §9.2.6)

"Once the end of the file is reached, any pending data must be discarded."
An incomplete event (no trailing blank line) is never dispatched. The
caller should simply stop calling `push`. The parser's internal buffers
hold the incomplete data which is discarded when the parser goes out of
scope or is reset.

### Security Limits

- On each character appended to `lineBuf`, check against `maxLineLen`.
  If exceeded, raise `SseLimitError`.
- On each append to `dataBuf`, check against `maxEventSize`.
  If exceeded, raise `SseLimitError`.

---

## 3. Writer (`writer.nim`)

Serializes `SseEvent` objects to `text/event-stream` format.

### Public API

```nim
proc serialize*(event: SseEvent): string
  ## Returns full text/event-stream representation of the event,
  ## including the trailing blank line.

proc write*(buf: var string, event: SseEvent)
  ## Appends event to an existing string buffer (avoids allocation).

proc serializeRetry*(ms: int): string
  ## Returns a retry: block that updates reconnection time without
  ## dispatching an event (metadata-only block).

proc writeRetry*(buf: var string, ms: int)
  ## Appends a retry: block to an existing buffer.

proc serializeComment*(text = ""): string
  ## Returns a comment block: ": <text>\n\n"
  ## Useful for keep-alive / heartbeat.

proc writeComment*(buf: var string, text = "")
  ## Appends a comment block to an existing buffer.
```

### Serialization Rules

- If `event.eventType` is non-empty, emit `event: <eventType>\n`.
- If `event.id` is non-empty, emit `id: <id>\n`.
- If `event.retry >= 0`, emit `retry: <retry>\n`.
- Split `event.data` on `\r\n`, `\r`, or `\n` (all three spec line-ending
  forms). For each segment, emit `data: <segment>\n`. If `data` is empty,
  emit a single `data\n` (field with no value, which produces empty data
  on the receiving end).
- Emit a trailing `\n` (blank line) to delimit the event.

Multi-line data example — input `data = "line1\nline2\nline3"` produces:

```
data: line1
data: line2
data: line3

```

### Input Validation

The spec's ABNF grammar restricts field values to `any-char` (excludes
CR and LF). The writer validates single-line fields before emitting:

- `eventType` — raises `SseError` if it contains CR or LF.
- `id` — raises `SseError` if it contains CR, LF, or NULL (a compliant
  parser ignores id fields containing NULL per §9.2.6).

Data fields do not need validation because CR/LF are handled by
splitting into multiple `data:` lines.

### Metadata-Only Blocks

The spec allows blocks like `retry: 5000\n\n` that update client state
without dispatching an event (the parser sees an empty data buffer at
the blank line and skips dispatch). `writeRetry`/`serializeRetry` emit
these blocks. This cannot be achieved through `SseEvent` because the
writer always emits a `data` field for events.

### Representational Limitations

The current `SseEvent` type cannot express "emit an `id` field with
empty value to reset the client's last event ID" — `id = ""` means
"don't emit an id field." This is a deliberate trade-off; the use case
is rare and callers can emit `id\n` manually via buffer append.

---

## 4. Server Helpers (`server.nim`)

Thin layer for sending SSE from an HTTP server. Works with
`std/asynchttpserver` but keeps coupling minimal.

### Constants & Helpers

```nim
const SseContentType* = "text/event-stream"

proc sseHeaders*(): HttpHeaders
  ## Returns standard SSE response headers:
  ##   Content-Type: text/event-stream; charset=utf-8
  ##   Cache-Control: no-cache
  ##   Connection: keep-alive
  ##   X-Accel-Buffering: no
```

`X-Accel-Buffering: no` disables nginx proxy buffering, which is a
common real-world deployment issue (spec §9.2.7 warns about proxy/chunking
problems).

### Heartbeat

Per spec §9.2.7: "authors can include a comment line (one starting with
a ':' character) every 15 seconds or so" to prevent proxy timeout.

The server helpers should provide a mechanism to send periodic keep-alive
comments. Implementation: track the time of the last write. Before each
send, or on a timer, if `heartbeatInterval` has elapsed since the last
write, send a comment line. The default interval is 15 seconds.

---

## 5. Sync Client (`client.nim`)

Full `EventSource`-equivalent implementing the spec §9.2.2–3 processing
model.

### Why Raw Sockets

`std/httpclient` does not reliably expose a streaming response body for
long-lived connections. The client uses `std/net.Socket` directly. SSE's
HTTP needs are minimal (one GET request, read headers, stream body), so
implementing the HTTP request/response framing manually is straightforward
and gives full control over buffering and timeouts.

### Public API

```nim
type
  EventSource* = ref object
    url*: Uri
    readyState*: ReadyState
    onOpen*: proc()
    onMessage*: proc(event: SseEvent)
    onError*: proc(msg: string)
    config: EventSourceConfig
    # internal: socket, parser, reconnect state...

proc newEventSource*(url: string,
                     config = initEventSourceConfig()): EventSource

proc connect*(es: EventSource)
  ## Blocking. Connects to the server, reads events in a loop, and
  ## auto-reconnects per spec. Returns only when the connection is
  ## closed (via close(), cancel token, or fatal error).

proc close*(es: EventSource)
  ## Sets readyState to CLOSED. The connect loop will exit after
  ## the current socket timeout cycle.
```

### Connection Lifecycle (spec §9.2.2–3)

#### Initial Connection

1. Set `readyState` to `CONNECTING`.
2. Open a TCP socket to the server.
3. Send HTTP GET request with headers:
   - `Accept: text/event-stream`
   - `Cache-Control: no-cache`
   - If `lastEventId` is non-empty: `Last-Event-ID: <value>`
4. Read response status and headers.

#### Response Handling

- **200 with `Content-Type: text/event-stream`** → announce the connection:
  set `readyState` to `OPEN`, call `onOpen`. Begin streaming the body
  through the parser.
- **HTTP 204 No Content** → fail the connection. Set `readyState` to
  `CLOSED`, call `onError`. Do NOT reconnect.
- **HTTP 301 / 307** → follow the redirect (update URL, reconnect).
  301 updates the URL permanently; 307 does not.
- **Any other non-200, or wrong `Content-Type`** → fail the connection.
  Set `readyState` to `CLOSED`, call `onError`. Do NOT reconnect.
- **Network error** → reestablish the connection (reconnect), unless
  the error is known to be unrecoverable.
- **Aborted network error** → fail the connection (no reconnect).

#### Reconnection (spec §9.2.3)

1. Set `readyState` to `CONNECTING`.
2. Call `onError`.
3. Wait `reconnectionTime` milliseconds.
4. If `lastEventId` is non-empty, set the `Last-Event-ID` header.
5. Open a new connection.

The reconnection time can be updated by the server via the `retry` field.
The client enforces `minReconnectTime` and `maxReconnectTime` bounds from
the config.

If `maxReconnectAttempts` is set and exceeded, fail the connection.

#### Cancellation

The socket read timeout is set to ~1 second. The read loop checks
`cancelToken.isCancelled` (if a token was provided) and the internal
closed flag after each timeout. This ensures cancellation is responsive
within ~1 second without requiring threads or atomics.

`EventSource.close()` sets the internal closed flag. The read loop
observes it on the next timeout cycle and exits cleanly.

#### Inactivity Timeout

If `config.inactivityTimeout > 0`, the client tracks the timestamp of
the last received data (including comments). If no data arrives within
the timeout window, the client treats the connection as dead and
initiates reconnection.

### Last-Event-ID (spec §9.2.4)

The `Last-Event-ID` HTTP request header is sent when reconnecting.
Its value is the `lastEventId` string maintained by the parser (set by
`id` fields in the event stream). The value is UTF-8 encoded and must
not contain NULL (U+0000), LF (U+000A), or CR (U+000D).

---

## 6. Async Client (`asyncclient.nim`)

Mirrors the sync client API but uses `std/asyncnet.AsyncSocket` and
`std/asyncdispatch`. Each blocking operation becomes an `{.async.}` proc
returning a `Future`.

```nim
type
  AsyncEventSource* = ref object
    url*: Uri
    readyState*: ReadyState
    onOpen*: proc() {.async.}
    onMessage*: proc(event: SseEvent) {.async.}
    onError*: proc(msg: string) {.async.}
    config: EventSourceConfig

proc newAsyncEventSource*(url: string,
                          config = initEventSourceConfig()): AsyncEventSource

proc connect*(es: AsyncEventSource): Future[void]
  ## Async. Connects, reads events, auto-reconnects.
  ## Completes when the connection is closed.

proc close*(es: AsyncEventSource)
```

Cancellation is simpler in async — every `await` is a yield point where
the cancel token and closed flag can be checked.

---

## 7. Testing Strategy

### Parser Tests (`t_parser.nim`)

The parser is the most spec-critical component. Test cases derived directly
from the spec examples in §9.2.6:

| # | Input | Expected |
|---|---|---|
| 1 | `data: YHOO\ndata: +2\ndata: 10\n\n` | One event, data = `"YHOO\n+2\n10"` |
| 2 | `data: first event\nid: 1\n\n` | One event, data = `"first event"`, id = `"1"` |
| 3 | `data:second event\nid\n\n` | One event, data = `"second event"`, id = `""` (reset) |
| 4 | `data:  third event\n\n` | One event, data = `" third event"` (one leading space kept) |
| 5 | `data\n\ndata\ndata\n\ndata:\n` | Two events: data = `""`, data = `"\n"`. Third block discarded (no trailing blank line). |
| 6 | `data:test\n\ndata: test\n\n` | Two identical events, data = `"test"` (space after colon stripped) |
| 7 | `: comment\n\n` | No event (comment + empty dispatch = no data). |

Additional edge case tests:

- **Line endings:** CR, LF, CRLF, mixed within one stream.
- **CR at chunk boundary:** CR as last char of chunk 1, LF as first char of chunk 2 → single CRLF line ending.
- **BOM:** UTF-8 BOM (EF BB BF) at start of stream is stripped. BOM mid-stream is not stripped.
- **`id` with NULL:** `id: abc\x00def\n` → ignored (id not updated).
- **`retry` validation:** `retry: 3000\n` → accepted. `retry: 3.0\n` → ignored. `retry: abc\n` → ignored. `retry: \n` → ignored (empty is not all digits).
- **Unknown fields:** `foo: bar\n` → silently ignored.
- **Field with no colon:** `data\n` → field name = `"data"`, value = `""`.
- **Empty `data` dispatch:** `event: foo\n\n` → no event (data buffer empty, buffers reset).
- **`id` persistence:** `id: 1\n\ndata: x\n\n` → second event still has id = `"1"`.
- **Incremental feeding:** feed the same stream one byte at a time; verify identical output to feeding it all at once.
- **Limit exceeded:** line longer than `maxLineLen` → `SseLimitError`. Data larger than `maxEventSize` → `SseLimitError`.

### Writer Tests (`t_writer.nim`)

- Single-line data serialization.
- Multi-line data splits into multiple `data:` lines.
- Event with all fields set (eventType, id, retry, data).
- Event with only data (minimal).
- Field order verified (event, id, retry, data).
- **CR/CRLF in data:** standalone CR, CRLF, mixed line endings, trailing CR/CRLF — all split correctly into `data:` lines using LF only on the wire.
- **Field validation:** id with LF/CR/NULL raises `SseError`. eventType with LF/CR raises `SseError`. Valid and empty values pass.
- **Metadata-only blocks:** `serializeRetry` produces correct output. Retry block does not dispatch an event when parsed. Retry block followed by event doesn't leak retry value.
- Comment serialization (empty, with text, multi-line, CR/CRLF in comments).
- Buffer append (`write`, `writeComment`, `writeRetry` append to existing buffers).
- Roundtrip: `parse(serialize(event)) == event` for all field combinations.
- Roundtrip: CR/CRLF/mixed line endings in data normalize to LF.
- Roundtrip: id persistence across sequential events (spec behavior).

### Client Tests (`t_client.nim`)

Integration tests using a local `std/asynchttpserver`:

- Successful connection and event receipt.
- Auto-reconnection on connection drop.
- `Last-Event-ID` sent on reconnection.
- HTTP 204 → connection closed, no reconnect.
- HTTP 301/307 → redirect followed.
- Non-200 / wrong content type → connection failed.
- `retry` field updates reconnection time.
- Cancel token stops the connection.
- Inactivity timeout triggers reconnection.
- Security limits enforced through the client.

### Server Tests (`t_server.nim`)

- Correct headers returned by `sseHeaders()`.
- Event serialization through server helpers.
- Heartbeat comment sent after configured interval of inactivity.

---

## 8. Spec Compliance Checklist

### §9.2.2 — EventSource Interface

- [ ] `readyState`: CONNECTING (0), OPEN (1), CLOSED (2)
- [ ] `url` stored at construction time
- [ ] `close()` aborts connection, sets readyState to CLOSED
- [ ] `onopen` callback when connection established
- [ ] `onmessage` callback when event dispatched
- [ ] `onerror` callback on error / reconnection

### §9.2.3 — Processing Model

- [ ] Announce connection: set readyState to OPEN, fire `open`
- [ ] Reestablish connection: set readyState to CONNECTING, fire `error`, wait reconnectionTime, reconnect with Last-Event-ID
- [ ] Fail connection: set readyState to CLOSED, fire `error`, do not reconnect
- [ ] Response validation: must be 200 with Content-Type text/event-stream
- [ ] Network error → reestablish (unless futile)
- [ ] Aborted network error → fail
- [ ] Non-200 or wrong content type → fail

### §9.2.4 — Last-Event-ID Header

- [ ] Sent on reconnection if lastEventId is non-empty
- [ ] Value is UTF-8 encoded
- [ ] Must not contain NULL, LF, or CR

### §9.2.5 — Parsing an Event Stream

- [ ] MIME type: text/event-stream
- [ ] Always UTF-8
- [ ] Line endings: CRLF, LF, or CR
- [ ] BOM at start of stream stripped
- [ ] ABNF grammar followed (stream, event, comment, field productions)

### §9.2.6 — Interpreting an Event Stream

- [ ] Empty line → dispatch event
- [ ] Line starting with `:` → comment, ignored
- [ ] Line with `:` → split at first `:`, strip one leading space from value
- [ ] Line without `:` → field name is whole line, value is empty
- [ ] Field `data` → append value + LF to data buffer
- [ ] Field `event` → set event type buffer
- [ ] Field `id` → set last event ID buffer (unless value contains NULL)
- [ ] Field `retry` → set reconnection time (if all ASCII digits)
- [ ] Unknown fields → ignored
- [ ] Dispatch: skip if data buffer empty; strip trailing LF from data
- [ ] Dispatch resets data buffer and event type buffer, NOT last event ID buffer
- [ ] End of stream discards incomplete event

### §9.2.7 — Authoring Notes (Server Guidance)

- [ ] Keep-alive comments every ~15 seconds to prevent proxy timeout
- [ ] HTTP 204 tells client to stop reconnecting
- [ ] Chunking may cause issues; document this

---

## 9. Implementation Order

Build bottom-up, each layer tested before the next:

1. **`types.nim`** — data model, config, errors, cancel token
2. **`parser.nim`** + **`t_parser.nim`** — the hardest part; exhaustive tests
3. **`writer.nim`** + **`t_writer.nim`** — serialization + roundtrip tests against parser
4. **`server.nim`** + **`t_server.nim`** — thin server helpers
5. **`client.nim`** + **`t_client.nim`** — sync client with full lifecycle
6. **`asyncclient.nim`** — async mirror of sync client
7. **`sse.nim`** — umbrella re-exports
