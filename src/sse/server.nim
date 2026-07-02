## High-level helpers for serving SSE streams.
##
## Sits between the pure wire serializer (`writer.nim`) and whatever transport
## the caller is using (e.g. `std/asynchttpserver`, raw sockets, etc.). Manages
## per-connection state: auto-incrementing event IDs, a bounded replay buffer
## for reconnection support, and keep-alive tracking.
##
## All procs are pure (return `string`). No I/O, no sockets, no async. The
## caller is responsible for writing the returned bytes to the transport.
##
## Thread safety: `SseConnection` is a mutable value type. Under single-
## threaded async (cooperative multitasking) it is safe without locks. Under
## multi-threaded use the caller must synchronize access.
##
## `httpHeaders` uses `\r\n` line endings (HTTP requirement). All SSE payload
## procs use `\n` (SSE wire format).

import types, writer
import std/deques

type
  HistoryEntry = object
    id: string
    wire: string

  SseConnection* = object
    ## Represents one open SSE stream to a client.
    ##
    ## Tracks auto-incrementing event IDs, retains a bounded history of
    ## recently sent events for replay on reconnection, and supports
    ## keep-alive interval tracking.
    nextId*: int              ## Next auto-assigned event ID (starts at 1).
    lastId*: string           ## Most recently sent event ID.
    lastActivityMs*: int      ## Monotonic timestamp (ms) of last send;
                              ## caller-managed via `markActivity`.
    keepAliveIntervalMs*: int ## Threshold for `needsKeepAlive` (default 15 000).
    history: Deque[HistoryEntry]
    historyLimit: int
    buffer: string

func initSseConnection*(historyLimit = 0;
                         keepAliveIntervalMs = 15_000;
                         nowMs = 0): SseConnection =
  ## Create a new connection.
  ##
  ## `historyLimit` controls the maximum number of events retained for replay
  ## (0 disables the replay buffer). `keepAliveIntervalMs` sets the threshold
  ## for `needsKeepAlive` (default 15 000 ms per spec §9.1). `nowMs` seeds
  ## `lastActivityMs` so the first `needsKeepAlive` check doesn't spuriously
  ## fire; pass the current monotonic time at connection establishment.
  result = SseConnection(
    nextId: 1,
    keepAliveIntervalMs: keepAliveIntervalMs,
    historyLimit: historyLimit,
    lastActivityMs: nowMs,
  )

# ---------------------------------------------------------------------------
# Event formatting
# ---------------------------------------------------------------------------

func formatEvent*(conn: var SseConnection; data: string; eventType = "";
                  id = ""): string =
  ## Build a complete event block ready for the wire.
  ##
  ## Every event produced by this proc carries an ID:
  ## - If `id` is non-empty, that explicit value is used.
  ## - If `id` is empty, an auto-incremented integer ID is assigned from
  ##   `conn.nextId` (and the counter is advanced).
  ##
  ## The event is appended to the replay buffer when `historyLimit > 0`.
  ## Use `serializeEvent` from `writer` directly if you need an event
  ## with no ID at all.
  var effectiveId: string
  if id.len > 0:
    effectiveId = id
  else:
    effectiveId = $conn.nextId
    inc conn.nextId
  conn.lastId = effectiveId
  result = serializeEvent(
    SseEvent(eventType: eventType, data: data, lastEventId: effectiveId))
  if conn.historyLimit > 0:
    if conn.history.len >= conn.historyLimit:
      conn.history.popFirst()
    conn.history.addLast(HistoryEntry(id: effectiveId, wire: result))

func formatComment*(comment: string): string =
  ## Serialize one or more comment lines. Delegates to `serializeComment`.
  result = serializeComment(comment)

func formatRetry*(ms: int): string =
  ## Serialize a `retry:` line. Delegates to `serializeRetry`.
  result = serializeRetry(ms)

func formatId*(conn: var SseConnection; id: string): string =
  ## Emit a standalone `id:` line and update `conn.lastId`.
  conn.lastId = id
  result = serializeId(id)

# ---------------------------------------------------------------------------
# Replay buffer
# ---------------------------------------------------------------------------

func hasEvent*(conn: SseConnection; id: string): bool =
  ## Check whether the replay buffer contains an event with the given `id`.
  result = false
  for entry in conn.history:
    if entry.id == id:
      return true

func replay*(conn: SseConnection; lastEventId: string): string =
  ## Return the wire bytes for all events sent after `lastEventId`.
  ##
  ## If `lastEventId` is found in the buffer, returns everything after it.
  ## If it is not found (client is too stale or buffer is empty), all
  ## buffered events are returned — the caller can use `hasEvent` to
  ## distinguish between the two cases when that matters.
  var afterIdx = -1
  for i in 0 ..< conn.history.len:
    if conn.history[i].id == lastEventId:
      afterIdx = i
      break
  let startIdx = if afterIdx >= 0: afterIdx + 1 else: 0
  for i in startIdx ..< conn.history.len:
    result.add(conn.history[i].wire)

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

func httpHeaders*(contentType = "text/event-stream";
                  extraHeaders: openArray[(string, string)] = []): string =
  ## Build the HTTP response header block for an SSE stream.
  ##
  ## Includes status line, `Content-Type`, `Cache-Control`, and `Connection`
  ## headers. Terminated by `\r\n\r\n`. `extraHeaders` allows the caller to
  ## add CORS or custom headers.
  result.add("HTTP/1.1 200 OK\r\n")
  result.add("Content-Type: " & contentType & "\r\n")
  result.add("Cache-Control: no-cache\r\n")
  result.add("Connection: keep-alive\r\n")
  for (name, value) in extraHeaders:
    result.add(name & ": " & value & "\r\n")
  result.add("\r\n")

func eqIgnoreAsciiCase(a, b: char): bool {.inline.} =
  let al = if a in 'A'..'Z': chr(ord(a) + 32) else: a
  let bl = if b in 'A'..'Z': chr(ord(b) + 32) else: b
  result = al == bl

func parseLastEventId*(headers: string): string =
  ## Extract the `Last-Event-ID` value from raw HTTP request headers.
  ##
  ## Performs a case-insensitive match on the header name (per HTTP/1.1
  ## semantics). Returns the header value with leading and trailing
  ## whitespace trimmed, or `""` if the header is not present.
  const target = "last-event-id:"
  var i = 0
  while i < headers.len:
    let lineStart = i
    while i < headers.len and headers[i] != '\r' and headers[i] != '\n':
      inc i
    let lineEnd = i
    if i < headers.len:
      if headers[i] == '\r':
        inc i
        if i < headers.len and headers[i] == '\n':
          inc i
      else:
        inc i

    let lineLen = lineEnd - lineStart
    if lineLen >= target.len:
      var matches = true
      for j in 0 ..< target.len:
        if not eqIgnoreAsciiCase(headers[lineStart + j], target[j]):
          matches = false
          break
      if matches:
        var valStart = lineStart + target.len
        while valStart < lineEnd and headers[valStart] in {' ', '\t'}:
          inc valStart
        var valEnd = lineEnd
        while valEnd > valStart and headers[valEnd - 1] in {' ', '\t'}:
          dec valEnd
        return headers[valStart ..< valEnd]
  result = ""

# ---------------------------------------------------------------------------
# Keep-alive tracking
# ---------------------------------------------------------------------------

func markActivity*(conn: var SseConnection; nowMs: int) =
  ## Record that data was written to the transport at `nowMs` (caller-provided
  ## monotonic time in milliseconds).
  conn.lastActivityMs = nowMs

func needsKeepAlive*(conn: SseConnection; nowMs: int): bool =
  ## Check whether a keep-alive comment is due given the current time `nowMs`.
  ## Returns `true` when `nowMs - lastActivityMs >= keepAliveIntervalMs`.
  result = nowMs - conn.lastActivityMs >= conn.keepAliveIntervalMs

func keepAliveComment*(): string =
  ## Returns a keep-alive comment suitable for a ~15 s heartbeat ping (spec §9.1).
  result = ": keepalive\n"

# ---------------------------------------------------------------------------
# Stream builder / batching
# ---------------------------------------------------------------------------

func add*(conn: var SseConnection; chunk: string) =
  ## Append `chunk` to the connection's internal send buffer. Call `flush`
  ## to retrieve the accumulated bytes and clear the buffer.
  conn.buffer.add(chunk)

func flush*(conn: var SseConnection): string =
  ## Return the contents of the internal send buffer and clear it.
  result = conn.buffer
  conn.buffer = ""
