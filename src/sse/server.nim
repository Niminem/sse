import std/[httpcore, monotimes, times]
import ./types, ./writer

const SseContentType* = "text/event-stream"

type
  SseConnection* = object
    buf*: string
    lastWriteTime*: MonoTime
    config*: SseServerConfig

proc sseHeaders*(): HttpHeaders =
  result = newHttpHeaders([
    ("Content-Type", "text/event-stream; charset=utf-8"),
    ("Cache-Control", "no-cache"),
    ("Connection", "keep-alive"),
    ("X-Accel-Buffering", "no")
  ])

proc initSseConnection*(config = initSseServerConfig()): SseConnection =
  result = SseConnection(
    buf: "",
    lastWriteTime: getMonoTime(),
    config: config
  )

proc needsHeartbeat*(conn: SseConnection): bool =
  if conn.config.heartbeatInterval <= 0: return false
  let elapsed = (getMonoTime() - conn.lastWriteTime).inMilliseconds
  result = elapsed >= conn.config.heartbeatInterval

proc maybeSendHeartbeat*(conn: var SseConnection): bool =
  ## Sends a heartbeat comment if the configured interval has elapsed
  ## since the last write. Returns true if a heartbeat was sent.
  if conn.needsHeartbeat():
    conn.buf.writeComment("")
    conn.lastWriteTime = getMonoTime()
    result = true

proc sendEvent*(conn: var SseConnection, event: SseEvent) =
  discard conn.maybeSendHeartbeat()
  conn.buf.write(event)
  conn.lastWriteTime = getMonoTime()

proc sendComment*(conn: var SseConnection, text = "") =
  conn.buf.writeComment(text)
  conn.lastWriteTime = getMonoTime()

proc sendRetry*(conn: var SseConnection, ms: int) =
  conn.buf.writeRetry(ms)
  conn.lastWriteTime = getMonoTime()

proc flush*(conn: var SseConnection): string =
  ## Returns the buffered data and clears the internal buffer.
  result = conn.buf
  conn.buf = ""
