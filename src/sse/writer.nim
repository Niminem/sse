## Pure (no I/O) serializer for the Server-Sent Events wire format.
##
## Produces valid `text/event-stream` bytes from structured data. This is the
## server-side dual of `parser.nim`. Like the parser, it imports only `types`
## and has no other dependencies.
##
## All procs return `string` (the serialized wire bytes). No I/O, no side effects.
##
## Line-ending safety: the SSE wire format treats CR, LF, and CRLF as line
## terminators with no escape mechanism. All serialization procs therefore
## split multi-segment values on all three forms, and single-line fields
## (`event`, `id`) have CR/LF stripped to prevent wire corruption.

import types

const
  LineEndChars = {'\r', '\n'}
  IdStripChars = {'\0', '\r', '\n'}

func sanitize(s: string; strip: set[char]): string =
  result = newStringOfCap(s.len)
  for ch in s:
    if ch notin strip:
      result.add(ch)

func emitLines(result: var string; prefix: string; value: string) =
  ## Split `value` on all line endings (CR, LF, CRLF) and emit one
  ## `prefix`-prefixed line per segment. A trailing line ending in the
  ## value produces a final empty segment.
  var i = 0
  while true:
    var j = i
    while j < value.len and value[j] notin LineEndChars:
      inc j

    result.add(prefix)
    if j > i:
      result.add(value[i ..< j])
    result.add('\n')

    if j >= value.len:
      break

    if value[j] == '\r':
      inc j
      if j < value.len and value[j] == '\n':
        inc j
    else:
      inc j

    i = j

    if i >= value.len:
      result.add(prefix)
      result.add('\n')
      break

func serializeEvent*(event: SseEvent): string =
  ## Serialize a full `SseEvent` to wire format, terminated by a blank line.
  ##
  ## - If `eventType` is non-empty and not `"message"`, emits an `event:` line.
  ##   CR/LF characters in the type are stripped to prevent wire corruption.
  ## - If `lastEventId` is non-empty, emits an `id:` line. NULL, CR, and LF
  ##   characters are stripped (spec §7.3 forbids them in IDs).
  ## - Splits `data` on all line endings (CR, LF, CRLF) and emits a `data:`
  ##   line per segment.
  ## - Terminates with a trailing blank line (`\n`) to trigger client dispatch.
  let cleanType = sanitize(event.eventType, LineEndChars)
  if cleanType.len > 0 and cleanType != "message":
    result.add("event: ")
    result.add(cleanType)
    result.add('\n')

  let cleanId = sanitize(event.lastEventId, IdStripChars)
  if cleanId.len > 0:
    result.add("id: ")
    result.add(cleanId)
    result.add('\n')

  emitLines(result, "data: ", event.data)

  result.add('\n')

func serializeComment*(comment: string): string =
  ## Emit one or more comment lines (`:` prefix). If the comment contains
  ## line endings (CR, LF, CRLF), emits one comment line per segment. No
  ## trailing blank line (comments don't dispatch events on their own).
  emitLines(result, ": ", comment)

func serializeRetry*(ms: int): string =
  ## Emit a `retry:` line with the given millisecond value. The value must be
  ## non-negative; negative values are clamped to 0. No trailing blank line.
  result.add("retry: ")
  result.add($(max(ms, 0)))
  result.add('\n')

func serializeId*(id: string): string =
  ## Emit a standalone `id:` line. Useful for resetting the last event ID
  ## without an accompanying event. NULL, CR, and LF characters are stripped
  ## (spec §7.3 forbids them in IDs). No trailing blank line.
  result.add("id: ")
  result.add(sanitize(id, IdStripChars))
  result.add('\n')
