---
name: outbox-review
description: Use when reviewing a diff or PR in a codebase that uses outbox-core, to check for the dual-write bug this library exists to prevent — a DB commit followed by a separate, direct publish call outside the outbox. Triggers on requests like "review this PR", "check this diff for outbox issues", or any review of code that both writes to the database and sends a message/event.
---

# Reviewing for outbox regressions

This is a standing check, not a one-time fix. The library removes the dual-write
bug from the code paths that already use it correctly — it does nothing to
stop a *new* call site from reintroducing the same bug by bypassing the outbox
entirely. That's what this skill looks for in a diff.

## The pattern to flag

Any hunk that adds (or modifies) code where, in the same logical operation:

1. A domain write is committed to the database (`session.commit()`, or an ORM
   flow that implies one), **and**
2. A message/event/notification is sent through something that is **not**
   `OutboxWriter.enqueue()` on that same session before the commit —
   a direct queue client, an HTTP call to a webhook, an SDK's `.publish()`
   or `.send()`, a Celery task dispatch that fires a downstream notification,
   etc.

That combination is the bug, full stop, regardless of how it's dressed up —
"just for this one urgent case," a try/except around the publish call, a
background task that fires after commit. All of those preserve the exact
failure mode: commit succeeds, publish doesn't, and the event is gone with no
record that it was ever supposed to happen.

## What a correct call site looks like, for contrast

```python
await writer.enqueue(session, OutboxMessage(topic=..., payload=...))
await session.commit()
```

If a diff instead shows something like:

```python
await session.commit()
await queue_client.send_message(...)  # <-- flag this
```

...or the publish happens in a `finally:` block, a background task, an event
handler fired after commit, or any other place structurally separate from the
transaction — flag it, regardless of how reasonable the surrounding code
looks.

## Two more regressions worth the same scrutiny

- **`enqueue()` misused.** `writer.enqueue(session, ...)` called *after*
  `session.commit()`, or on a *different* session than the domain write it's
  paired with. The call looks outbox-shaped but reintroduces the same gap:
  the outbox row is no longer atomic with the data it describes.
- **A provider that swallows failures.** A `MessageProvider.send()`
  implementation that catches the transport SDK's exception and returns
  normally (or retries internally before giving up). The relay's
  retry/dead-letter state machine is driven entirely by whether `send()`
  raised — a swallowed exception marks the row `delivered` without a real
  delivery. Same bug, relocated into the provider.

## What's fine and shouldn't be flagged

- Calls to `Relay.poll_once()` / `Relay.run_forever()` — that's the delivery
  side of the outbox itself, not a bypass of it.
- A `MessageProvider.send()` implementation calling the real transport SDK —
  that's expected; it only runs from inside the relay, on already-committed
  outbox rows.
- Reads, queries, or any code that doesn't pair a DB write with a
  notification in the same operation.
- Genuinely independent operations that happen to both touch the DB and send
  a message, where the message isn't *about* the DB write (no consistency
  requirement between them).

## How to report a finding

State the failure scenario concretely, the way you would for any other
correctness bug: *if the process crashes (or the publish call fails/times
out) between this commit and this send call, the row is durably persisted but
the event is never delivered, and \[whatever redelivery/dedup logic exists\]
will skip it on retry.* Point at the specific commit and the specific
publish call. Suggest the fix: move the publish into an `OutboxWriter.enqueue()`
call on the same session, before the commit.
