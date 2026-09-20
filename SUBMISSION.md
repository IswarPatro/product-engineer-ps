# Product Engineering Challenge Submission

## Candidate

- **Name:** TODO
- **Email:** TODO
- **GitHub:** TODO
- **Selected problem:** Problem 1 — Resumable realtime conversation
- **Demo video:** TODO (paste an accessible Loom / YouTube / Drive link here)

## Run the project

Prerequisites: **Node ≥ 22.13** (uses the built-in `node:sqlite`, so there is no native module to compile). No API keys or environment secrets are needed. The reply generator is a deterministic fake, and its text is intentionally independent of the user's message (every reply is `The-1 quick-2 … The-40`), so that ordering, replay, deduplication and failure recovery can be verified exactly. The only message-dependent behaviour is the documented `/fail` prefix, which makes the generator fail after 10 events.

Cursor semantics: the cursor shown in the UI is the highest event sequence number the client has applied (a finished reply shows `cursor 41` = 40 text events + the terminal `completed` event). Reconnecting sends `after=<cursor>` and receives events with `seq > cursor`.

```bash
npm install
npm run dev          # service on :8787 + web client on http://localhost:5173
```

Optional environment variables (all have defaults): `PORT` (8787), `DB_PATH` (`data/companion.db`), `CHUNK_COUNT` (40), `CHUNK_DELAY_MS` (120).

**Successful scenario (AC1):** open http://localhost:5173, send any message. The reply streams in 40 events; the badge goes `connecting → connected → completed`, and the cursor counts up.

**Recovery scenarios** (each reproducible in the UI):

| Scenario | How to trigger | What you should see |
| --- | --- | --- |
| Interruption + missed events (AC2/AC3) | Click **Simulate dropped connection** mid-reply, wait a few seconds, click **Reconnect from cursor N** | Badge `disconnected`; the server keeps generating; on reconnect the text continues from event N+1 with no repeats. |
| Service restart (AC4) | Kill the service (Ctrl-C the `dev:server` process, or `npm run dev` and restart it) mid-reply, then start it again | Badge shows `reconnecting (attempt n)` with a backoff countdown; after the restart the reply ends as `interrupted` with its partial text preserved. Reload the page: history is replayed from the durable log. |
| Generation failure (AC5) | Send a message that starts with `/fail` | The fake generator throws after 10 events; badge `failed`; the 10 events stay visible and inspectable. |
| Stale cursor (AC6) | Covered by tests (`tests/server.test.ts`, `tests/client.test.ts`) | Explicit `cursor_invalid` frame; client resyncs from 0, or stops with a clear reason if history was pruned. |

Each reply has an expandable **Connection timeline** under it that narrates recovery, for example: `connected from cursor 0` → `connection lost at cursor 12 (…); retry 1 in 300ms` → `reconnected from cursor 12, 8 events to catch up` → `completed at cursor 41`. The catch-up size comes from an `X-Run-Last-Seq` header the service sends when a stream opens.

For a production-like run of the client: `npm run build` (output in `dist/client`).

## Run the tests

```bash
npm test             # 27 deterministic tests, no network services, no sleeps
npm run typecheck
```

## Acceptance scenarios and verification

All six acceptance scenarios are implemented and covered by tests:

| AC | Behaviour | Where it is tested |
| --- | --- | --- |
| AC1 | Ordered live stream, each event once, ends `completed` | `tests/server.test.ts` › AC1 |
| AC2 | Reconnect from a cursor yields exactly the events after it | › AC2 (+ `RunStream` resume tests) |
| AC3 | Replay/live overlap yields one ordered response | › AC3 (injects appends *during* the replay read; redundant wake-ups) |
| AC4 | Restart keeps history; unfinished run becomes `interrupted`; the same message does not start a new run | `tests/server.test.ts` › AC4, and `tests/http.test.ts` (real HTTP, real process-style restart while a client is connected) |
| AC5 | Generator failure → `failed`, history intact, can never become `completed` | › AC5 |
| AC6 | Invalid / ahead / expired cursors get an explicit `cursor_invalid` frame | › AC6, `tests/client.test.ts` |

**Verification benchmark** (a real HTTP/SSE run, not mocked):

```bash
npm run benchmark
```

Observed output on my machine (the `resumed from cursors` value can vary by a few events between runs; the pass criteria do not):

```text
connections opened            2 (resumed from cursors: 0 -> 12)
events produced while offline 8
text events expected          40
text events displayed         40
missing                       0
duplicates displayed          0
duplicates dropped by client  0
final cursor (incl. terminal) 41
final run state               completed
text == expected reply        true
client == server event log    true

RESULT: PASS
```

The same scenario runs as an automated test in `tests/benchmark.test.ts`. The interruption is condition-based: the client is severed at event 12 and held offline until the server has produced at least 8 more events, then reconnects from its cursor.

**Demo failure/recovery path:** service restart (client reconnects with backoff and receives `interrupted`) and `/fail` generation failure.

## Architecture and data flow

```text
  React UI ──useSyncExternalStore──> RunStream (reconnect loop) ──> reducer (cursor, dedupe, state)
                                          │  fetch + SSE, GET /api/runs/:id/events?after=<cursor>
                                          ▼
                           http.ts ──> streamRun (replay→live loop) ◄── Notifier (wake-ups only)
                              │                     │ reads                    ▲ notify after commit
                    POST /api/messages              ▼                          │
                              └────────> RunManager ──append──> RunStore (SQLite: runs + events)
                                          │ drives                 ▲ assigns seq, guards terminal state
                                          ▼
                                    Generator (fake)
```

- **Durable vs transient.** `RunStore` (SQLite, WAL) is the only durable state: a `runs` table and an append-only `events` table keyed by `(run_id, seq)`. Connections, subscribers and the notifier are in-memory and disposable.
- **`RunManager`** starts a run (idempotent on `userMessageId`, one active run per conversation) and drives the generator, persisting each event before announcing it.
- **`streamRun`** is transport-agnostic (it writes to a sink) and owns replay→live delivery. `http.ts` only does SSE framing, heartbeats and routing.
- **Client** is split into a pure `reducer` (state transitions and dedupe rules), `RunStream` (I/O, backoff, resume) and thin React components. The core has no React dependency, which is what makes it testable without a browser.

## Technology choices

- **TypeScript end to end**, with the wire types in `src/shared/protocol.ts` shared by client and server.
- **SSE over WebSockets.** Traffic is one-directional (server → client); user messages are ordinary `POST`s. SSE has a built-in notion of event ids and resume, works through plain HTTP infrastructure, and needs no custom framing. Trade-off: no client→server channel on the same connection, so future features like cancellation would use a second request. I use `fetch` streaming rather than `EventSource` so the client controls backoff, sees failures, and can act on `cursor_invalid`; the server still honours `Last-Event-ID`.
- **SQLite via `node:sqlite`.** Real durability and transactions with zero infrastructure and no native build step. Trade-off: single-writer, single process (see limitations). The experimental-API warning printed by Node 22 is expected.
- **Plain `node:http`.** The API is three routes; a framework would add surface without adding value.
- **Vite + React** for the client; **Vitest** for tests.

## Important decisions

Documented as requested by the brief:

- **What is an event / cursor?** An event is `{runId, seq, type, text?, error?}`. `seq` is a gapless per-run integer. The cursor is "the highest seq the client has applied"; resuming with `after=N` returns exactly `seq > N`. Terminal states (`completed`, `failed`, `interrupted`) are events in the same log, so they are ordered, durable and replayable, and a client cannot miss the ending.
- **Who owns ordering?** `RunStore.append` alone. It assigns `seq = last_seq + 1` and inserts the event, updating the run's status, in one transaction. It refuses to write to a run that is already terminal, so a `failed` run can never later become `completed` (tested).
- **How replay transitions to live.** They are the *same code path*. `streamRun` loops "read events after `lastSent` from the log → send → wait". The notifier carries no data, only a wake-up (latched, so one arriving between the read and the wait is not lost, and subscribed *before* the first read). Live delivery therefore cannot disagree with replay, and the overlap race is removed by construction rather than handled by a merge step. Trade-off: one small indexed query per wake-up, which is fine here and is where an in-memory tail cache would go at scale.
- **Where deduplication happens.** The server never emits an out-of-order or repeated seq to one connection (`lastSent` gate). The client independently enforces the contract in the reducer: apply only `cursor + 1`, drop `seq ≤ cursor` as a duplicate (counted and shown), and on `seq > cursor + 1` treat it as a gap, drop the connection and resume from the cursor rather than skipping.
- **What survives a restart.** Everything in SQLite: runs, events, statuses. **Policy:** an in-flight generator does *not* resume (it lived in the dead process). At boot, `interruptRunning()` appends a durable `interrupted` event to every run still marked running. Clients that reconnect get the persisted history followed by that explicit terminal event. Retrying the same `userMessageId` returns the original run rather than starting an unrelated one. A run is never left looking alive, and never silently restarted.
- **Reconnect delay and bounds.** Capped exponential backoff with equal jitter (250 ms base, 5 s cap), at most 8 consecutive failed attempts (the budget resets after a connection that worked). After that the client shows `disconnected` with the reason and a manual **Reconnect** button. A connection that is open but silent is treated as dead after 15 s without bytes; the server sends a heartbeat comment every 5 s.
- **Unknown / stale cursors.** The server answers with an explicit `cursor_invalid` frame (`invalid`, `ahead` or `expired`) instead of replaying. On `invalid`/`ahead` the client discards its local text and replays from 0 (bounded to 3 resets). On `expired` it stops with a clear message, since replay from 0 cannot succeed either.
- **Retention.** The prototype keeps all events forever. `RunStore.prune(runId, keepLast)` exists and the expiry path is tested, but nothing calls it automatically.

**Follow-up: if the server kept only the last 50 events and a client came back with an older cursor.** `checkCursor` already detects this (`cursor < oldestSeq - 1` → `expired`), so the client is told explicitly instead of silently missing text. The missing piece is what to do next, and replay cannot help because the early events are gone. I would persist a compacted snapshot alongside the log (the accumulated text through seq `S`, updated as events are pruned) and have the server answer `expired` with `{snapshotSeq, snapshotText}`. The client would replace its local text with the snapshot and continue from `snapshotSeq`, so the reconnect stays lossless without retaining every event forever. Today the client stops with a clear error in that case.

## Assumptions and limitations

- One service process owns the database (single writer). `interruptRunning()` at boot assumes no other live process; multiple servers would need leases/heartbeats and a shared broker instead of the in-process notifier.
- One active reply per conversation (out of scope: parallel runs); a second concurrent message gets `409`.
- The client stores turns and the conversation id in `localStorage` so a reload replays history from the server. Clearing storage loses the mapping (not the server data). Reloaded turns replay from cursor 0 rather than a stored cursor: simpler and still correct, at the cost of re-downloading the reply.
- No authentication, no real model provider, no cancellation, no automatic retention.
- The UI is deliberately plain; it exists to make connection and run states visible.
- Node's `node:sqlite` is marked experimental in Node 22.

## Production and scale

Changes I would make first, in order (none of these are implemented):

1. **Retention with snapshots** (above), plus a TTL on finished runs.
2. **Multi-instance safety.** Run ownership via a lease so only one worker drives a generator, a shared Postgres log, and a broker (Redis/NATS/Postgres `LISTEN`) in place of the in-process notifier. The wake-up-then-read design carries over unchanged, because the notifier already only signals.
3. **Resumable generation.** Store provider-side checkpoints so an interrupted run can resume instead of ending `interrupted`.
4. **Auth and per-conversation authorization** on both endpoints; rate limits on `POST /api/messages`.
5. **Observability:** metrics for reconnect rate, replay size, and time-to-first-event after resume, and structured logs keyed by `runId`/`seq`.

## AI usage

- **Tool:** Claude Code (Anthropic) was used to scaffold and write the implementation, tests and this document, working from a design I agreed to beforehand (SSE + SQLite event log; replay and live as one code path).
- **Review and verification:** the behaviour is checked by the deterministic test suite and the benchmark above, which I ran (27 tests, repeated runs for flakiness, a manual smoke test against the real service including a restart).
- TODO (candidate): confirm you have read and can explain each module, and adjust this section to describe your own review honestly.

## Credibility note

TODO (candidate): describe one product or system you previously helped ship — the problem it solved, your personal contribution, scale/operational complexity, one difficult engineering or product decision, and a public link or other evidence if available.
