import { isTerminalStatus, isTerminalType, type CursorInvalidDetail, type StreamFrame } from '../shared/protocol';
import type { Notifier } from './notifier';
import type { RunStore } from './store';

export interface StreamSink {
  write(frame: StreamFrame): void;
}

export type StreamOutcome = 'ended' | 'aborted' | 'cursor_invalid' | 'not_found';

export interface StreamRunOptions {
  store: RunStore;
  notifier: Notifier;
  runId: string;
  /** Raw cursor as received on the wire (undefined = from the beginning). */
  after: string | undefined;
  sink: StreamSink;
  signal: AbortSignal;
}

/** Returns the cursor as a number, or the reason it cannot safely be used. */
export function checkCursor(raw: string | undefined, oldestSeq: number, lastSeq: number): number | CursorInvalidDetail {
  const requested = raw ?? '0';
  const detail = (reason: CursorInvalidDetail['reason']): CursorInvalidDetail => ({ reason, requested, oldestSeq, lastSeq });
  if (!/^\d{1,15}$/.test(requested)) return detail('invalid');
  const after = Number(requested);
  if (after > lastSeq) return detail('ahead');
  if (after < oldestSeq - 1) return detail('expired');
  return after;
}

/**
 * Replay-then-live delivery for one subscriber.
 *
 * Replay and live are the same code path: the loop repeatedly reads "events
 * after lastSent" from the durable log and writes them in seq order. The
 * notifier only wakes the loop; it never carries events. That removes the
 * replay/live overlap race by construction — an event can only be sent when
 * its seq is exactly the next one after `lastSent`, so nothing is skipped or
 * sent twice, no matter how appends interleave with reads.
 */
export async function streamRun({ store, notifier, runId, after, sink, signal }: StreamRunOptions): Promise<StreamOutcome> {
  const run = store.getRun(runId);
  if (!run) return 'not_found';

  const cursor = checkCursor(after, run.oldestSeq, run.lastSeq);
  if (typeof cursor !== 'number') {
    sink.write({ kind: 'cursor_invalid', detail: cursor });
    return 'cursor_invalid';
  }

  // Latch wake-ups so one arriving between "read log" and "wait" is never lost.
  let pending = false;
  let wake: (() => void) | null = null;
  const signalWake = () => {
    if (wake) {
      const resolve = wake;
      wake = null;
      resolve();
    } else {
      pending = true;
    }
  };
  // Subscribe BEFORE the first read so appends during replay still wake us.
  const unsubscribe = notifier.subscribe(runId, signalWake);
  signal.addEventListener('abort', signalWake);

  let lastSent = cursor;
  try {
    for (;;) {
      pending = false;
      // Status is read before the log: if it is already terminal, the terminal
      // event is in the log we are about to read, so an empty batch means done.
      const status = store.getRun(runId)!.status;
      const batch = store.eventsAfter(runId, lastSent);

      for (const event of batch) {
        if (signal.aborted) return 'aborted';
        sink.write({ kind: 'event', event });
        lastSent = event.seq;
        if (isTerminalType(event.type)) return 'ended';
      }

      if (signal.aborted) return 'aborted';
      if (batch.length > 0) continue; // the batch may have been capped; read again
      if (isTerminalStatus(status)) return 'ended';
      if (!pending) await new Promise<void>((resolve) => (wake = resolve));
    }
  } finally {
    unsubscribe();
    signal.removeEventListener('abort', signalWake);
  }
}
