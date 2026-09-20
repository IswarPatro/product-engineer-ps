import { fakeChunk, type Generator } from '../src/server/generator';
import { Notifier } from '../src/server/notifier';
import { RunManager } from '../src/server/runManager';
import { RunStore } from '../src/server/store';
import { streamRun, type StreamOutcome } from '../src/server/streamRun';
import type { RunEvent, StreamFrame } from '../src/shared/protocol';

/** A generator that only advances when the test says so — no timers, no sleeps. */
export function createGatedGenerator(total: number) {
  let released = 0;
  let failure: Error | null = null;
  let waiters: Array<() => void> = [];
  const wakeAll = () => {
    const current = waiters;
    waiters = [];
    current.forEach((wake) => wake());
  };

  const generator: Generator = async function* () {
    for (let i = 0; i < total; i++) {
      while (i >= released && !failure) await new Promise<void>((resolve) => waiters.push(resolve));
      if (i >= released && failure) throw failure;
      yield fakeChunk(i);
    }
  };

  return {
    generator,
    /** Allow `count` more chunks to be produced. */
    release(count: number) {
      released += count;
      wakeAll();
    },
    /** Make the generator throw as soon as it has produced everything released so far. */
    fail(error = new Error('boom')) {
      failure = error;
      wakeAll();
    },
  };
}

export function createHarness(generator: Generator, store = new RunStore()) {
  const notifier = new Notifier();
  const manager = new RunManager(store, notifier, generator);
  return { store, notifier, manager };
}

/** Resolves once the run has persisted at least `seq` events. Event-driven. */
export function untilSeq(h: { store: RunStore; notifier: Notifier }, runId: string, seq: number): Promise<void> {
  return new Promise((resolve) => {
    const check = () => {
      if (h.store.getRun(runId)!.lastSeq < seq) return;
      unsubscribe();
      resolve();
    };
    const unsubscribe = h.notifier.subscribe(runId, check);
    check();
  });
}

/** Subscribes to a run through streamRun and records every frame it delivers. */
export function openStream(h: { store: RunStore; notifier: Notifier }, runId: string, after?: string) {
  const frames: StreamFrame[] = [];
  const abort = new AbortController();
  const done: Promise<StreamOutcome> = streamRun({
    ...h,
    runId,
    after,
    signal: abort.signal,
    sink: { write: (frame) => frames.push(frame) },
  });
  return {
    frames,
    done,
    abort: () => abort.abort(),
    events: (): RunEvent[] => frames.flatMap((f) => (f.kind === 'event' ? [f.event] : [])),
    seqs() {
      return this.events().map((e) => e.seq);
    },
  };
}

export const range = (from: number, to: number): number[] => Array.from({ length: to - from + 1 }, (_, i) => from + i);
