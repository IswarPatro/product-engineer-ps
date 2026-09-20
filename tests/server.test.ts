import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { fakeChunk } from '../src/server/generator';
import { ConflictError } from '../src/server/runManager';
import { RunStore } from '../src/server/store';
import { createGatedGenerator, createHarness, openStream, range, untilSeq } from './helpers';

const request = (userMessageId: string, conversationId = 'c1') => ({ conversationId, userMessageId, text: 'hello' });

describe('AC1: ordered live stream', () => {
  it('delivers every event once, in seq order, and ends on completed', async () => {
    const gen = createGatedGenerator(5);
    const h = createHarness(gen.generator);
    const { run } = h.manager.start(request('m1'));

    const stream = openStream(h, run.id); // subscribed before anything is generated
    for (let i = 1; i <= 5; i++) {
      gen.release(1);
      await untilSeq(h, run.id, i);
    }

    expect(await stream.done).toBe('ended');
    expect(stream.seqs()).toEqual(range(1, 6));
    expect(stream.events().map((e) => e.type)).toEqual(['chunk', 'chunk', 'chunk', 'chunk', 'chunk', 'completed']);
    expect(stream.events().slice(0, 5).map((e) => e.text)).toEqual([0, 1, 2, 3, 4].map(fakeChunk));
    expect(h.store.getRun(run.id)!.status).toBe('completed');
  });
});

describe('AC2: missed-event recovery', () => {
  it('replays exactly the events after the cursor, with no gaps or repeats', async () => {
    const gen = createGatedGenerator(10);
    const h = createHarness(gen.generator);
    const { run } = h.manager.start(request('m1'));

    gen.release(4);
    await untilSeq(h, run.id, 4);
    const first = openStream(h, run.id);
    expect(first.seqs()).toEqual(range(1, 4));
    first.abort(); // connection drops; the client has applied through seq 4
    const cursor = 4;

    gen.release(6); // produced while the client is away
    await h.manager.idle();

    const resumed = openStream(h, run.id, String(cursor));
    expect(await resumed.done).toBe('ended');
    expect(resumed.seqs()).toEqual(range(5, 11));
    expect(resumed.events().at(-1)?.type).toBe('completed');
  });

  it('sends nothing but ends cleanly when the cursor is already at a terminal event', async () => {
    const gen = createGatedGenerator(2);
    const h = createHarness(gen.generator);
    const { run } = h.manager.start(request('m1'));
    gen.release(2);
    await h.manager.idle();

    const stream = openStream(h, run.id, '3'); // seq 3 is the completed event
    expect(await stream.done).toBe('ended');
    expect(stream.frames).toEqual([]);
  });
});

describe('AC3: replay/live overlap', () => {
  it('appends that land while replay is being read are delivered once and in order', async () => {
    const gen = createGatedGenerator(20);
    const h = createHarness(gen.generator);
    const { run } = h.manager.start(request('m1'));
    gen.release(3);
    await untilSeq(h, run.id, 3);

    // Worst-case race: two events are appended and announced right after the
    // replay batch was read but before the subscriber waits for live events.
    const realEventsAfter = h.store.eventsAfter.bind(h.store);
    let injected = false;
    h.store.eventsAfter = (runId, after, limit) => {
      const batch = realEventsAfter(runId, after, limit);
      if (!injected) {
        injected = true;
        h.store.append(runId, { type: 'chunk', text: fakeChunk(3) });
        h.store.append(runId, { type: 'chunk', text: fakeChunk(4) });
        h.notifier.notify(runId);
        h.notifier.notify(runId); // redundant wake-ups must not cause duplicates
      }
      return batch;
    };

    const stream = openStream(h, run.id, '0');
    await untilSeq(h, run.id, 5);
    h.store.append(run.id, { type: 'completed' });
    h.notifier.notify(run.id);

    expect(await stream.done).toBe('ended');
    expect(stream.seqs()).toEqual(range(1, 6));
    expect(new Set(stream.seqs()).size).toBe(stream.seqs().length);
  });

  it('two subscribers at different cursors each get a contiguous, consistent view', async () => {
    const gen = createGatedGenerator(6);
    const h = createHarness(gen.generator);
    const { run } = h.manager.start(request('m1'));
    gen.release(3);
    await untilSeq(h, run.id, 3);

    const early = openStream(h, run.id, '0');
    const late = openStream(h, run.id, '2');
    gen.release(3);
    await h.manager.idle();

    expect(await early.done).toBe('ended');
    expect(await late.done).toBe('ended');
    expect(early.seqs()).toEqual(range(1, 7));
    expect(late.seqs()).toEqual(range(3, 7));
  });
});

describe('AC4: service restart', () => {
  let dir: string | undefined;
  afterEach(() => dir && rmSync(dir, { recursive: true, force: true }));

  it('keeps persisted history, marks the unfinished run interrupted, and does not start an unrelated run', async () => {
    dir = mkdtempSync(join(tmpdir(), 'restart-'));
    const file = join(dir, 'db.sqlite');

    const gen = createGatedGenerator(10);
    const first = createHarness(gen.generator, new RunStore(file));
    const { run } = first.manager.start(request('m1'));
    gen.release(3);
    await untilSeq(first, run.id, 3);
    first.store.close(); // process dies mid-reply; its generator is never resumed

    const restarted = createHarness(createGatedGenerator(10).generator, new RunStore(file));
    expect(restarted.store.getRun(run.id)!.status).toBe('running'); // stale until boot recovery runs
    expect(restarted.store.interruptRunning()).toEqual([run.id]);

    const recovered = restarted.store.getRun(run.id)!;
    expect(recovered.status).toBe('interrupted');
    expect(recovered.lastSeq).toBe(4);

    // A reconnecting client resumes from its cursor and learns the outcome explicitly.
    const stream = openStream(restarted, run.id, '2');
    expect(await stream.done).toBe('ended');
    expect(stream.seqs()).toEqual([3, 4]);
    expect(stream.events().map((e) => e.type)).toEqual(['chunk', 'interrupted']);

    // Retrying the same user message finds the old run instead of starting a new reply.
    const retry = restarted.manager.start(request('m1'));
    expect(retry.created).toBe(false);
    expect(retry.run.id).toBe(run.id);

    // A completed run is untouched by recovery.
    const done = createGatedGenerator(1);
    const second = createHarness(done.generator, restarted.store);
    const finished = second.manager.start(request('m2'));
    done.release(1);
    await second.manager.idle();
    expect(second.store.interruptRunning()).toEqual([]);
    expect(second.store.getRun(finished.run.id)!.status).toBe('completed');
    restarted.store.close();
  });
});

describe('AC5: generation failure', () => {
  it('becomes failed after partial output, keeps its history, and can never complete later', async () => {
    const gen = createGatedGenerator(10);
    const h = createHarness(gen.generator);
    const { run } = h.manager.start(request('m1'));
    gen.release(3);
    gen.fail(new Error('model exploded'));
    await h.manager.idle();

    const state = h.store.getRun(run.id)!;
    expect(state.status).toBe('failed');
    const events = h.store.eventsAfter(run.id, 0);
    expect(events.map((e) => e.type)).toEqual(['chunk', 'chunk', 'chunk', 'failed']);
    expect(events.at(-1)?.error).toBe('model exploded');

    expect(h.store.append(run.id, { type: 'completed' })).toBeNull();
    expect(h.store.append(run.id, { type: 'chunk', text: 'late' })).toBeNull();
    expect(h.store.getRun(run.id)!.status).toBe('failed');
    expect(h.store.eventsAfter(run.id, 0)).toEqual(events);

    // The failed history is replayable, ending in the failed event.
    const stream = openStream(h, run.id, '0');
    expect(await stream.done).toBe('ended');
    expect(stream.events().at(-1)?.type).toBe('failed');
  });
});

describe('AC6: unknown or stale cursor', () => {
  async function finishedRun(count: number) {
    const gen = createGatedGenerator(count);
    const h = createHarness(gen.generator);
    const { run } = h.manager.start(request('m1'));
    gen.release(count);
    await h.manager.idle();
    return { h, run };
  }

  it.each([
    ['abc', 'invalid'],
    ['-1', 'invalid'],
    ['1.5', 'invalid'],
    ['999', 'ahead'],
  ])('rejects cursor %j as %s instead of replaying', async (cursor, reason) => {
    const { h, run } = await finishedRun(3);
    const stream = openStream(h, run.id, cursor);
    expect(await stream.done).toBe('cursor_invalid');
    expect(stream.frames).toEqual([{ kind: 'cursor_invalid', detail: expect.objectContaining({ reason, requested: cursor, lastSeq: 4 }) }]);
  });

  it('reports expired when retention pruned events the client still needs', async () => {
    const { h, run } = await finishedRun(9); // events 1..10
    h.store.prune(run.id, 5); // keeps 6..10

    const stale = openStream(h, run.id, '2');
    expect(await stale.done).toBe('cursor_invalid');
    expect(stale.frames[0]).toMatchObject({ kind: 'cursor_invalid', detail: { reason: 'expired', oldestSeq: 6, lastSeq: 10 } });

    const boundary = openStream(h, run.id, '5'); // oldest retained is 6, so 5 is still safe
    expect(await boundary.done).toBe('ended');
    expect(boundary.seqs()).toEqual(range(6, 10));
  });

  it('reports an unknown run', async () => {
    const h = createHarness(createGatedGenerator(1).generator);
    expect(await openStream(h, 'nope').done).toBe('not_found');
  });
});

describe('starting messages', () => {
  it('is idempotent per userMessageId and allows one active run per conversation', () => {
    const gen = createGatedGenerator(3);
    const h = createHarness(gen.generator);
    const first = h.manager.start(request('m1'));
    expect(first.created).toBe(true);
    expect(h.manager.start(request('m1'))).toEqual({ run: first.run, created: false });
    expect(() => h.manager.start(request('m2'))).toThrow(ConflictError);
    expect(h.manager.start(request('m3', 'another-conversation')).created).toBe(true);
  });
});
