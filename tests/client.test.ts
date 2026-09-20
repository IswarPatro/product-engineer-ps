import { describe, expect, it } from 'vitest';
import { exponentialBackoff } from '../src/client/core/backoff';
import { classifyEvent, initialState, reduce } from '../src/client/core/reducer';
import { RunStream } from '../src/client/core/runStream';
import { readSse } from '../src/client/core/sse';
import { formatFrame } from '../src/server/http';
import type { CursorInvalidDetail, RunEvent } from '../src/shared/protocol';

const chunk = (seq: number): RunEvent => ({ runId: 'r', seq, type: 'chunk', text: `t${seq} ` });
const terminal = (seq: number, type: 'completed' | 'failed', error?: string): RunEvent => ({ runId: 'r', seq, type, ...(error && { error }) });

/** A scripted server: each connection replays the next canned response. */
function scriptedFetch(responses: Array<Array<RunEvent | { invalid: CursorInvalidDetail } | 'hang'>>) {
  const urls: string[] = [];
  const fetchImpl = (async (input: string | URL | Request, init?: RequestInit) => {
    urls.push(String(input));
    const script = responses[urls.length - 1] ?? [];
    const encoder = new TextEncoder();
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        for (const item of script) {
          if (item === 'hang') return void init?.signal?.addEventListener('abort', () => controller.error(new DOMException('aborted', 'AbortError')));
          controller.enqueue(encoder.encode('invalid' in item ? `event: cursor_invalid\ndata: ${JSON.stringify(item.invalid)}\n\n` : formatFrame({ kind: 'event', event: item })));
        }
        controller.close();
      },
    });
    return new Response(body, { status: 200 });
  }) as typeof fetch;
  return { fetchImpl, urls };
}

const instantSleep = () => Promise.resolve();
const quickBackoff = { maxAttempts: 3, delayMs: () => 0 };

describe('reducer', () => {
  it('applies only the next seq, drops duplicates, and refuses to skip a gap', () => {
    expect(classifyEvent(3, 4)).toBe('apply');
    expect(classifyEvent(3, 3)).toBe('duplicate');
    expect(classifyEvent(3, 2)).toBe('duplicate');
    expect(classifyEvent(3, 5)).toBe('gap');

    let state = reduce(initialState, { type: 'event', event: chunk(1) });
    state = reduce(state, { type: 'event', event: chunk(1) });
    state = reduce(state, { type: 'event', event: chunk(3) });
    expect(state.chunks).toEqual(['t1 ']);
    expect(state.cursor).toBe(1);
  });

  it('ignores anything after a terminal event', () => {
    let state = reduce(initialState, { type: 'event', event: chunk(1) });
    state = reduce(state, { type: 'event', event: terminal(2, 'failed', 'boom') });
    state = reduce(state, { type: 'event', event: terminal(3, 'completed') });
    state = reduce(state, { type: 'event', event: chunk(3) });
    expect(state).toMatchObject({ phase: 'failed', connection: 'closed', cursor: 2, error: 'boom' });
  });
});

describe('SSE parsing', () => {
  it('reassembles frames split across reads and skips heartbeat comments', async () => {
    const wire = `: ping\n\n${formatFrame({ kind: 'event', event: chunk(1) })}${formatFrame({ kind: 'event', event: chunk(2) })}`;
    const bytes = new TextEncoder().encode(wire.replace(/\n/g, '\r\n'));
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        for (let i = 0; i < bytes.length; i += 7) controller.enqueue(bytes.slice(i, i + 7)); // awkward 7-byte reads
        controller.close();
      },
    });
    const seen: string[] = [];
    for await (const message of readSse(body)) seen.push(`${message.id}:${message.event}`);
    expect(seen).toEqual(['1:chunk', '2:chunk']);
  });
});

describe('backoff', () => {
  it('grows exponentially, is capped, and stays within its jittered band', () => {
    const low = exponentialBackoff({ baseMs: 100, capMs: 1000, random: () => 0 });
    const high = exponentialBackoff({ baseMs: 100, capMs: 1000, random: () => 1 });
    expect([1, 2, 3, 4, 5, 6].map((n) => low.delayMs(n))).toEqual([50, 100, 200, 400, 500, 500]);
    expect([1, 2, 3, 4, 5, 6].map((n) => high.delayMs(n))).toEqual([100, 200, 400, 800, 1000, 1000]);
  });
});

describe('RunStream against a scripted server', () => {
  it('drops replayed duplicates so each event is displayed once', async () => {
    const { fetchImpl } = scriptedFetch([[chunk(1), chunk(2), chunk(2), chunk(1), chunk(3), terminal(4, 'completed')]]);
    const stream = new RunStream({ runId: 'r', fetch: fetchImpl, sleep: instantSleep, backoff: quickBackoff });
    stream.start();
    await stream.whenSettled();

    expect(stream.getState()).toMatchObject({ phase: 'completed', cursor: 4, duplicatesDropped: 2 });
    expect(stream.getState().chunks.join('')).toBe('t1 t2 t3 ');
  });

  it('never skips a gap: it drops the connection and resumes from its cursor', async () => {
    const { fetchImpl, urls } = scriptedFetch([
      [chunk(1), chunk(2), chunk(4)], // seq 3 missing
      [chunk(3), chunk(4), terminal(5, 'completed')],
    ]);
    const stream = new RunStream({ runId: 'r', fetch: fetchImpl, sleep: instantSleep, backoff: quickBackoff });
    stream.start();
    await stream.whenSettled();

    expect(urls.map((u) => new URL(u, 'http://x').searchParams.get('after'))).toEqual(['0', '2']);
    expect(stream.getState().chunks.join('')).toBe('t1 t2 t3 t4 ');
    expect(stream.getState().phase).toBe('completed');
  });

  it('resets and replays from 0 when the server rejects its cursor as ahead', async () => {
    const invalid: CursorInvalidDetail = { reason: 'ahead', requested: '2', oldestSeq: 1, lastSeq: 0 };
    const { fetchImpl, urls } = scriptedFetch([[chunk(1), chunk(2), 'hang'], [{ invalid }], [chunk(1), terminal(2, 'completed')]]);
    const stream = new RunStream({ runId: 'r', fetch: fetchImpl, sleep: instantSleep, backoff: quickBackoff, idleTimeoutMs: 5 });
    stream.start();
    await stream.whenSettled();

    expect(urls.map((u) => new URL(u, 'http://x').searchParams.get('after'))).toEqual(['0', '2', '0']);
    expect(stream.getState()).toMatchObject({ phase: 'completed', resets: 1 });
    expect(stream.getState().chunks.join('')).toBe('t1 ');
  });

  it('gives up visibly (disconnected, with the reason) once its reconnect budget is spent', async () => {
    const failing = (async () => {
      throw new TypeError('offline');
    }) as typeof fetch;
    const stream = new RunStream({ runId: 'r', fetch: failing, sleep: instantSleep, backoff: quickBackoff });
    stream.start();
    await stream.whenSettled();

    expect(stream.getState()).toMatchObject({ connection: 'disconnected', phase: 'unknown' });
    expect(stream.getState().error).toMatch(/offline.*gave up after 3 attempts/);
  });

  it('keeps its cursor across a manual disconnect and resumes on reconnect()', async () => {
    const { fetchImpl, urls } = scriptedFetch([[chunk(1), chunk(2), 'hang'], [chunk(3), terminal(4, 'completed')]]);
    const stream = new RunStream({ runId: 'r', fetch: fetchImpl, sleep: instantSleep, backoff: quickBackoff });
    const atCursor2 = new Promise<void>((resolve) => stream.subscribe(() => stream.getState().cursor === 2 && resolve()));
    stream.start();
    await atCursor2;

    stream.stop();
    expect(stream.getState().connection).toBe('disconnected');

    stream.reconnect();
    await stream.whenSettled();
    expect(urls.map((u) => new URL(u, 'http://x').searchParams.get('after'))).toEqual(['0', '2']);
    expect(stream.getState()).toMatchObject({ phase: 'completed', connection: 'closed' });
    expect(stream.getState().chunks.join('')).toBe('t1 t2 t3 ');
    expect(stream.getState().timeline).toEqual([
      'connected from cursor 0',
      'disconnected at cursor 2',
      'reconnected from cursor 2',
      'completed at cursor 4',
    ]);
  });

  it('records loss, retry and catch-up size in the timeline when the server reports where its log stands', async () => {
    const encoder = new TextEncoder();
    const respond = (events: RunEvent[], lastSeq: number) =>
      new Response(
        new ReadableStream<Uint8Array>({
          start(controller) {
            for (const event of events) controller.enqueue(encoder.encode(formatFrame({ kind: 'event', event })));
            controller.close();
          },
        }),
        { status: 200, headers: { 'x-run-last-seq': String(lastSeq) } },
      );
    const responses = [respond([chunk(1)], 0), respond([chunk(2), chunk(3), terminal(4, 'completed')], 4)]; // first closes early
    const fetchImpl = (async () => responses.shift()!) as typeof fetch;

    const stream = new RunStream({ runId: 'r', fetch: fetchImpl, sleep: instantSleep, backoff: { maxAttempts: 3, delayMs: () => 7 } });
    stream.start();
    await stream.whenSettled();

    expect(stream.getState().timeline).toEqual([
      'connected from cursor 0',
      'connection lost at cursor 1 (stream closed before the reply finished); retry 1 in 7ms',
      'reconnected from cursor 1, 3 events to catch up',
      'completed at cursor 4',
    ]);
  });
});
