import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { startMessage } from '../src/client/core/api';
import { RunStream } from '../src/client/core/runStream';
import { createApp, listen, shutdownServer } from '../src/server/http';
import { RunStore } from '../src/server/store';
import { createGatedGenerator, createHarness, untilSeq } from './helpers';

const cleanups: Array<() => void | Promise<void>> = [];
afterEach(async () => {
  while (cleanups.length) await cleanups.pop()!();
});

const untilState = (stream: RunStream, predicate: (s: ReturnType<RunStream['getState']>) => boolean) =>
  new Promise<void>((resolve) => {
    const check = () => predicate(stream.getState()) && (unsubscribe(), resolve());
    const unsubscribe = stream.subscribe(check);
    check();
  });

describe('HTTP API', () => {
  it('starts a run idempotently and rejects a second concurrent reply in the same conversation', async () => {
    const gen = createGatedGenerator(3);
    const h = createHarness(gen.generator);
    const server = createApp(h);
    const baseUrl = `http://localhost:${await listen(server)}`;
    cleanups.push(() => shutdownServer(server));

    const first = await startMessage({ conversationId: 'c', userMessageId: 'm1', text: 'hi' }, baseUrl);
    const again = await startMessage({ conversationId: 'c', userMessageId: 'm1', text: 'hi' }, baseUrl);
    expect(first.created).toBe(true);
    expect(again).toMatchObject({ created: false, run: { id: first.run.id } });
    await expect(startMessage({ conversationId: 'c', userMessageId: 'm2', text: 'hi' }, baseUrl)).rejects.toMatchObject({ status: 409 });
    expect((await fetch(`${baseUrl}/api/runs/missing/events`)).status).toBe(404);

    // The events endpoint tells a connecting client where the log currently stands.
    const events = await fetch(`${baseUrl}/api/runs/${first.run.id}/events?after=0`);
    expect(events.headers.get('x-run-last-seq')).toBe('0');
    await events.body!.cancel();
  });
});

describe('AC4 over the wire: service restart while a client is connected', () => {
  it('reconnects with backoff after the process dies and receives the interrupted state instead of a new run', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'restart-http-'));
    cleanups.push(() => rmSync(dir, { recursive: true, force: true }));
    const file = join(dir, 'db.sqlite');

    // Process #1
    const gen = createGatedGenerator(10);
    const one = createHarness(gen.generator, new RunStore(file));
    const server1 = createApp(one);
    const port = await listen(server1);
    const baseUrl = `http://localhost:${port}`;
    const { run } = await startMessage({ conversationId: 'c', userMessageId: 'm1', text: 'hi' }, baseUrl);

    // The client's backoff sleep is a gate the test opens once "process #2" is up.
    let openGate!: () => void;
    const gate = new Promise<void>((resolve) => (openGate = resolve));
    const stream = new RunStream({ runId: run.id, baseUrl, sleep: () => gate, backoff: { maxAttempts: 5, delayMs: () => 0 } });
    stream.start();
    cleanups.push(() => stream.stop());

    gen.release(3);
    await untilSeq(one, run.id, 3);
    await untilState(stream, (s) => s.cursor === 3);

    // Kill process #1: connections severed, database closed, generator gone.
    await shutdownServer(server1);
    one.store.close();
    await untilState(stream, (s) => s.connection === 'reconnecting');
    expect(stream.getState()).toMatchObject({ cursor: 3, phase: 'running' }); // honest about being stale

    // Process #2 boots on the same database and port, running the boot recovery step.
    const two = createHarness(createGatedGenerator(10).generator, new RunStore(file));
    two.store.interruptRunning();
    const server2 = createApp(two);
    await listen(server2, port);
    cleanups.push(async () => {
      await shutdownServer(server2);
      two.store.close();
    });

    openGate();
    await stream.whenSettled();

    expect(stream.getState()).toMatchObject({ phase: 'interrupted', connection: 'closed', cursor: 4, duplicatesDropped: 0 });
    expect(stream.getState().chunks).toHaveLength(3);
    expect(two.store.getRun(run.id)!.status).toBe('interrupted');
  });
});
