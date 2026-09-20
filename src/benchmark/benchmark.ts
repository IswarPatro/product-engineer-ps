import { RunStream } from '../client/core/runStream';
import { startMessage } from '../client/core/api';
import { createApp, listen, shutdownServer } from '../server/http';
import { createFakeGenerator, fakeChunks } from '../server/generator';
import { Notifier } from '../server/notifier';
import { RunManager } from '../server/runManager';
import { RunStore } from '../server/store';

export interface BenchmarkOptions {
  chunkCount?: number;
  /** Sever the connection once the client has applied this many events. */
  dropAtSeq?: number;
  /** Hold the client offline until the server has produced at least this many more events. */
  outageEvents?: number;
  delayMs?: number;
}

export interface BenchmarkReport {
  runId: string;
  expectedChunks: number;
  chunksDisplayed: number;
  missing: number;
  duplicatesDisplayed: number;
  duplicatesDropped: number;
  connections: number;
  /** The `after` cursor of every connection the client opened. */
  resumeCursors: number[];
  eventsProducedDuringOutage: number;
  /** Highest seq the client applied (chunks + the terminal event). */
  finalCursor: number;
  finalRunState: string;
  textMatchesExpected: boolean;
  matchesServerLog: boolean;
  pass: boolean;
}

/**
 * End-to-end correctness benchmark over real HTTP/SSE: a client streams a
 * 40-event reply, loses its connection mid-run, stays offline while the server
 * keeps generating, reconnects from its cursor, and must reconstruct the exact
 * reply — no missing and no duplicate events.
 */
export async function runBenchmark({ chunkCount = 40, dropAtSeq = 12, outageEvents = 8, delayMs = 10 }: BenchmarkOptions = {}): Promise<BenchmarkReport> {
  const store = new RunStore();
  const notifier = new Notifier();
  const manager = new RunManager(store, notifier, createFakeGenerator({ chunkCount, delayMs }));
  const server = createApp({ store, notifier, manager });
  const baseUrl = `http://localhost:${await listen(server)}`;

  // Simulated network: can sever live connections and refuse new ones.
  const network = { down: false, live: new Set<AbortController>() };
  const resumeCursors: number[] = [];
  const flakyFetch: typeof fetch = async (input, init) => {
    if (network.down) throw new TypeError('network down (simulated)');
    resumeCursors.push(Number(new URL(String(input)).searchParams.get('after')));
    const connection = new AbortController();
    network.live.add(connection);
    init?.signal?.addEventListener('abort', () => connection.abort());
    return fetch(input, { ...init, signal: connection.signal });
  };

  try {
    const { run } = await startMessage({ conversationId: 'benchmark', userMessageId: 'benchmark-1', text: 'benchmark' }, baseUrl);

    let eventsProducedDuringOutage = 0;
    const stream = new RunStream({
      runId: run.id,
      baseUrl,
      fetch: flakyFetch,
      // Condition-based, not time-based: stay offline until the server is `outageEvents` ahead of us.
      sleep: async () => {
        const target = stream.getState().cursor + outageEvents;
        await new Promise<void>((resolve) => {
          const unsubscribe = notifier.subscribe(run.id, check);
          function check() {
            if (store.getRun(run.id)!.lastSeq < target) return;
            unsubscribe();
            resolve();
          }
          check();
        });
        eventsProducedDuringOutage = store.getRun(run.id)!.lastSeq - stream.getState().cursor;
        network.down = false;
      },
    });

    let dropped = false;
    stream.subscribe(() => {
      if (dropped || stream.getState().cursor < dropAtSeq) return;
      dropped = true;
      network.down = true;
      for (const connection of network.live) connection.abort();
    });

    stream.start();
    await stream.whenSettled();

    const state = stream.getState();
    const expected = fakeChunks(chunkCount);
    const serverChunks = store.eventsAfter(run.id, 0, chunkCount + 10).filter((e) => e.type === 'chunk').map((e) => e.text);
    const textMatchesExpected = state.chunks.join('') === expected.join('');
    const matchesServerLog = JSON.stringify(state.chunks) === JSON.stringify(serverChunks);
    const report: BenchmarkReport = {
      runId: run.id,
      expectedChunks: expected.length,
      chunksDisplayed: state.chunks.length,
      missing: Math.max(0, expected.length - state.chunks.length),
      duplicatesDisplayed: Math.max(0, state.chunks.length - expected.length),
      duplicatesDropped: state.duplicatesDropped,
      connections: resumeCursors.length,
      resumeCursors,
      eventsProducedDuringOutage,
      finalCursor: state.cursor,
      finalRunState: state.phase,
      textMatchesExpected,
      matchesServerLog,
      pass: false,
    };
    report.pass =
      report.finalRunState === 'completed' &&
      report.missing === 0 &&
      report.duplicatesDisplayed === 0 &&
      textMatchesExpected &&
      matchesServerLog &&
      report.connections >= 2 &&
      report.eventsProducedDuringOutage > 0;
    return report;
  } finally {
    await shutdownServer(server);
    store.close();
  }
}
