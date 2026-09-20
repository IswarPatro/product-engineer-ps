import { expect, it } from 'vitest';
import { runBenchmark } from '../src/benchmark/benchmark';

it('benchmark: 40 events, one mid-run interruption, zero missing and zero duplicate events', async () => {
  const report = await runBenchmark({ chunkCount: 40, dropAtSeq: 12, outageEvents: 8, delayMs: 2 });

  expect(report).toMatchObject({
    expectedChunks: 40,
    chunksDisplayed: 40,
    missing: 0,
    duplicatesDisplayed: 0,
    duplicatesDropped: 0,
    finalCursor: 41,
    finalRunState: 'completed',
    textMatchesExpected: true,
    matchesServerLog: true,
    pass: true,
  });
  expect(report.connections).toBeGreaterThanOrEqual(2);
  expect(report.eventsProducedDuringOutage).toBeGreaterThan(0);
  expect(report.resumeCursors[0]).toBe(0);
  expect(report.resumeCursors[1]).toBeGreaterThanOrEqual(12); // resumed from its checkpoint, not from scratch
}, 15_000);
