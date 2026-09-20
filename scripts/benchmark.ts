import { runBenchmark } from '../src/benchmark/benchmark';

const report = await runBenchmark();

console.log('Verification benchmark: resumable stream with one mid-run interruption');
console.log('-----------------------------------------------------------------------');
console.log(`run id                        ${report.runId}`);
console.log(`connections opened            ${report.connections} (resumed from cursors: ${report.resumeCursors.join(' -> ')})`);
console.log(`events produced while offline ${report.eventsProducedDuringOutage}`);
console.log(`text events expected          ${report.expectedChunks}`);
console.log(`text events displayed         ${report.chunksDisplayed}`);
console.log(`missing                       ${report.missing}`);
console.log(`duplicates displayed          ${report.duplicatesDisplayed}`);
console.log(`duplicates dropped by client  ${report.duplicatesDropped}`);
console.log(`final cursor (incl. terminal) ${report.finalCursor}`);
console.log(`final run state               ${report.finalRunState}`);
console.log(`text == expected reply        ${report.textMatchesExpected}`);
console.log(`client == server event log    ${report.matchesServerLog}`);
console.log(report.pass ? '\nRESULT: PASS' : '\nRESULT: FAIL');
process.exit(report.pass ? 0 : 1);
