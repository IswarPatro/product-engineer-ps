import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { createFakeGenerator } from './generator';
import { createApp } from './http';
import { Notifier } from './notifier';
import { RunManager } from './runManager';
import { RunStore } from './store';

const port = Number(process.env.PORT ?? 8787);
const dbPath = process.env.DB_PATH ?? 'data/companion.db';
const chunkCount = Number(process.env.CHUNK_COUNT ?? 40);
const delayMs = Number(process.env.CHUNK_DELAY_MS ?? 120);

mkdirSync(dirname(dbPath), { recursive: true });
const store = new RunStore(dbPath);

// Restart policy: generators do not survive the process, so runs that were
// still running are moved to a clear, durable `interrupted` state.
const interrupted = store.interruptRunning();
if (interrupted.length > 0) console.log(`marked ${interrupted.length} unfinished run(s) as interrupted`);

const notifier = new Notifier();
const manager = new RunManager(store, notifier, createFakeGenerator({ chunkCount, delayMs }), (message, error) => console.error(message, error));
const server = createApp({ store, notifier, manager });

server.listen(port, () => console.log(`service listening on http://localhost:${port} (db: ${dbPath})`));

const shutdown = () => {
  server.closeAllConnections();
  server.close(() => {
    store.close();
    process.exit(0);
  });
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
