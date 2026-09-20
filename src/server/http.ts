import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import type { StartMessageRequest, StreamFrame } from '../shared/protocol';
import { ConflictError, type RunManager } from './runManager';
import type { Notifier } from './notifier';
import type { RunStore } from './store';
import { streamRun } from './streamRun';

export interface AppDeps {
  store: RunStore;
  notifier: Notifier;
  manager: RunManager;
  /** SSE comment interval; lets clients (and proxies) detect a dead connection. */
  heartbeatMs?: number;
}

const MAX_BODY_BYTES = 64 * 1024;

export function formatFrame(frame: StreamFrame): string {
  if (frame.kind === 'cursor_invalid') return `event: cursor_invalid\ndata: ${JSON.stringify(frame.detail)}\n\n`;
  const { event } = frame;
  return `id: ${event.seq}\nevent: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`;
}

export function createApp({ store, notifier, manager, heartbeatMs = 5000 }: AppDeps): Server {
  return createServer((req, res) => {
    handle(req, res).catch((error) => {
      console.error('request failed', error);
      if (!res.headersSent) sendJson(res, 500, { error: 'internal error' });
      else res.end();
    });
  });

  async function handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const url = new URL(req.url ?? '/', 'http://localhost');
    const path = url.pathname;

    if (req.method === 'POST' && path === '/api/messages') return postMessage(req, res);

    const runMatch = /^\/api\/runs\/([^/]+)(\/events)?$/.exec(path);
    if (req.method === 'GET' && runMatch) {
      const runId = decodeURIComponent(runMatch[1]!);
      if (runMatch[2]) return getEvents(req, res, runId, url);
      const run = store.getRun(runId);
      return run ? sendJson(res, 200, run) : sendJson(res, 404, { error: 'run not found' });
    }

    sendJson(res, 404, { error: 'not found' });
  }

  async function postMessage(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const body = parseStartRequest(await readJson(req));
    if (!body) return sendJson(res, 400, { error: 'conversationId, userMessageId and text are required strings' });
    try {
      const result = manager.start(body);
      sendJson(res, result.created ? 201 : 200, result);
    } catch (error) {
      if (error instanceof ConflictError) return sendJson(res, 409, { error: error.message, activeRunId: error.activeRunId });
      throw error;
    }
  }

  async function getEvents(req: IncomingMessage, res: ServerResponse, runId: string, url: URL): Promise<void> {
    const run = store.getRun(runId);
    if (!run) return sendJson(res, 404, { error: 'run not found' });

    // `?after=` wins; Last-Event-ID lets a plain EventSource resume too.
    const header = req.headers['last-event-id'];
    const after = url.searchParams.get('after') ?? (typeof header === 'string' ? header : undefined);

    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no',
      // Where the log stands at connect time, so a client can tell how much it has to catch up on.
      'X-Run-Last-Seq': String(run.lastSeq),
    });
    res.write(': connected\n\n');

    const abort = new AbortController();
    res.on('close', () => abort.abort());
    const heartbeat = setInterval(() => res.write(': ping\n\n'), heartbeatMs);
    try {
      await streamRun({ store, notifier, runId, after, signal: abort.signal, sink: { write: (frame) => res.write(formatFrame(frame)) } });
    } finally {
      clearInterval(heartbeat);
      res.end();
    }
  }
}

function parseStartRequest(value: unknown): StartMessageRequest | null {
  if (typeof value !== 'object' || value === null) return null;
  const { conversationId, userMessageId, text } = value as Record<string, unknown>;
  const valid = (s: unknown, max: number): s is string => typeof s === 'string' && s.trim() !== '' && s.length <= max;
  if (!valid(conversationId, 200) || !valid(userMessageId, 200) || !valid(text, 10_000)) return null;
  return { conversationId, userMessageId, text };
}

async function readJson(req: IncomingMessage): Promise<unknown> {
  let size = 0;
  const chunks: Buffer[] = [];
  for await (const chunk of req as AsyncIterable<Buffer>) {
    size += chunk.length;
    if (size > MAX_BODY_BYTES) return null;
    chunks.push(chunk);
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString('utf8'));
  } catch {
    return null;
  }
}

function sendJson(res: ServerResponse, status: number, body: unknown): void {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(body));
}

/** Listens on `port` (0 = ephemeral) and resolves with the bound port. */
export function listen(server: Server, port = 0): Promise<number> {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, () => resolve((server.address() as AddressInfo).port));
  });
}

/** Stops accepting connections, severs open streams, and waits for shutdown. */
export function shutdownServer(server: Server): Promise<void> {
  return new Promise((resolve) => {
    server.close(() => resolve());
    server.closeAllConnections();
  });
}
