import type { CursorInvalidDetail, RunEvent } from '../../shared/protocol';
import { isTerminalType } from '../../shared/protocol';
import { abortableSleep, exponentialBackoff, type BackoffPolicy } from './backoff';
import { classifyEvent, initialState, reduce, type Action, type StreamState } from './reducer';
import { readSse } from './sse';

export interface RunStreamOptions {
  runId: string;
  baseUrl?: string;
  fetch?: typeof fetch;
  backoff?: BackoffPolicy;
  sleep?: (ms: number, signal: AbortSignal) => Promise<void>;
  /** Abort and reconnect when no bytes (not even a heartbeat) arrive for this long. */
  idleTimeoutMs?: number;
  /** Consecutive cursor rejections tolerated before giving up. */
  maxResets?: number;
}

type Outcome =
  | { kind: 'terminal' }
  | { kind: 'ended' } // server closed the stream without a terminal event
  | { kind: 'reset'; detail: CursorInvalidDetail }
  | { kind: 'fatal'; error: string };

/**
 * Owns one run's client-side delivery: connects, applies events strictly in
 * seq order, and resumes from its cursor after any interruption.
 *
 * The cursor lives in the reducer state and only advances when an event is
 * applied, so a reconnect can never skip content, and anything already applied
 * is dropped as a duplicate.
 */
export class RunStream {
  private state: StreamState = initialState;
  private readonly listeners = new Set<() => void>();
  private controller: AbortController | null = null;
  private loop: Promise<void> = Promise.resolve();

  private readonly runId: string;
  private readonly baseUrl: string;
  private readonly fetchImpl: typeof fetch;
  private readonly backoff: BackoffPolicy;
  private readonly sleep: (ms: number, signal: AbortSignal) => Promise<void>;
  private readonly idleTimeoutMs: number;
  private readonly maxResets: number;

  constructor(options: RunStreamOptions) {
    this.runId = options.runId;
    this.baseUrl = options.baseUrl ?? '';
    this.fetchImpl = options.fetch ?? ((...args) => globalThis.fetch(...args));
    this.backoff = options.backoff ?? exponentialBackoff();
    this.sleep = options.sleep ?? abortableSleep;
    this.idleTimeoutMs = options.idleTimeoutMs ?? 15_000;
    this.maxResets = options.maxResets ?? 3;
  }

  // Arrow properties so they can be handed straight to useSyncExternalStore.
  getState = (): StreamState => this.state;

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  /** Begin (or continue) streaming from the current cursor. No-op while active. */
  start(): void {
    if (this.controller || this.isTerminal()) return;
    this.launch();
  }

  /** Manual, immediate retry from the current cursor (resets the attempt budget). */
  reconnect(): void {
    if (this.isTerminal()) return;
    this.halt();
    this.launch();
  }

  /** Drop the connection on purpose; the cursor is kept so `reconnect()` resumes. */
  stop(): void {
    if (!this.controller) return;
    this.halt();
    this.dispatch({ type: 'stopped' });
  }

  /** Resolves when the current delivery loop has ended (terminal, gave up, or stopped). */
  whenSettled(): Promise<void> {
    return this.loop;
  }

  private isTerminal(): boolean {
    return this.state.connection === 'closed';
  }

  private halt(): void {
    this.controller?.abort();
    this.controller = null;
  }

  private launch(): void {
    const controller = new AbortController();
    this.controller = controller;
    this.loop = this.run(controller.signal).finally(() => {
      if (this.controller === controller) this.controller = null;
    });
  }

  private dispatch(action: Action): void {
    this.state = reduce(this.state, action);
    for (const listener of [...this.listeners]) listener();
  }

  private async run(signal: AbortSignal): Promise<void> {
    // A stopped/replaced loop must never touch state again.
    const emit = (action: Action) => {
      if (!signal.aborted) this.dispatch(action);
    };
    let attempt = 0;
    let resetsInRow = 0;

    while (!signal.aborted) {
      emit({ type: 'connecting' });
      const round = { connected: false };
      let failure: string;

      try {
        const outcome = await this.consume(signal, emit, round);
        if (signal.aborted) return;
        if (outcome.kind === 'terminal') return;
        if (outcome.kind === 'fatal') return emit({ type: 'gave_up', error: outcome.error });
        if (outcome.kind === 'reset') {
          // The server cannot replay from our cursor. Start over from 0 — unless it
          // has also dropped the early history, in which case replay cannot help.
          if (outcome.detail.reason === 'expired' || ++resetsInRow > this.maxResets) {
            return emit({ type: 'gave_up', error: `cannot resume: cursor ${outcome.detail.requested} is ${outcome.detail.reason}` });
          }
          emit({ type: 'reset' });
          continue;
        }
        failure = 'stream closed before the reply finished';
      } catch (error) {
        if (signal.aborted) return;
        failure = error instanceof Error ? error.message : String(error);
      }

      if (round.connected) attempt = 0; // it worked for a while; the budget starts over
      attempt += 1;
      if (attempt > this.backoff.maxAttempts) {
        return emit({ type: 'gave_up', error: `${failure} (gave up after ${this.backoff.maxAttempts} attempts)` });
      }
      const delayMs = this.backoff.delayMs(attempt);
      emit({ type: 'retry_scheduled', attempt, delayMs, error: failure });
      await this.sleep(delayMs, signal);
    }
  }

  private async consume(signal: AbortSignal, emit: (action: Action) => void, round: { connected: boolean }): Promise<Outcome> {
    const connection = new AbortController();
    const abortConnection = () => connection.abort();
    signal.addEventListener('abort', abortConnection);

    let idleTimer: ReturnType<typeof setTimeout> | undefined;
    let idle = false;
    const armIdleTimer = () => {
      clearTimeout(idleTimer);
      idleTimer = setTimeout(() => {
        idle = true;
        connection.abort();
      }, this.idleTimeoutMs);
    };

    try {
      armIdleTimer();
      const url = `${this.baseUrl}/api/runs/${encodeURIComponent(this.runId)}/events?after=${this.state.cursor}`;
      const response = await this.fetchImpl(url, { signal: connection.signal, headers: { Accept: 'text/event-stream' } });
      if (response.status === 404) return { kind: 'fatal', error: 'run not found' };
      if (!response.ok || !response.body) throw new Error(`unexpected HTTP ${response.status}`);

      round.connected = true;
      const lastSeqHeader = response.headers.get('x-run-last-seq');
      emit({ type: 'connected', ...(lastSeqHeader !== null && /^\d+$/.test(lastSeqHeader) && { serverLastSeq: Number(lastSeqHeader) }) });

      for await (const message of readSse(response.body, armIdleTimer)) {
        if (message.event === 'cursor_invalid') return { kind: 'reset', detail: JSON.parse(message.data) as CursorInvalidDetail };

        const event = JSON.parse(message.data) as RunEvent;
        switch (classifyEvent(this.state.cursor, event.seq)) {
          case 'duplicate':
            emit({ type: 'duplicate' });
            break;
          case 'gap':
            // Never paper over missing events: drop the connection and resume from the cursor.
            throw new Error(`gap in stream: expected seq ${this.state.cursor + 1}, received ${event.seq}`);
          case 'apply':
            emit({ type: 'event', event });
            if (isTerminalType(event.type)) return { kind: 'terminal' };
        }
      }
      return { kind: 'ended' };
    } catch (error) {
      if (idle && !signal.aborted) throw new Error(`no data for ${this.idleTimeoutMs}ms`);
      throw error;
    } finally {
      clearTimeout(idleTimer);
      signal.removeEventListener('abort', abortConnection);
      connection.abort();
    }
  }
}
