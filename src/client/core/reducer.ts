import { isTerminalType, type RunEvent } from '../../shared/protocol';

export type Connection = 'idle' | 'connecting' | 'connected' | 'reconnecting' | 'disconnected' | 'closed';
export type RunPhase = 'unknown' | 'running' | 'completed' | 'failed' | 'interrupted';

export interface StreamState {
  connection: Connection;
  /** What the client knows about the run. `unknown` until an event arrives. */
  phase: RunPhase;
  /** Highest seq applied; the resume checkpoint. */
  cursor: number;
  /** Applied chunk texts in seq order (terminal events add none). */
  chunks: string[];
  /** Consecutive failed reconnect attempts. */
  attempt: number;
  nextRetryMs: number | null;
  error: string | null;
  duplicatesDropped: number;
  /** Times the client discarded local state because the server rejected its cursor. */
  resets: number;
  /** Successful connections so far (2+ means we have resumed at least once). */
  connects: number;
  /** Human-readable connection history, newest last, for making recovery visible. */
  timeline: string[];
}

const MAX_TIMELINE = 50;

export const initialState: StreamState = {
  connection: 'idle',
  phase: 'unknown',
  cursor: 0,
  chunks: [],
  attempt: 0,
  nextRetryMs: null,
  error: null,
  duplicatesDropped: 0,
  resets: 0,
  connects: 0,
  timeline: [],
};

export type Action =
  | { type: 'connecting' }
  | { type: 'connected'; serverLastSeq?: number }
  | { type: 'event'; event: RunEvent }
  | { type: 'duplicate' }
  | { type: 'retry_scheduled'; attempt: number; delayMs: number; error: string }
  | { type: 'gave_up'; error: string }
  | { type: 'stopped' }
  | { type: 'reset' };

/** How an incoming event relates to the cursor. */
export function classifyEvent(cursor: number, seq: number): 'apply' | 'duplicate' | 'gap' {
  if (seq === cursor + 1) return 'apply';
  return seq <= cursor ? 'duplicate' : 'gap';
}

export function reduce(state: StreamState, action: Action): StreamState {
  switch (action.type) {
    case 'connecting':
      return { ...state, connection: state.attempt > 0 ? 'reconnecting' : 'connecting', nextRetryMs: null };
    case 'connected': {
      const missed = action.serverLastSeq === undefined ? 0 : Math.max(0, action.serverLastSeq - state.cursor);
      const resumed = state.connects > 0;
      const detail = missed > 0 ? `, ${missed} event${missed === 1 ? '' : 's'} to catch up` : '';
      return note(
        { ...state, connection: 'connected', attempt: 0, nextRetryMs: null, error: null, connects: state.connects + 1 },
        `${resumed ? 'reconnected' : 'connected'} from cursor ${state.cursor}${detail}`,
      );
    }
    case 'duplicate':
      return { ...state, duplicatesDropped: state.duplicatesDropped + 1 };
    case 'retry_scheduled':
      return note(
        { ...state, connection: 'reconnecting', attempt: action.attempt, nextRetryMs: action.delayMs, error: action.error },
        `connection lost at cursor ${state.cursor} (${action.error}); retry ${action.attempt} in ${action.delayMs}ms`,
      );
    case 'gave_up':
      return note({ ...state, connection: 'disconnected', nextRetryMs: null, error: action.error }, `gave up: ${action.error}`);
    case 'stopped':
      return state.connection === 'closed'
        ? state
        : note({ ...state, connection: 'disconnected', nextRetryMs: null }, `disconnected at cursor ${state.cursor}`);
    case 'reset':
      return note({ ...state, cursor: 0, chunks: [], phase: 'unknown', resets: state.resets + 1 }, `server rejected cursor ${state.cursor}; restarting from 0`);
    case 'event':
      return applyEvent(state, action.event);
  }
}

function note(state: StreamState, entry: string): StreamState {
  return { ...state, timeline: [...state.timeline, entry].slice(-MAX_TIMELINE) };
}

function applyEvent(state: StreamState, event: RunEvent): StreamState {
  // Guards, not expected paths: only the next seq is applied, and nothing follows a terminal state.
  if (classifyEvent(state.cursor, event.seq) !== 'apply') return state;
  if (state.phase === 'completed' || state.phase === 'failed' || state.phase === 'interrupted') return state;

  if (!isTerminalType(event.type)) {
    return { ...state, cursor: event.seq, phase: 'running', chunks: [...state.chunks, event.text ?? ''] };
  }
  const error = event.type === 'completed' ? null : (event.error ?? event.type);
  return note(
    { ...state, cursor: event.seq, phase: event.type as RunPhase, connection: 'closed', nextRetryMs: null, error },
    `${event.type} at cursor ${event.seq}${error ? `: ${error}` : ''}`,
  );
}
