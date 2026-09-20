import type { StreamState } from '../core/reducer';
import { useRunStream } from './useRunStream';

export interface TurnData {
  userMessageId: string;
  runId: string;
  text: string;
}

/** The single label a user sees, derived from connection + run state. */
export function statusLabel(state: StreamState): { label: string; tone: 'ok' | 'warn' | 'bad' | 'done' } {
  if (state.phase === 'completed') return { label: 'completed', tone: 'done' };
  if (state.phase === 'failed') return { label: 'failed', tone: 'bad' };
  if (state.phase === 'interrupted') return { label: 'interrupted', tone: 'bad' };
  switch (state.connection) {
    case 'connected':
      return { label: 'connected', tone: 'ok' };
    case 'reconnecting':
      return { label: `reconnecting (attempt ${state.attempt})`, tone: 'warn' };
    case 'disconnected':
      return { label: 'disconnected', tone: 'bad' };
    default:
      return { label: 'connecting', tone: 'warn' };
  }
}

export function Turn({ turn }: { turn: TurnData }) {
  const { state, stream } = useRunStream(turn.runId);
  const { label, tone } = statusLabel(state);
  const finished = state.connection === 'closed';

  return (
    <section className="turn">
      <div className="bubble user">{turn.text}</div>
      <div className="bubble assistant">
        {state.chunks.length > 0 ? state.chunks.join('') : <span className="muted">waiting for the first event…</span>}
      </div>

      <div className="meta">
        <span className={`pill ${tone}`}>{label}</span>
        <span className="muted">cursor {state.cursor}</span>
        {state.duplicatesDropped > 0 && <span className="muted">{state.duplicatesDropped} duplicate(s) dropped</span>}
        {state.resets > 0 && <span className="muted">resynced {state.resets}×</span>}
        {state.nextRetryMs !== null && <span className="muted">retrying in {state.nextRetryMs}ms</span>}
        {!finished && state.connection !== 'disconnected' && (
          <button onClick={() => stream.stop()}>Simulate dropped connection</button>
        )}
        {state.connection === 'disconnected' && <button onClick={() => stream.reconnect()}>Reconnect from cursor {state.cursor}</button>}
      </div>
      {state.error && <div className="error">{state.error}</div>}

      <details className="timeline">
        <summary>Connection timeline ({state.timeline.length})</summary>
        <ol>
          {state.timeline.map((entry, index) => (
            <li key={index}>{entry}</li>
          ))}
        </ol>
      </details>
    </section>
  );
}
