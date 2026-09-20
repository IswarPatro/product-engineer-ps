import { FormEvent, useState } from 'react';
import { startMessage } from '../core/api';
import { Turn, type TurnData } from './Turn';

const load = <T,>(key: string, fallback: T): T => {
  try {
    const raw = localStorage.getItem(key);
    return raw ? (JSON.parse(raw) as T) : fallback;
  } catch {
    return fallback;
  }
};
const save = (key: string, value: unknown) => {
  try {
    localStorage.setItem(key, JSON.stringify(value));
  } catch {
    /* storage unavailable: the app still works, it just won't survive a reload */
  }
};

export function App() {
  const [conversationId] = useState(() => {
    const existing = load<string | null>('conversationId', null);
    if (existing) return existing;
    const created = crypto.randomUUID();
    save('conversationId', created);
    return created;
  });
  // Turns are remembered so a page reload replays each run from the server's durable log.
  const [turns, setTurns] = useState<TurnData[]>(() => load('turns', []));
  const [text, setText] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [sending, setSending] = useState(false);

  async function onSubmit(event: FormEvent) {
    event.preventDefault();
    const trimmed = text.trim();
    if (!trimmed || sending) return;
    setSending(true);
    setError(null);
    try {
      const userMessageId = crypto.randomUUID();
      const { run } = await startMessage({ conversationId, userMessageId, text: trimmed });
      const next = [...turns, { userMessageId, runId: run.id, text: trimmed }];
      setTurns(next);
      save('turns', next);
      setText('');
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSending(false);
    }
  }

  function newConversation() {
    localStorage.removeItem('turns');
    localStorage.removeItem('conversationId');
    location.reload();
  }

  return (
    <main>
      <header>
        <h1>Resumable conversation</h1>
        <button onClick={newConversation}>New conversation</button>
      </header>
      <p className="muted hint">
        Send a message to stream a reply. Use “Simulate dropped connection”, or kill and restart the service. Start a message with <code>/fail</code> to see a
        generation failure.
      </p>

      {turns.map((turn) => (
        <Turn key={turn.userMessageId} turn={turn} />
      ))}

      <form onSubmit={onSubmit}>
        <input value={text} onChange={(e) => setText(e.target.value)} placeholder="Say something…" autoFocus />
        <button type="submit" disabled={sending || !text.trim()}>
          Send
        </button>
      </form>
      {error && <div className="error">{error}</div>}
    </main>
  );
}
