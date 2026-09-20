import { DatabaseSync } from 'node:sqlite';
import type { EventType, RunEvent, RunStatus, RunSummary } from '../shared/protocol';

const SCHEMA = `
CREATE TABLE IF NOT EXISTS runs (
  id              TEXT PRIMARY KEY,
  conversation_id TEXT NOT NULL,
  user_message_id TEXT NOT NULL UNIQUE,
  user_text       TEXT NOT NULL,
  status          TEXT NOT NULL CHECK (status IN ('running','completed','failed','interrupted')),
  last_seq        INTEGER NOT NULL DEFAULT 0,
  created_at      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS runs_by_conversation ON runs (conversation_id, status);
CREATE TABLE IF NOT EXISTS events (
  run_id TEXT NOT NULL REFERENCES runs (id),
  seq    INTEGER NOT NULL,
  type   TEXT NOT NULL,
  text   TEXT,
  error  TEXT,
  PRIMARY KEY (run_id, seq)
) WITHOUT ROWID;
`;

const RUN_SELECT = `
SELECT r.id, r.conversation_id, r.user_message_id, r.user_text, r.status, r.last_seq,
       COALESCE((SELECT MIN(seq) FROM events e WHERE e.run_id = r.id), r.last_seq + 1) AS oldest_seq
FROM runs r`;

type Row = Record<string, unknown>;

export interface NewRun {
  id: string;
  conversationId: string;
  userMessageId: string;
  userText: string;
}

export interface EventDraft {
  type: EventType;
  text?: string;
  error?: string;
}

/**
 * Durable run history. This is the single source of truth for ordering:
 * `append` assigns `seq` and refuses to write to a run that already reached a
 * terminal state, so a failed run can never later become completed.
 */
export class RunStore {
  private readonly db: DatabaseSync;

  constructor(path = ':memory:') {
    this.db = new DatabaseSync(path);
    this.db.exec('PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;');
    this.db.exec(SCHEMA);
  }

  createRun(input: NewRun): RunSummary {
    this.db
      .prepare(
        `INSERT INTO runs (id, conversation_id, user_message_id, user_text, status, created_at)
         VALUES (?, ?, ?, ?, 'running', ?)`,
      )
      .run(input.id, input.conversationId, input.userMessageId, input.userText, Date.now());
    return this.getRun(input.id)!;
  }

  getRun(id: string): RunSummary | undefined {
    return this.toSummary(this.db.prepare(`${RUN_SELECT} WHERE r.id = ?`).get(id));
  }

  findRunByUserMessage(userMessageId: string): RunSummary | undefined {
    return this.toSummary(this.db.prepare(`${RUN_SELECT} WHERE r.user_message_id = ?`).get(userMessageId));
  }

  findActiveRun(conversationId: string): RunSummary | undefined {
    return this.toSummary(
      this.db.prepare(`${RUN_SELECT} WHERE r.conversation_id = ? AND r.status = 'running'`).get(conversationId),
    );
  }

  /**
   * Atomically assigns the next seq and persists the event (and the run's new
   * status for terminal events). Returns null if the run is unknown or already
   * terminal, in which case nothing is written.
   */
  append(runId: string, draft: EventDraft): RunEvent | null {
    return this.transaction(() => {
      const run = this.db.prepare('SELECT status, last_seq FROM runs WHERE id = ?').get(runId) as Row | undefined;
      if (!run || run.status !== 'running') return null;

      const seq = (run.last_seq as number) + 1;
      const status: RunStatus = draft.type === 'chunk' ? 'running' : draft.type;
      this.db
        .prepare('INSERT INTO events (run_id, seq, type, text, error) VALUES (?, ?, ?, ?, ?)')
        .run(runId, seq, draft.type, draft.text ?? null, draft.error ?? null);
      this.db.prepare('UPDATE runs SET last_seq = ?, status = ? WHERE id = ?').run(seq, status, runId);

      return { runId, seq, type: draft.type, ...(draft.text !== undefined && { text: draft.text }), ...(draft.error !== undefined && { error: draft.error }) };
    });
  }

  /** Events with seq > afterSeq, in ascending seq order. */
  eventsAfter(runId: string, afterSeq: number, limit = 500): RunEvent[] {
    const rows = this.db
      .prepare('SELECT seq, type, text, error FROM events WHERE run_id = ? AND seq > ? ORDER BY seq ASC LIMIT ?')
      .all(runId, afterSeq, limit) as Row[];
    return rows.map((row) => ({
      runId,
      seq: row.seq as number,
      type: row.type as EventType,
      ...(row.text !== null && { text: row.text as string }),
      ...(row.error !== null && { error: row.error as string }),
    }));
  }

  /**
   * Restart policy: a generator cannot outlive its process, so every run still
   * marked `running` at boot gets an explicit, durable `interrupted` event.
   * Assumes a single service process owns the database.
   */
  interruptRunning(reason = 'service restarted while the reply was being generated'): string[] {
    const rows = this.db.prepare("SELECT id FROM runs WHERE status = 'running'").all() as Row[];
    const interrupted: string[] = [];
    for (const row of rows) {
      const runId = row.id as string;
      if (this.append(runId, { type: 'interrupted', error: reason })) interrupted.push(runId);
    }
    return interrupted;
  }

  /** Retention: keep only the newest `keepLast` events of a run. */
  prune(runId: string, keepLast: number): void {
    this.db
      .prepare('DELETE FROM events WHERE run_id = ? AND seq <= (SELECT last_seq FROM runs WHERE id = ?) - ?')
      .run(runId, runId, keepLast);
  }

  close(): void {
    this.db.close();
  }

  private transaction<T>(fn: () => T): T {
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const result = fn();
      this.db.exec('COMMIT');
      return result;
    } catch (error) {
      this.db.exec('ROLLBACK');
      throw error;
    }
  }

  private toSummary(row: unknown): RunSummary | undefined {
    if (!row) return undefined;
    const r = row as Row;
    return {
      id: r.id as string,
      conversationId: r.conversation_id as string,
      userMessageId: r.user_message_id as string,
      userText: r.user_text as string,
      status: r.status as RunStatus,
      lastSeq: r.last_seq as number,
      oldestSeq: r.oldest_seq as number,
    };
  }
}
