import { randomUUID } from 'node:crypto';
import type { EventDraft, RunStore } from './store';
import type { Notifier } from './notifier';
import type { Generator } from './generator';
import type { RunSummary, StartMessageRequest, StartMessageResponse } from '../shared/protocol';

export class ConflictError extends Error {
  constructor(readonly activeRunId: string) {
    super('this conversation already has a reply in progress');
  }
}

/**
 * Starts runs and drives their generators, persisting every event before
 * announcing it. Owns no ordering itself: the store assigns seq.
 */
export class RunManager {
  private readonly inFlight = new Set<Promise<void>>();

  constructor(
    private readonly store: RunStore,
    private readonly notifier: Notifier,
    private readonly generator: Generator,
    private readonly log: (message: string, error?: unknown) => void = () => {},
  ) {}

  /** Idempotent on userMessageId; one active run per conversation. */
  start({ conversationId, userMessageId, text }: StartMessageRequest): StartMessageResponse {
    const existing = this.store.findRunByUserMessage(userMessageId);
    if (existing) return { run: existing, created: false };

    const active = this.store.findActiveRun(conversationId);
    if (active) throw new ConflictError(active.id);

    const run = this.store.createRun({ id: randomUUID(), conversationId, userMessageId, userText: text });
    const task = this.drive(run).finally(() => this.inFlight.delete(task));
    this.inFlight.add(task);
    return { run, created: true };
  }

  /** Resolves when every generator started so far has finished (used by tests). */
  async idle(): Promise<void> {
    while (this.inFlight.size > 0) await Promise.all([...this.inFlight]);
  }

  private async drive(run: RunSummary): Promise<void> {
    try {
      for await (const text of this.generator({ runId: run.id, text: run.userText })) {
        if (!this.publish(run.id, { type: 'chunk', text })) return; // already terminal: stop generating
      }
      this.publish(run.id, { type: 'completed' });
    } catch (error) {
      this.log(`run ${run.id} generator failed`, error);
      try {
        this.publish(run.id, { type: 'failed', error: error instanceof Error ? error.message : String(error) });
      } catch (persistError) {
        this.log(`run ${run.id} could not record failure`, persistError);
      }
    }
  }

  /** Persist first, then wake subscribers. False when the run is terminal. */
  private publish(runId: string, draft: EventDraft): boolean {
    const event = this.store.append(runId, draft);
    if (!event) return false;
    this.notifier.notify(runId);
    return true;
  }
}
