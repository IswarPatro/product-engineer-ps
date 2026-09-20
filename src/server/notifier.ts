/**
 * Transient, in-process "something new was appended for this run" signal.
 *
 * It carries no data on purpose: subscribers wake up and read the durable log,
 * so live delivery can never disagree with replay about content or order.
 */
export class Notifier {
  private readonly subscribers = new Map<string, Set<() => void>>();

  subscribe(runId: string, wake: () => void): () => void {
    let set = this.subscribers.get(runId);
    if (!set) this.subscribers.set(runId, (set = new Set()));
    set.add(wake);
    return () => {
      set.delete(wake);
      if (set.size === 0) this.subscribers.delete(runId);
    };
  }

  notify(runId: string): void {
    for (const wake of [...(this.subscribers.get(runId) ?? [])]) wake();
  }
}
