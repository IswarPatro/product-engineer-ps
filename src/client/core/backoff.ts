export interface BackoffPolicy {
  /** Reconnect attempts allowed in a row before giving up. */
  maxAttempts: number;
  /** Milliseconds to wait before attempt number `attempt` (1-based). */
  delayMs(attempt: number): number;
}

/** Capped exponential backoff with "equal jitter" (half fixed, half random). */
export function exponentialBackoff({
  baseMs = 250,
  capMs = 5000,
  maxAttempts = 8,
  random = Math.random,
}: { baseMs?: number; capMs?: number; maxAttempts?: number; random?: () => number } = {}): BackoffPolicy {
  return {
    maxAttempts,
    delayMs(attempt) {
      const ceiling = Math.min(capMs, baseMs * 2 ** (attempt - 1));
      return Math.round(ceiling / 2 + (random() * ceiling) / 2);
    },
  };
}

/** Abortable timer; resolves early (without throwing) when the signal aborts. */
export function abortableSleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) return resolve();
    const done = () => {
      clearTimeout(timer);
      signal.removeEventListener('abort', done);
      resolve();
    };
    const timer = setTimeout(done, ms);
    signal.addEventListener('abort', done);
  });
}
