/**
 * Wire contract shared by the service and the client.
 *
 * - A run's history is an append-only log of RunEvents, ordered by `seq`
 *   (1, 2, 3, ... with no gaps). `seq` is assigned by the server's store and is
 *   the only ordering authority.
 * - The client cursor is "the highest seq I have applied". Reconnecting with
 *   `?after=<cursor>` yields exactly the events with seq > cursor.
 * - Terminal states are themselves events in the log (completed / failed /
 *   interrupted), so they are ordered, durable and replayable like any chunk.
 */

export type EventType = 'chunk' | 'completed' | 'failed' | 'interrupted';
export type RunStatus = 'running' | 'completed' | 'failed' | 'interrupted';

export interface RunEvent {
  runId: string;
  seq: number;
  type: EventType;
  /** Present on `chunk` events. */
  text?: string;
  /** Present on `failed` / `interrupted` events. */
  error?: string;
}

export interface RunSummary {
  id: string;
  conversationId: string;
  userMessageId: string;
  userText: string;
  status: RunStatus;
  /** Highest seq persisted for this run (0 when nothing has been emitted yet). */
  lastSeq: number;
  /** Lowest seq still retained; replay is only possible from oldestSeq - 1. */
  oldestSeq: number;
}

export interface StartMessageRequest {
  conversationId: string;
  /** Client-generated and stable: retrying the same POST never starts a second run. */
  userMessageId: string;
  text: string;
}

export interface StartMessageResponse {
  run: RunSummary;
  /** false when userMessageId was already accepted and the existing run is returned. */
  created: boolean;
}

export type CursorInvalidReason =
  | 'invalid' // not a non-negative integer
  | 'ahead' // cursor is beyond anything this server has for the run
  | 'expired'; // events before the cursor were pruned; replay would leave a gap

/** Sent as an SSE `cursor_invalid` frame instead of replaying when a cursor is unsafe. */
export interface CursorInvalidDetail {
  reason: CursorInvalidReason;
  requested: string;
  oldestSeq: number;
  lastSeq: number;
}

export type StreamFrame =
  | { kind: 'event'; event: RunEvent }
  | { kind: 'cursor_invalid'; detail: CursorInvalidDetail };

export const isTerminalType = (type: EventType): boolean => type !== 'chunk';
export const isTerminalStatus = (status: RunStatus): boolean => status !== 'running';
