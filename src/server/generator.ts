/** A reply generator yields text chunks; it may throw at any point to signal failure. */
export type Generator = (input: { runId: string; text: string }) => AsyncIterable<string>;

const WORDS = ['The', 'quick', 'brown', 'fox', 'jumps', 'over', 'the', 'lazy', 'dog', 'and', 'keeps', 'on', 'running'];

/** Deterministic chunk i, independent of the user's message. */
export const fakeChunk = (index: number): string => `${WORDS[index % WORDS.length]}-${index + 1} `;

export const fakeChunks = (count: number): string[] => Array.from({ length: count }, (_, i) => fakeChunk(i));

export const fakeReply = (count: number): string => fakeChunks(count).join('');

export interface FakeGeneratorOptions {
  chunkCount?: number;
  delayMs?: number;
  /** Throw after emitting this many chunks. */
  failAfter?: number;
}

/** Demo hook: a message starting with this prefix makes the fake generator fail mid-reply. */
export const FAIL_DIRECTIVE = '/fail';
const DIRECTIVE_FAIL_AFTER = 10;

export function createFakeGenerator({ chunkCount = 40, delayMs = 120, failAfter }: FakeGeneratorOptions = {}): Generator {
  return async function* fakeGenerator({ text }) {
    const failAt = text.startsWith(FAIL_DIRECTIVE) ? DIRECTIVE_FAIL_AFTER : failAfter;
    for (let i = 0; i < chunkCount; i++) {
      if (failAt !== undefined && i >= failAt) throw new Error('fake generator failure');
      if (delayMs > 0) await new Promise((resolve) => setTimeout(resolve, delayMs));
      yield fakeChunk(i);
    }
  };
}
