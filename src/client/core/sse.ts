export interface SseMessage {
  id?: string;
  event: string;
  data: string;
}

/**
 * Minimal SSE parser over a fetch body. Yields complete messages only, so a
 * frame split across network reads is never half-applied. `onBytes` fires on
 * every read (including heartbeat comments) so callers can detect silence.
 */
export async function* readSse(body: ReadableStream<Uint8Array>, onBytes?: () => void): AsyncGenerator<SseMessage> {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) return;
      onBytes?.();
      buffer += decoder.decode(value, { stream: true }).replace(/\r\n/g, '\n');

      let boundary: number;
      while ((boundary = buffer.indexOf('\n\n')) !== -1) {
        const message = parseMessage(buffer.slice(0, boundary));
        buffer = buffer.slice(boundary + 2);
        if (message) yield message;
      }
    }
  } finally {
    reader.cancel().catch(() => {});
  }
}

function parseMessage(raw: string): SseMessage | null {
  let id: string | undefined;
  let event = 'message';
  const data: string[] = [];
  for (const line of raw.split('\n')) {
    if (line === '' || line.startsWith(':')) continue; // blank or comment (heartbeat)
    const colon = line.indexOf(':');
    const field = colon === -1 ? line : line.slice(0, colon);
    const value = colon === -1 ? '' : line.slice(colon + 1).replace(/^ /, '');
    if (field === 'id') id = value;
    else if (field === 'event') event = value;
    else if (field === 'data') data.push(value);
  }
  return data.length === 0 ? null : { ...(id !== undefined && { id }), event, data: data.join('\n') };
}
