import type { StartMessageRequest, StartMessageResponse } from '../../shared/protocol';

export class StartMessageError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

/** Idempotent: re-sending the same userMessageId returns the existing run. */
export async function startMessage(request: StartMessageRequest, baseUrl = '', fetchImpl: typeof fetch = (...a) => fetch(...a)): Promise<StartMessageResponse> {
  const response = await fetchImpl(`${baseUrl}/api/messages`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(request),
  });
  const body = (await response.json().catch(() => ({}))) as { error?: string };
  if (!response.ok) throw new StartMessageError(response.status, body.error ?? `request failed (${response.status})`);
  return body as unknown as StartMessageResponse;
}
