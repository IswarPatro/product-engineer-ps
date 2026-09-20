import { useEffect, useState, useSyncExternalStore } from 'react';
import { RunStream } from '../core/runStream';
import type { StreamState } from '../core/reducer';

/** Binds one run's RunStream to a component; the stream is stopped on unmount. */
export function useRunStream(runId: string): { state: StreamState; stream: RunStream } {
  const [stream] = useState(() => new RunStream({ runId }));
  useEffect(() => {
    stream.start();
    return () => stream.stop();
  }, [stream]);
  const state = useSyncExternalStore(stream.subscribe, stream.getState);
  return { state, stream };
}
