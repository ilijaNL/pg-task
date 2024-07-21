import { setTimeout as setTimeoutPromise } from 'timers/promises';
import { serializeError } from 'serialize-error';

export type JsonArray = JsonValue[];

export type JsonObject = {
  [K in string]?: JsonValue;
};

export type JsonPrimitive = boolean | null | number | string;

export type JsonValue = JsonArray | JsonObject | JsonPrimitive;

export const SECONDS_IN_HOUR = 60 * 60;
export const SECONDS_IN_DAY = SECONDS_IN_HOUR * 24;

export class DeferredPromise<T = unknown> {
  #resolve: ((value: T | PromiseLike<T>) => void) | null = null;
  #reject: ((reason?: any) => void) | null = null;
  public readonly promise: Promise<T>;

  constructor() {
    this.promise = new Promise<T>((resolve, reject) => {
      this.#resolve = resolve;
      this.#reject = reject;
    });
  }

  resolve(value: T) {
    this.#resolve?.(value);
    this.#reject = null;
    this.#resolve = null;
  }

  reject(reason?: any) {
    this.#reject?.(reason);
    this.#reject = null;
    this.#resolve = null;
  }
}

export type ClearablePromise<T> = Promise<T> & {
  abort: () => void;
};

export function delay<T>(ms: number, shouldErrorMessage?: string): ClearablePromise<T> {
  const ac = new AbortController();

  const promise = new Promise<void>((resolve, reject) => {
    setTimeoutPromise(ms, null, { signal: ac.signal })
      .then(() => {
        if (shouldErrorMessage) {
          reject(new Error(shouldErrorMessage));
        } else {
          resolve();
        }
      })
      .catch(resolve);
  }) as ClearablePromise<T>;

  promise.abort = () => {
    if (!ac.signal.aborted) {
      ac.abort();
    }
  };

  return promise;
}

export const resolveWithinSeconds = async <T>(promise: Promise<T>, seconds: number): Promise<T> => {
  const timeout = Math.max(0.001, seconds) * 1000;
  const timeoutReject = delay(timeout, `handler execution exceeded ${timeout}ms`);

  let result;

  try {
    result = await Promise.race([promise, timeoutReject]);
  } finally {
    try {
      timeoutReject.abort();
    } catch {
      //
    }
  }

  return result as T;
};

export function mapCompletionDataArg(data: any) {
  if (data === null || typeof data === 'undefined' || typeof data === 'function') {
    return null;
  }

  const result = typeof data === 'object' && !Array.isArray(data) ? data : { value: data };

  return serializeError(result);
}
