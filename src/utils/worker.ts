import { ClearablePromise, delay } from './common';

export type Worker = {
  notify: () => void;
  start: () => void;
  working: () => boolean;
  stop: () => Promise<void>;
};

type ShouldContinue = boolean | void | undefined;

export function createBaseWorker(run: () => Promise<ShouldContinue>, props: { loopInterval: number }): Worker {
  const { loopInterval } = props;
  let loopPromise: Promise<any>;
  let loopDelayPromise: ClearablePromise<void> | null = null;
  const state = {
    polling: false,
    notified: false,
    executing: false,
  };

  async function loop() {
    while (state.polling) {
      const started = Date.now();
      state.executing = true;
      const shouldContinue = await run().catch((err) => {
        console.log('error in worker loop', err?.message ?? err);
        return false;
      });
      state.executing = false;
      const duration = Date.now() - started;

      if (state.polling) {
        const delayDuration = Math.max(
          shouldContinue === true || state.notified === true ? 5 : loopInterval - duration,
          // do alteast 5 ms for non blocking loop
          5
        );
        loopDelayPromise = delay(delayDuration);
        await loopDelayPromise;
      }

      // clean up
      loopDelayPromise = null;
      state.notified = false;
    }
  }

  function notify() {
    // does not make sense to notify when not polling
    if (!state.polling) {
      return;
    }

    state.notified = true;
    if (loopDelayPromise) {
      loopDelayPromise.abort();
    }
  }

  function start() {
    if (state.polling) {
      return;
    }
    state.polling = true;
    loopPromise = loop();
  }

  async function stop() {
    if (state.polling === false) {
      return;
    }

    state.polling = false;

    // fix for clear bug
    setImmediate(() => loopDelayPromise?.abort());

    await loopPromise;
  }

  return {
    start,
    notify,
    working() {
      return state.executing;
    },
    stop,
  };
}
