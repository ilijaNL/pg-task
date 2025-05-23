import { TaskResultStates, SelectedTask, TaskResult } from './task';
import { DeferredPromise, JsonValue, mapCompletionDataArg, resolveWithinSeconds } from './utils/common';
import { createBaseWorker, Worker } from './utils/worker';

export type TaskWorkerConfig = {
  /**
   * Default `10`
   */
  maxConcurrency: number;
  /**
   * Default `2000`
   */
  poolInternvalInMs: number;
  /**
   * A value between 0 and 1.
   * Default `0.33`
   */
  refillThresholdPct: number;

  onResolve?: (task: SelectedTask, err: unknown, result?: unknown) => void;
};

export const defaultTaskWorkerConfig: TaskWorkerConfig = {
  maxConcurrency: 10,
  poolInternvalInMs: 2000,
  refillThresholdPct: 0.33,
};

export interface TaskWorker extends Worker {
  activeTasks: number;
  isIdle: () => boolean;
}

export type TaskContext = {
  id: string;
  meta_data: JsonValue;
  created_on: Date;
  expire_in: number;
  attempt: number;
  queue: string;
  singleton_key: string | null;
  max_attempts: number;
  fail: (reason?: any) => void;
};

export type TaskHandler<TData = JsonValue> = (data: TData, context: TaskContext) => Promise<unknown>;

export interface WorkerImpl {
  resolveTask: (task: TaskResult) => Promise<void>;
  popTasks: (amount: number) => Promise<SelectedTask[]>;
  handler: (task: SelectedTask, handlers: { reject: (reason?: any) => void }) => Promise<unknown>;
}

export const createTaskWorker = (implementation: WorkerImpl, config: TaskWorkerConfig): TaskWorker => {
  const activeTasks = new Map<string, Promise<unknown>>();
  const { maxConcurrency, poolInternvalInMs, refillThresholdPct } = config;
  // used to determine if we can refetch early
  let probablyHasMoreTasks = false;

  async function resolveTask(task: SelectedTask, err: unknown, result?: unknown) {
    await implementation.resolveTask({
      result: mapCompletionDataArg(err ?? result),
      state: err === null ? TaskResultStates.success : TaskResultStates.failed,
      task_id: task.id,
    });

    activeTasks.delete(task.id);

    // if some treshhold is reached, we can refetch
    const threshHoldPct = refillThresholdPct;
    if (probablyHasMoreTasks && activeTasks.size / maxConcurrency <= threshHoldPct) {
      worker.notify();
    }

    config?.onResolve?.(task, err, result);
  }

  function isFull() {
    return activeTasks.size >= maxConcurrency;
  }

  async function loop(): Promise<void> {
    if (isFull()) {
      return;
    }

    const requestedAmount = maxConcurrency - activeTasks.size;
    const tasks = await implementation.popTasks(requestedAmount).catch((err) => {
      console.log('error popping tasks', 'message' in err ? err.message : err);
      return [] as SelectedTask[];
    });

    // high chance that there are more tasks when requested amount is same as fetched
    probablyHasMoreTasks = tasks.length === requestedAmount;

    if (tasks.length === 0) {
      return;
    }

    tasks.forEach((task) => {
      const deferredPromise = new DeferredPromise();
      implementation
        .handler(task, {
          reject(value) {
            deferredPromise.reject(value);
          },
        })
        .then((v) => deferredPromise.resolve(v))
        .catch((e) => deferredPromise.reject(e));

      const taskPromise = resolveWithinSeconds(deferredPromise.promise, task.expire_in)
        .then((result) => {
          return resolveTask(task, null, result);
        })
        .catch((err) => {
          return resolveTask(task, err);
        });

      activeTasks.set(task.id, taskPromise);
    });
  }

  const worker = createBaseWorker(loop, { loopInterval: poolInternvalInMs });

  return {
    ...worker,
    isIdle() {
      return !worker.working() && !isFull();
    },
    /* istanbul ignore next */
    get activeTasks() {
      return activeTasks.size;
    },
    async stop() {
      await worker.stop();
      await Promise.all(Array.from(activeTasks.values()));
    },
  };
};
