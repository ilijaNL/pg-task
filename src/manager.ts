import { createMaintainceWorker } from './maintaince';
import { createMigrations } from './migrations';
import { QueueWorkerConfig, createQueueWorker } from './queue-worker';
import type { TaskWorker, TaskWorkerConfig } from './task-worker';
import { SECONDS_IN_DAY } from './utils/common';
import { migrate } from './utils/migrate';
import type { Pool } from './utils/sql';

const brandSymbol = Symbol('brand');
type WorkerId = number & { [brandSymbol]: 'WorkerId' };

export type WorkerManager = {
  start(): Promise<void>;
  work<TData>(props: QueueWorkerConfig<TData>): Promise<WorkerId>;
  notifyWorker(workerId: WorkerId): void;
  stopWorker(workerId: WorkerId): Promise<void>;
  stop(): Promise<void>;
};

export type WorkerManagerProperties = {
  pgClient: Pool;
  schema: string;
  maintainceWorker?: Partial<{
    disabled: boolean;
    clearStalledTasksAfterSeconds: number;
    deleteArchivedAfterSeonds: number;
  }>;
  defaultWorkerConfig?: Partial<TaskWorkerConfig>;
};

export const createManager = (properties: WorkerManagerProperties): WorkerManager => {
  const { pgClient, schema, defaultWorkerConfig } = properties;
  const workers = new Map<WorkerId, TaskWorker>();
  let workerCount = 0;

  let state: 'starting' | 'running' | 'stopping' | 'idle' = 'idle';

  let startPromise: Promise<void> | null = null;

  async function init() {
    state = 'starting';

    const allMigrations = createMigrations(properties.schema);

    await migrate(pgClient, schema, allMigrations);

    if (properties.maintainceWorker?.disabled !== true) {
      startWorker(
        await createMaintainceWorker(pgClient, schema, {
          clearStalledAfterSeconds: properties.maintainceWorker?.clearStalledTasksAfterSeconds ?? SECONDS_IN_DAY * 7,
          clearExecutionLogsAfterSeconds: properties.maintainceWorker?.deleteArchivedAfterSeonds ?? SECONDS_IN_DAY * 30,
        })
      );
    }

    state = 'running';
  }

  async function ensureStarted() {
    if (!startPromise) {
      startPromise = init();
    }
    return startPromise;
  }

  function startWorker<TData>(queueWorkerConfig: QueueWorkerConfig<TData>): WorkerId {
    const workerOptions = Object.assign({}, defaultWorkerConfig, queueWorkerConfig.options);

    const worker = createQueueWorker(
      {
        client: pgClient,
        handler: queueWorkerConfig.handler,
        queue: queueWorkerConfig.queue,
        schema: schema,
      },
      workerOptions
    );

    const workerId = ++workerCount as WorkerId;
    // start in background
    worker.start();
    workers.set(workerId, worker);

    return workerId;
  }

  async function work<TData>(config: QueueWorkerConfig<TData>): Promise<WorkerId> {
    await ensureStarted();
    return startWorker(config);
  }

  return {
    start() {
      return ensureStarted();
    },
    work,
    notifyWorker(workerId) {
      workers.get(workerId)?.notify();
    },
    async stopWorker(workerId) {
      const worker = workers.get(workerId);
      if (!worker) {
        return;
      }

      await worker.stop();
      workers.delete(workerId);
    },
    async stop() {
      if (state === 'stopping') {
        return;
      }

      state = 'stopping';

      // stop all workers
      await Promise.all(Array.from(workers.values()).map((worker) => worker.stop()));

      state = 'idle';
      startPromise = null;
    },
  };
};
