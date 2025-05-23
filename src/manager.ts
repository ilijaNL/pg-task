import { createMaintainceWorker } from './maintaince';
import { createMigrations } from './migrations';
import { QueueWorkerConfig, createQueueWorker } from './queue-worker';
import type { TaskWorkerConfig } from './task-worker';
import { SECONDS_IN_DAY } from './utils/common';
import { migrate } from './utils/migrate';
import type { Pool } from './utils/sql';
import type { Worker } from './utils/worker';

const brandSymbol = Symbol('brand');
type WorkerId = number & { [brandSymbol]: 'WorkerId' };

export type RegisteredWorker = {
  workerId: WorkerId;
  start: () => void;
  stop: () => Promise<void>;
};

export interface WorkerManager {
  start(): Promise<void>;
  register<TData>(props: QueueWorkerConfig<TData>): Promise<RegisteredWorker>;
  notifyWorker(workerId: WorkerId): void;
  stopWorker(workerId: WorkerId): Promise<void>;
  stop(): Promise<void>;
}

export type WorkerManagerProperties = {
  pgClient: Pool;
  schema: string;
  maintainceWorker?: Partial<{
    disabled: boolean;
    clearStalledTasksAfterSeconds: number;
    deleteArchivedAfterSeonds: number;
  }>;
  migrationTable?: string;
  defaultWorkerConfig?: Partial<TaskWorkerConfig>;
};

export const createManager = (properties: WorkerManagerProperties): WorkerManager => {
  const { pgClient, schema, defaultWorkerConfig, migrationTable = '_pg_task_migrations' } = properties;
  const workers = new Map<WorkerId, Worker>();
  let workerCount = 0;

  let state: 'starting' | 'running' | 'stopping' | 'idle' = 'idle';

  let startPromise: Promise<void> | null = null;

  const onError = (err: any) => {
    console.log('pgClient error', err?.message ?? err);
  };

  pgClient.on('error', onError);

  async function init() {
    state = 'starting';

    const allMigrations = createMigrations(properties.schema);

    await migrate(pgClient, schema, allMigrations, migrationTable);

    if (properties.maintainceWorker?.disabled !== true) {
      registerAndStartWorker(
        await createMaintainceWorker(pgClient, schema, {
          clearStalledAfterSeconds: properties.maintainceWorker?.clearStalledTasksAfterSeconds ?? SECONDS_IN_DAY * 7,
          clearExecutionLogsAfterSeconds: properties.maintainceWorker?.deleteArchivedAfterSeonds ?? SECONDS_IN_DAY * 30,
          clearMaxAttemptsInterval: 60,
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

  function registerAndStartWorker<TData>(queueWorkerConfig: QueueWorkerConfig<TData>): RegisteredWorker {
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

    return {
      workerId,
      stop: () => worker.stop(),
      start: () => worker.start(),
    };
  }

  return {
    start() {
      return ensureStarted();
    },
    async register<TData>(config: QueueWorkerConfig<TData>): Promise<RegisteredWorker> {
      // we can only start if not stopping
      if (state === 'stopping') {
        throw new Error('cannot register worker when manager is stopping');
      }

      await ensureStarted();
      return registerAndStartWorker(config);
    },
    notifyWorker(workerId) {
      workers.get(workerId)?.notify();
    },
    async stopWorker(workerId) {
      const worker = workers.get(workerId);
      if (!worker) {
        return;
      }

      await worker.stop();
      // we don't need to delete since id is always incremental
      // workers.delete(workerId);
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
      pgClient.off('error', onError);
    },
  };
};
