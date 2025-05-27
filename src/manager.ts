import { createMaintainceWorker } from './maintaince';
import { createMigrations } from './migrations';
import { QueueWorkerConfig, createQueueWorker } from './queue-worker';
import type { TaskWorkerConfig } from './task-worker';
import { Branded, DeferredPromise, SECONDS_IN_DAY } from './utils/common';
import { migrate } from './utils/migrate';
import type { Pool } from './utils/sql';
import type { Worker } from './utils/worker';

type WorkerId = Branded<'WorkerId', number>;

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

type Normalize<T> = {
  [K in keyof T as T[K] extends undefined ? never : K]: T[K];
};

type State<S extends string, Ctx = undefined> = Normalize<{
  state: S;
  context: Ctx;
}>;

type RunningState = State<
  'running',
  {
    workers: Map<WorkerId, Worker>;
    workerCount: number;
    removeListeners: () => void;
  }
>;

type StoppingState = State<
  'stopping',
  {
    workers: Map<WorkerId, Worker>;
    removeListeners: () => void;
    stoppingPromise: Promise<unknown>;
  }
>;

type IdleState = State<'idle'>;
type StartingState = State<
  'starting',
  {
    startingPromise: Promise<unknown>;
  }
>;

type ManagerState = RunningState | StartingState | StoppingState | IdleState;

function registerAndStartWorker<TData>(
  currentState: RunningState,
  properties: WorkerManagerProperties,
  queueWorkerConfig: QueueWorkerConfig<TData>
): RegisteredWorker {
  const { pgClient, schema, defaultWorkerConfig } = properties;
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

  const workerId = ++currentState.context.workerCount as WorkerId;
  // start in background
  worker.start();
  currentState.context.workers.set(workerId, worker);

  return {
    workerId,
    stop: worker.stop.bind(worker),
    start: worker.start.bind(worker),
  };
}

function startInitialize(_currentState: IdleState, startingPromise: Promise<unknown>): StartingState {
  return {
    state: 'starting',
    context: {
      startingPromise,
    },
  };
}

async function initialize(currentState: StartingState, properties: WorkerManagerProperties): Promise<RunningState> {
  const { pgClient, schema, migrationTable = '_pg_task_migrations' } = properties;
  const allMigrations = createMigrations(properties.schema);

  const onError = (err: any) => {
    console.log('pgClient error', err?.message ?? err);
  };

  pgClient.on('error', onError);

  function stopListeners() {
    pgClient.off('error', onError);
  }

  const runningState: RunningState = {
    state: 'running',
    context: {
      workerCount: 0,
      removeListeners: stopListeners,
      workers: new Map<WorkerId, Worker>(),
    },
  };

  await migrate(pgClient, schema, allMigrations, migrationTable).catch((e) => {
    // in case of a throw, we probably want to stop the listeners
    stopListeners();
    throw e;
  });

  if (properties.maintainceWorker?.disabled !== true) {
    registerAndStartWorker(
      runningState,
      properties,
      await createMaintainceWorker(pgClient, schema, {
        clearStalledAfterSeconds: properties.maintainceWorker?.clearStalledTasksAfterSeconds ?? SECONDS_IN_DAY * 7,
        clearExecutionLogsAfterSeconds: properties.maintainceWorker?.deleteArchivedAfterSeonds ?? SECONDS_IN_DAY * 30,
        clearMaxAttemptsInterval: 60,
      })
    );
  }

  return runningState;
}

function startStopping(currentState: RunningState, promise: Promise<unknown>): StoppingState {
  return {
    context: {
      removeListeners: currentState.context.removeListeners,
      stoppingPromise: promise,
      workers: currentState.context.workers,
    },
    state: 'stopping',
  };
}

async function stopManager(currentState: StoppingState): Promise<IdleState> {
  const workers = currentState.context.workers;
  // stop all workers
  await Promise.all(Array.from(workers.values()).map((worker) => worker.stop()));

  currentState.context.removeListeners();

  return {
    state: 'idle',
  };
}

export const createManager = (properties: WorkerManagerProperties): WorkerManager => {
  let currentState: ManagerState = { state: 'idle' };

  async function start() {
    switch (currentState.state) {
      case 'stopping':
        throw new Error(`You cannot start the manager when in ${currentState.state} state.`);
      case 'running':
        return;
      case 'starting':
        await currentState.context.startingPromise;
        return;
      case 'idle': {
        const deferredPromise = new DeferredPromise<void>();
        currentState = startInitialize(currentState, deferredPromise.promise);
        currentState = await initialize(currentState, properties).catch((e) => {
          deferredPromise.reject(e);
          throw e;
        });
        // we only resolve after we reassigned currenstate to avoid race conditions
        deferredPromise.resolve();
        break;
      }
      default:
        throw new Error(`unknown manager state: ${currentState satisfies never}`);
    }
  }

  return {
    start,
    async register<TData>(config: QueueWorkerConfig<TData>): Promise<RegisteredWorker> {
      // in case of idle, we just start the manager
      if (currentState.state === 'idle') {
        await start();
      }

      // if already starting, just wait before it is started
      if (currentState.state === 'starting') {
        await currentState.context.startingPromise;
      }

      if (currentState.state !== 'running') {
        throw new Error(`cannot register worker not in running state. Current state: ${currentState.state}`);
      }

      return registerAndStartWorker(currentState, properties, config);
    },
    notifyWorker(workerId) {
      if (currentState.state !== 'running') {
        return;
      }

      currentState.context.workers.get(workerId)?.notify();
    },
    async stopWorker(workerId) {
      if (currentState.state !== 'running') {
        return;
      }

      const workers = currentState.context.workers;
      const worker = workers.get(workerId);
      if (!worker) {
        return;
      }

      await worker.stop();
      workers.delete(workerId);
    },
    async stop() {
      // nothing to do
      switch (currentState.state) {
        case 'idle':
          return;
        // in case of starting, we wait before it is fully started and then we stop
        case 'starting': {
          // wait for the starting promise before stopping
          await currentState.context.startingPromise;
          return this.stop();
        }
        case 'running': {
          const deferredPromise = new DeferredPromise<void>();
          currentState = startStopping(currentState, deferredPromise.promise);
          await stopManager(currentState)
            .then((result) => {
              currentState = result;
              // we only resolve after we reassigned current state to avoid race conditions
              deferredPromise.resolve();
            })
            .catch((e) => {
              deferredPromise.reject(e);
              throw e;
            });

          break;
        }
        case 'stopping':
          await currentState.context.stoppingPromise;
          return;

        default:
          throw new Error(`unknown manager state: ${currentState satisfies never}`);
      }
    },
  };
};
