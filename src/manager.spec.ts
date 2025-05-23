import { PostgreSqlContainer, StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { Pool } from 'pg';
import { cleanupSchema, createRandomSchema } from '../__utils__/db';
import { createManager, WorkerManager } from './manager';
import EventEmitter, { once } from 'node:events';
import { executeQuery } from './utils/sql';
import { createPlans } from './plans';
import { setTimeout } from 'timers/promises';
import { DeferredPromise } from './utils/common';

describe('pg worker', () => {
  jest.setTimeout(30000);
  let pool: Pool;
  let container: StartedPostgreSqlContainer;
  let manager: WorkerManager;

  beforeAll(async () => {
    container = await new PostgreSqlContainer('postgres:16.7-alpine').start();
  });

  afterAll(async () => {
    await container.stop();
  });

  let schema = createRandomSchema();

  beforeEach(async () => {
    pool = new Pool({
      connectionString: container.getConnectionUri(),
    });

    schema = createRandomSchema();
  });

  afterEach(async () => {
    await manager?.stop();
    await cleanupSchema(pool, schema);
    await pool?.end();
  });

  it('smoke test', async () => {
    manager = createManager({
      pgClient: pool,
      schema,
    });
    await manager.start();
    await manager.start();

    let stopPromise = manager.stop();
    manager.stop();
    await stopPromise;
  });

  it('smoke test with task', async () => {
    manager = createManager({
      pgClient: pool,
      schema,
    });

    const queue = 'test-queue';

    const ee = new EventEmitter();
    const promise = once(ee, 'received');

    await manager.register({
      queue: queue,
      options: {
        poolInternvalInMs: 20,
        maxConcurrency: 10,
        refillThresholdPct: 0.33,
      },
      handler(data) {
        ee.emit('received', data);
      },
    });
    const plans = createPlans(schema);

    await executeQuery(
      pool,
      plans.enqueueTasks({
        data: { nosmoke: true },
        expireInSeconds: 1,
        maxAttempts: 1,
        metaData: {},
        queue: queue,
        retryBackoff: false,
        retryDelayInSeconds: 10,
        singletonKey: null,
        startAfterSeconds: 0,
      })
    );

    await expect(promise).resolves.toEqual([{ nosmoke: true }]);
    await setTimeout(1000);
    await manager.stop();

    // we should not have any pending tasks left
    await expect(executeQuery(pool, plans.popTasks(queue, 100))).resolves.toHaveLength(0);
  });

  it('smoke test with connection drop', async () => {
    manager = createManager({
      pgClient: pool,
      schema,
    });

    const queue = 'test-queue';

    const deferredPromise = new DeferredPromise();

    await manager.register({
      queue: queue,
      options: {
        poolInternvalInMs: 20,
        maxConcurrency: 10,
        refillThresholdPct: 0.33,
      },
      handler(data) {
        deferredPromise.resolve(data);
      },
    });

    await setTimeout(200);

    pool
      .query(
        'SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND usename = current_user'
      )
      .catch(() => {});

    await setTimeout(200);

    const plans = createPlans(schema);

    await executeQuery(
      pool,
      plans.enqueueTasks({
        data: { nosmoke: true },
        expireInSeconds: 1,
        maxAttempts: 1,
        metaData: {},
        queue: queue,
        retryBackoff: false,
        retryDelayInSeconds: 10,
        singletonKey: null,
        startAfterSeconds: 0,
      })
    );

    await expect(deferredPromise.promise).resolves.toEqual({ nosmoke: true });
  });
});
