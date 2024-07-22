import { PostgreSqlContainer, StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { Pool } from 'pg';
import { cleanupSchema, createRandomSchema } from '../__utils__/db';
import { createManager } from './manager';
import EventEmitter, { once } from 'node:events';
import { executeQuery } from './utils/sql';
import { createPlans } from './plans';
import { setTimeout } from 'timers/promises';

describe('pg worker', () => {
  jest.setTimeout(30000);
  let pool: Pool;
  let container: StartedPostgreSqlContainer;

  beforeAll(async () => {
    container = await new PostgreSqlContainer().start();
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
    await cleanupSchema(pool, schema);
    await pool?.end();
  });

  it('smoke test', async () => {
    const manager = createManager({
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
    const manager = createManager({
      pgClient: pool,
      schema,
    });

    const queue = 'test-queue';

    const ee = new EventEmitter();
    const promise = once(ee, 'received');

    const workerId = await manager.register({
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
      plans.enqueueTasks([
        {
          data: { nosmoke: true },
          expireInSeconds: 1,
          maxAttempts: 1,
          metaData: {},
          queue: queue,
          retryBackoff: false,
          retryDelayInSeconds: 10,
          singletonKey: null,
          startAfterSeconds: 0,
        },
      ])
    );

    await expect(promise).resolves.toEqual([{ nosmoke: true }]);
    await setTimeout(1000);
    await manager.stopWorker(workerId);
    await manager.stop();

    // we should not have any pending tasks left
    await expect(executeQuery(pool, plans.popTasks(queue, 100))).resolves.toHaveLength(0);
  });
});
