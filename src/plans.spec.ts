import { Pool } from 'pg';
import { createPlans } from './plans';
import { ConfiguredTask, TaskResultStates, Task } from './task';
import { PostgreSqlContainer, StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { cleanupSchema, createRandomSchema } from '../__tests__/db';
import { executeQuery } from './utils/sql';
import { migrate } from './utils/migrate';
import { createMigrations } from './migrations';
import { setTimeout } from 'timers/promises';

const generateTasks = (amount: number, data: Task['data']): Array<ConfiguredTask> =>
  new Array(amount).fill({
    data: data,
    expireInSeconds: 100,
    maxAttempts: 3,
    metaData: {},
    queue: 'test-queue',
    retryBackoff: false,
    retryDelayInSeconds: 20,
    singletonKey: null,
    startAfterSeconds: 100,
  } as ConfiguredTask);

describe('plans', () => {
  describe('generates sql', () => {
    const plans = createPlans('schema_a');

    it('createTasks', () => {
      const q = plans.createTasks(generateTasks(3, { works: true, value: '123' }));
      expect(q.text).toMatchInlineSnapshot(`
  "
  SELECT
    task_id 
  FROM schema_a.create_tasks(
    $1::text[],
    $2::jsonb[],
    $3::jsonb[],
    $4::integer[],
    $5::smallint[],
    $6::boolean[],
    $7::text[],
    $8::integer[],
    $9::integer[]
  )
  "
  `);
      expect(q.values).toMatchInlineSnapshot(`
        [
          [
            "test-queue",
            "test-queue",
            "test-queue",
          ],
          [
            {
              "value": "123",
              "works": true,
            },
            {
              "value": "123",
              "works": true,
            },
            {
              "value": "123",
              "works": true,
            },
          ],
          [
            {},
            {},
            {},
          ],
          [
            20,
            20,
            20,
          ],
          [
            3,
            3,
            3,
          ],
          [
            false,
            false,
            false,
          ],
          [
            null,
            null,
            null,
          ],
          [
            100,
            100,
            100,
          ],
          [
            100,
            100,
            100,
          ],
        ]
      `);
    });

    it('getAndStartTasks', () => {
      const q = plans.getAndStartTasks('queue', 20);
      expect(q.text).toMatchInlineSnapshot(`
        "
        SELECT
          id,
          data,
          meta_data,
          created_on,
          expire_in,
          attempt,
          queue,
          max_attempts,
          singleton_key
        FROM schema_a.get_tasks($1, $2::integer)"
      `);
      expect(q.values).toMatchInlineSnapshot(`
        [
          "queue",
          20,
        ]
      `);
    });

    it('resolveTasks', () => {
      const q = plans.resolveTasks([
        { result: { nested: 'string', item: { a: true } }, state: TaskResultStates.fail, task_id: '12313' },
        { result: { nested: 'string', item: { a: true } }, state: TaskResultStates.fail, task_id: '2222' },
      ]);

      expect(q.text).toMatchInlineSnapshot(`
        "
        SELECT 
          task_id 
        FROM schema_a.resolve_tasks(
          $1::bigint[],
          $2::smallint[],
          $3::jsonb[]
        )"
      `);
      expect(q.values).toMatchInlineSnapshot(`
        [
          [
            "12313",
            "2222",
          ],
          [
            1,
            1,
          ],
          [
            {
              "item": {
                "a": true,
              },
              "nested": "string",
            },
            {
              "item": {
                "a": true,
              },
              "nested": "string",
            },
          ],
        ]
      `);
    });
  });

  describe('execute plans', () => {
    jest.setTimeout(30000);
    let pool: Pool;
    let container: StartedPostgreSqlContainer;

    let schema = createRandomSchema();
    let plans: ReturnType<typeof createPlans>;

    beforeAll(async () => {
      container = await new PostgreSqlContainer().start();
    });

    afterAll(async () => {
      await container.stop();
    });

    beforeEach(async () => {
      pool = new Pool({
        connectionString: container.getConnectionUri(),
      });
      schema = createRandomSchema();
      plans = createPlans(schema);
      await migrate(pool, schema, createMigrations(schema));
    });

    afterEach(async () => {
      await cleanupSchema(pool, schema);
      await pool?.end();
    });

    it('create tasks and resolves tasks', async () => {
      const tasks = [
        {
          data: { works: true },
          expireInSeconds: 100,
          maxAttempts: 3,
          metaData: {},
          queue: 'test-queue',
          retryBackoff: false,
          retryDelayInSeconds: 20,
          singletonKey: null,
          startAfterSeconds: 0,
        },
        {
          data: { works: false },
          expireInSeconds: 100,
          maxAttempts: 3,
          metaData: {},
          queue: 'test-queue',
          retryBackoff: false,
          retryDelayInSeconds: 20,
          singletonKey: null,
          startAfterSeconds: 0,
        },
      ];

      await executeQuery(pool, plans.createTasks(tasks));

      const fetchedTasks = await executeQuery(pool, plans.getAndStartTasks('test-queue', 11));

      expect(fetchedTasks).toHaveLength(tasks.length);
      expect(fetchedTasks.map((t) => t.data)).toEqual(tasks.map((t) => t.data));

      await executeQuery(
        pool,
        plans.resolveTasks(
          fetchedTasks.map((t) => ({
            task_id: t.id,
            result: { resolved: true },
            state: TaskResultStates.success,
          }))
        )
      );

      const [taskExecution] = await executeQuery(pool, plans.getTaskExecutionLog(fetchedTasks[0]!.id));

      expect(taskExecution?.task_id).toBe(fetchedTasks[0]!.id);
      expect(taskExecution?.attempt).toBe(1);
      // creates 1, get&start 2, resolves 3
      expect(taskExecution?.version).toBe(3);
      expect(taskExecution?.result).toEqual({ resolved: true });
    });

    it('create tasks, fails tasks, resolves and logs', async () => {
      const queue = 'test-queue';
      const task = {
        data: { works: true },
        expireInSeconds: 100,
        maxAttempts: 3,
        metaData: {},
        queue,
        retryBackoff: false,
        retryDelayInSeconds: 1,
        singletonKey: null,
        startAfterSeconds: 0,
      };

      await executeQuery(pool, plans.createTasks([task]));

      const fetchedTasks1 = await executeQuery(pool, plans.getAndStartTasks(queue, 11));

      // fails
      await executeQuery(
        pool,
        plans.resolveTasks(
          fetchedTasks1.map((t) => ({
            task_id: t.id,
            result: { resolved: false },
            state: TaskResultStates.fail,
          }))
        )
      );

      const fetchedTasks2 = await executeQuery(pool, plans.getAndStartTasks(queue, 11));

      expect(fetchedTasks2.length).toBe(0);

      await setTimeout(1500);

      const [fetchedTask3] = await executeQuery(pool, plans.getAndStartTasks(queue, 11));

      expect(fetchedTask3).toBeDefined();
      const succeedTask = fetchedTask3!;

      // success
      await executeQuery(
        pool,
        plans.resolveTasks([
          {
            task_id: succeedTask.id,
            result: { resolved: true },
            state: TaskResultStates.success,
          },
        ])
      );

      const taskExecutionLog = await executeQuery(pool, plans.getTaskExecutionLog(succeedTask.id));

      expect(
        taskExecutionLog.map((row) => ({
          task_id: row.task_id,
          version: row.version,
          attempt: row.attempt,
          state: row.state,
          queue: row.queue,
          config: row.config,
          data: row.data,
          meta_data: row.meta_data,
          result: row.result,
        }))
      ).toEqual([
        {
          task_id: succeedTask.id,
          version: 3,
          attempt: 1,
          config: {
            expire_in: task.expireInSeconds,
            max_attempts: task.maxAttempts,
            retry_backoff: task.retryBackoff,
            retry_delay: task.retryDelayInSeconds,
            singleton_key: task.singletonKey,
          },
          data: task.data,
          meta_data: task.metaData,
          queue: queue,
          result: {
            resolved: false,
          },
          state: TaskResultStates.fail,
        },
        {
          task_id: succeedTask.id,
          // creates 1, get&start 2, fails 3,  get&start 4, resolves 5,
          version: 5,
          attempt: 2,
          config: {
            expire_in: task.expireInSeconds,
            max_attempts: task.maxAttempts,
            retry_backoff: task.retryBackoff,
            retry_delay: task.retryDelayInSeconds,
            singleton_key: task.singletonKey,
          },
          data: task.data,
          meta_data: task.metaData,
          queue: queue,
          result: {
            resolved: true,
          },
          state: TaskResultStates.success,
        },
      ]);
    });
  });
});
