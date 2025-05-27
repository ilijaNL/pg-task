import { Pool } from 'pg';
import { createPlans } from './plans';
import { ConfiguredTask, TaskResultStates, Task, SelectedTask, TaskResult } from './task';
import { PostgreSqlContainer, StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { cleanupSchema, createRandomSchema } from '../__utils__/db';
import { executeQuery, TypedQuery } from './utils/sql';
import { migrate } from './utils/migrate';
import { createMigrations } from './migrations';
import { setTimeout } from 'timers/promises';
import { createBatcher } from 'node-batcher';
import { randomUUID } from 'crypto';

const generateTasks = (
  amount: number,
  data: Task['data'],
  overrides?: Partial<ConfiguredTask>
): Array<ConfiguredTask> =>
  new Array(amount).fill({
    data: data,
    expireInSeconds: overrides?.expireInSeconds ?? 100,
    maxAttempts: overrides?.maxAttempts ?? 3,
    metaData: overrides?.metaData ?? {},
    queue: overrides?.queue ?? 'test-queue',
    retryBackoff: overrides?.retryBackoff ?? false,
    retryDelayInSeconds: overrides?.retryDelayInSeconds ?? 20,
    singletonKey: overrides?.singletonKey ?? null,
    startAfterSeconds: overrides?.startAfterSeconds ?? 0,
  } as ConfiguredTask);

describe('plans', () => {
  describe('generates sql', () => {
    const plans = createPlans('schema_a');

    it('createTasks', () => {
      const q = plans.createTasks(
        ...generateTasks(
          3,
          { works: true, value: '123' },
          {
            startAfterSeconds: 100,
          }
        )
      );
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

    it('popTasks', () => {
      const q = plans.popTasks('queue', 20);
      expect(q.text).toMatchInlineSnapshot(
        `"SELECT id, data, meta_data, created_on, expire_in, attempt, queue, max_attempts, singleton_key FROM schema_a.get_tasks($1, $2::integer)"`
      );
      expect(q.values).toEqual(['queue', 20]);
    });

    it('peekTasks', () => {
      const q = plans.peekTasks('queue', 20, new Date('2023-01-01T00:00:00Z'));
      expect(q.text).toMatchInlineSnapshot(`
"
      SELECT id, data, meta_data, created_on, expire_in, attempt, queue, max_attempts, singleton_key, visible_at, started_on, updated_at FROM schema_a.task t
        WHERE t.queue = $1 
          AND t.visible_at >= $2
          AND t.attempt < t.max_attempts
        ORDER BY t.visible_at ASC
        LIMIT $3;
  "
`);
      expect(q.values).toEqual(['queue', '2023-01-01T00:00:00.000Z', 20]);
    });

    it('resolveTasks', () => {
      const q = plans.resolveTasks(
        { result: { nested: 'string', item: { a: true } }, state: TaskResultStates.failed, task_id: '12313' },
        { result: { nested: 'string', item: { a: true } }, state: TaskResultStates.failed, task_id: '2222' }
      );

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
      container = await new PostgreSqlContainer('postgres:16.7-alpine').start();
    });

    afterAll(async () => {
      await container.stop();
    });

    beforeEach(async () => {
      pool = new Pool({
        connectionString: container.getConnectionUri(),
        // ensures we have concurrent connections
        max: 10,
      });
      schema = createRandomSchema();
      plans = createPlans(schema);
      await migrate(pool, schema, createMigrations(schema), 'migrations');
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

      await executeQuery(pool, plans.createTasks(...tasks));

      const fetchedTasks = await executeQuery(pool, plans.popTasks('test-queue', 100));

      expect(fetchedTasks).toHaveLength(tasks.length);
      expect(fetchedTasks.map((t) => t.data)).toEqual(tasks.map((t) => t.data));

      await executeQuery(
        pool,
        plans.resolveTasks(
          ...fetchedTasks.map((t) => ({
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

      await executeQuery(pool, plans.createTasks(task));

      const fetchedTasks1 = await executeQuery(pool, plans.popTasks(queue, 11));

      // fails
      await executeQuery(
        pool,
        plans.resolveTasks(
          ...fetchedTasks1.map((t) => ({
            task_id: t.id,
            result: { resolved: false },
            state: TaskResultStates.failed,
          }))
        )
      );

      const fetchedTasks2 = await executeQuery(pool, plans.popTasks(queue, 11));

      expect(fetchedTasks2.length).toBe(0);

      await setTimeout(1500);

      const [fetchedTask3] = await executeQuery(pool, plans.popTasks(queue, 11));

      expect(fetchedTask3).toBeDefined();
      const succeedTask = fetchedTask3!;

      // success
      await executeQuery(
        pool,
        plans.resolveTasks({
          task_id: succeedTask.id,
          result: { resolved: true },
          state: TaskResultStates.success,
        })
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
          state: TaskResultStates.failed,
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

    it('pops tasks once', async () => {
      const queue = 'test-queue';
      const now = Date.now();
      const task = {
        data: { now },
        expireInSeconds: 100,
        maxAttempts: 3,
        metaData: {},
        queue,
        retryBackoff: false,
        retryDelayInSeconds: 1,
        singletonKey: null,
        startAfterSeconds: 0,
      };

      await executeQuery(pool, plans.createTasks(task));

      const result = await Promise.all([
        executeQuery(pool, plans.popTasks(queue, 20)),
        executeQuery(pool, plans.popTasks(queue, 20)),
        executeQuery(pool, plans.popTasks(queue, 20)),
        executeQuery(pool, plans.popTasks(queue, 20)),
        executeQuery(pool, plans.popTasks(queue, 20)),
      ]);

      const results = result.flat();

      expect(results.length).toBe(1);
    });

    it('does not pop tasks that have max attempt reached', async () => {
      const queue = 'test-queue';
      const now = Date.now();
      const task = {
        data: { now },
        expireInSeconds: 1,
        maxAttempts: 1,
        metaData: {},
        queue,
        retryBackoff: false,
        retryDelayInSeconds: 0,
        singletonKey: null,
        startAfterSeconds: 0,
      };

      await executeQuery(pool, plans.createTasks(task));

      {
        const tasks = await executeQuery(pool, plans.popTasks(queue, 20));
        expect(tasks.length).toBe(1);
      }

      // wait for task to expire
      await setTimeout(1100);

      {
        const tasks = await executeQuery(pool, plans.popTasks(queue, 20));
        expect(tasks.length).toBe(0);
      }
    });

    it('does pop tasks that have max attempt **not** reached', async () => {
      const queue = 'test-queue';
      const now = Date.now();
      const task = {
        data: { now },
        expireInSeconds: 1,
        maxAttempts: 2,
        metaData: {},
        queue,
        retryBackoff: false,
        retryDelayInSeconds: 0,
        singletonKey: null,
        startAfterSeconds: 0,
      };

      await executeQuery(pool, plans.createTasks(task));

      {
        const tasks = await executeQuery(pool, plans.popTasks(queue, 20));
        expect(tasks.length).toBe(1);
      }

      await setTimeout(1100);

      {
        const tasks = await executeQuery(pool, plans.popTasks(queue, 20));
        expect(tasks).toEqual([
          {
            attempt: 2,
            created_on: expect.any(Date),
            data: {
              now,
            },
            expire_in: 1,
            id: '1',
            max_attempts: 2,
            meta_data: {},
            queue,
            singleton_key: null,
          },
        ]);
      }
    });

    it('cleanups tasks when only max attempts reached', async () => {
      const queue = randomUUID();
      const now = Date.now();
      const task = {
        data: { now },
        expireInSeconds: 1,
        maxAttempts: 2,
        metaData: {},
        queue,
        retryBackoff: false,
        retryDelayInSeconds: 0,
        singletonKey: null,
        startAfterSeconds: 0,
      };

      const [createdTask] = await executeQuery(pool, plans.createTasks(task));

      {
        const tasks = await executeQuery(pool, plans.popTasks(queue, 20));
        expect(tasks).toHaveLength(1);
      }

      // wait for task to expire first time
      await setTimeout(1000);

      // try to clean up
      await executeQuery(pool, plans.failMaxAttemptsTasks());

      // expect executionlog to be empty since it is never resolved
      {
        const taskExecutionLog = await executeQuery(pool, plans.getTaskExecutionLog(createdTask!.task_id));
        expect(taskExecutionLog).toHaveLength(0);
      }

      // task should still exist
      {
        const tasks = await executeQuery(pool, plans.popTasks(queue, 20));
        expect(tasks).toHaveLength(1);
      }

      // wait for task to expire again
      await setTimeout(1000);

      await executeQuery(pool, plans.failMaxAttemptsTasks());

      // we expect to have exection log now since removeMaxAttemptsTasks resolves the task as failed
      {
        const taskExecutionLog = await executeQuery(pool, plans.getTaskExecutionLog(createdTask!.task_id));
        expect(taskExecutionLog).toHaveLength(1);
        expect(taskExecutionLog[0]).toHaveProperty('state', TaskResultStates.failed);
        expect(taskExecutionLog[0]).toHaveProperty('attempt', 2);
      }
    });

    it('fails task and reschedules with exponential backoff', async () => {
      const expireInSeconds = 20;
      const retryDelayInSeconds = 3;
      const queue = 'exp-backoff';
      const task = {
        data: { works: true },
        expireInSeconds: expireInSeconds,
        maxAttempts: 5,
        metaData: {},
        queue,
        retryBackoff: true,
        retryDelayInSeconds: retryDelayInSeconds,
        singletonKey: null,
        startAfterSeconds: 0,
      };

      const createdTasks = await executeQuery(pool, plans.createTasks(task));

      // pop the task which updates the next visibility
      await executeQuery(pool, plans.popTasks(queue, 11));

      {
        const [pendingTask] = await executeQuery(pool, plans.peekTasks(queue, 11, new Date('2000-01-01T00:00:00Z')));
        expect(pendingTask).toBeDefined();
        const duration = pendingTask!.visible_at.getTime() - pendingTask!.started_on.getTime();

        expect(duration).toBe((expireInSeconds + retryDelayInSeconds) * 1000);
      }

      // fail task which should reschedule it with exponential backoff
      await executeQuery(
        pool,
        plans.resolveTasks(
          ...createdTasks.map((t) => ({
            task_id: t.task_id,
            result: { resolved: false },
            state: TaskResultStates.failed,
          }))
        )
      );

      {
        const [pendingTask] = await executeQuery(pool, plans.peekTasks(queue, 11, new Date('2000-01-01T00:00:00Z')));
        expect(pendingTask).toBeDefined();
        const duration = pendingTask!.visible_at.getTime() - pendingTask!.updated_at.getTime();

        expect(duration).toBe(retryDelayInSeconds * 1000);
      }

      // pop & fail the task several times to ensure exponential backoff works
      await executeQuery(pool, plans.popTasks(queue, 11, new Date('2070-01-01T00:00:00Z')));
      await executeQuery(pool, plans.popTasks(queue, 11, new Date('2080-01-01T00:00:00Z')));
      await executeQuery(pool, plans.popTasks(queue, 11, new Date('2090-01-01T00:00:00Z')));
      await executeQuery(
        pool,
        plans.resolveTasks(
          ...createdTasks.map((t) => ({
            task_id: t.task_id,
            result: { resolved: false },
            state: TaskResultStates.failed,
          }))
        )
      );

      {
        const [pendingTask] = await executeQuery(pool, plans.peekTasks(queue, 11, new Date('2000-01-01T00:00:00Z')));
        expect(pendingTask).toBeDefined();
        const duration = pendingTask!.visible_at.getTime() - pendingTask!.updated_at.getTime();

        expect(pendingTask?.attempt).toBe(4);
        expect(duration).toBe(retryDelayInSeconds * Math.pow(2, 3) * 1000);
      }
    });
  });

  describe('performance', () => {
    let pool: Pool;
    let container: StartedPostgreSqlContainer;

    let schema = createRandomSchema();
    let plans: ReturnType<typeof createPlans>;

    beforeAll(async () => {
      container = await new PostgreSqlContainer('postgres:16.7-alpine').start();
    });

    afterAll(async () => {
      await container.stop();
    });

    beforeEach(async () => {
      pool = new Pool({
        connectionString: container.getConnectionUri(),
        max: 10,
      });
      schema = createRandomSchema();
      plans = createPlans(schema);
      await migrate(pool, schema, createMigrations(schema), 'migrations');
    });

    afterEach(async () => {
      await cleanupSchema(pool, schema);
      await pool?.end();
    });

    it('creates 100000 tasks under 10 seconds', async () => {
      jest.setTimeout(30000);

      // pre-generated batches
      const taskBatch = generateTasks(10, {
        nested: { morenested: { a: '123', b: true, deep: { abc: 'long-string', number: 123123123 } } },
      });

      const start = process.hrtime();
      const createTaskQuery = plans.createTasks(...taskBatch);
      // 5 concurrent batches of 10 tasks are created per iteration
      for (let i = 0; i < 2000; ++i) {
        await Promise.all([
          executeQuery(pool, createTaskQuery),
          executeQuery(pool, createTaskQuery),
          executeQuery(pool, createTaskQuery),
          executeQuery(pool, createTaskQuery),
          executeQuery(pool, createTaskQuery),
        ]);
      }

      const [seconds] = process.hrtime(start);

      expect(seconds).toBeLessThan(10);
    });

    it('pops 100000 tasks under 10 seconds', async () => {
      jest.setTimeout(30000);

      // pre-generated batches
      const taskBatch = generateTasks(10, {
        nested: { morenested: { a: '123', b: true, deep: { abc: 'long-string', number: 123123123 } } },
      });

      const createTasksQuery = plans.createTasks(...taskBatch);
      const createPromises = [];
      // create
      for (let i = 0; i < 10000; ++i) {
        createPromises.push(executeQuery(pool, createTasksQuery));
      }

      await Promise.all(createPromises);

      const getTaskQuery = plans.popTasks(taskBatch[0]!.queue, 10);
      const start = process.hrtime();

      const getTasksPromises: Promise<SelectedTask[]>[] = [];
      for (let i = 0; i < 10000; ++i) {
        getTasksPromises.push(executeQuery(pool, getTaskQuery));
      }
      const tasks = (await Promise.all(getTasksPromises)).flat();
      const [seconds] = process.hrtime(start);

      expect(tasks.length).toBe(100000);
      expect(seconds).toBeLessThan(10);
    });

    it('resolves 50000 tasks under 10 seconds', async () => {
      jest.setTimeout(40000);

      // pre-generated batches
      const taskBatch = generateTasks(100, {
        nested: { morenested: { a: '123', b: true, deep: { abc: 'long-string', number: 123123123 } } },
      });

      const createTasksQuery = plans.createTasks(...taskBatch);

      const createPromises = [];
      // create
      for (let i = 0; i < 500; ++i) {
        createPromises.push(executeQuery(pool, createTasksQuery));
      }

      await Promise.all(createPromises);

      const getTaskQuery = plans.popTasks(taskBatch[0]!.queue, 50);

      const getTasksPromises: Promise<SelectedTask[]>[] = [];
      for (let i = 0; i < 1000; ++i) {
        getTasksPromises.push(executeQuery(pool, getTaskQuery));
      }
      const taskIds = (await Promise.all(getTasksPromises)).flat().map((t) => t.id);

      expect(taskIds.length).toBe(50000);

      const resolveBatcher = createBatcher<TaskResult>({
        maxSize: 10,
        maxTimeInMs: 100,
        onFlush: async (batch) => {
          await executeQuery(pool, plans.resolveTasks(...batch.map((item) => item.data)));
        },
      });
      const start = process.hrtime();

      const promises: Array<Promise<any>> = [];
      for (let i = 0; i < taskIds.length; ++i) {
        promises.push(
          resolveBatcher.add({ result: { success: true }, state: TaskResultStates.success, task_id: taskIds[i]! })
        );
      }

      await Promise.all(promises);

      const [seconds] = process.hrtime(start);

      expect(seconds).toBeLessThan(10);

      // sanity check
      const [lastLog] = await executeQuery(pool, plans.getTaskExecutionLog(taskIds[taskIds.length - 1]!));
      expect(lastLog?.task_id).toBe(taskIds[taskIds.length - 1]!);
    });

    it('creates and resolves 100000 tasks with high queue cardinality', async () => {
      jest.setTimeout(30000);
      const tasksPerQueue = 8;
      const queues = 12500;

      const queueTasks: Array<{
        queue: string;
        query: TypedQuery<{
          task_id: string;
        }>;
      }> = [];

      const data = { morenested: { a: '123', b: true, deep: { abc: 'long-string', number: 123123123 } } };

      // this will create 1000 queues with 5 tasks each
      for (let i = 0; i < queues; ++i) {
        const queue = `${i}-test-queue`;
        queueTasks.push({
          query: plans.createTasks(
            ...generateTasks(tasksPerQueue, data, {
              queue: queue,
            })
          ),
          queue,
        });
      }

      {
        const startInserting = process.hrtime();
        await Promise.all(queueTasks.map((item) => executeQuery(pool, item.query)));
        expect(process.hrtime(startInserting)[0]).toBeLessThan(10);
      }

      // pop all tasks from all queues
      const startResolving = process.hrtime();
      const tasks = (
        await Promise.all(queueTasks.map((item) => executeQuery(pool, plans.popTasks(item.queue, 10))))
      ).flat();

      expect(tasks.length).toBe(queues * tasksPerQueue);

      // resolve all tasks
      let resolves = 0;

      const resolveBatcher = createBatcher<TaskResult>({
        // use multiple of task per queue to ensure we have some random queues when resolving
        maxSize: tasksPerQueue * 5,
        maxTimeInMs: 100,
        onFlush: async (batch) => {
          resolves += batch.length;
          await executeQuery(pool, plans.resolveTasks(...batch.map((item) => item.data)));
        },
      });

      await Promise.all(
        tasks.map((task) =>
          resolveBatcher.add(
            //
            { result: { success: true }, state: TaskResultStates.success, task_id: task.id }
          )
        )
      );

      expect(process.hrtime(startResolving)[0]).toBeLessThan(10);

      expect(resolves).toBe(queues * tasksPerQueue);
    });
  });
});
