# pg-task

A SQS like solution build on top of Postgres and NodeJS.

## Usage

```
npm install pg-task
```

```typescript
import { createManager, executeQuery, createPlans, createTaskQueueFactory } from 'pg-task';
import { Pool } from 'pg';

const pool = new Pool({});

const manager = createManager({
  pgClient: pool,
  schema,
});
await manager.start();

// Register a worker for `worker-queue` task queue
const workerId = await manager.work<MyTask>({
  queue: 'worker-queue',
  async handler(data) {
    await Promise.resolve();
  },
});

// enqueue tasks
const plans = createPlans(schema);
const taskFactory = createTaskQueueFactory('worker-queue');
await executeQuery(
  pool,
  plans.enqueueTasks(
    taskFactory([
      {
        data: { somepayload: 'test' },
      },
      {
        data: { somepayload: 'test' },
      },
    ])
  )
);

// On application shutdown
await manager.stop();
```
