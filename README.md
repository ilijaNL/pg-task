# pg-task

[![codecov](https://codecov.io/github/ilijaNL/pg-task/graph/badge.svg?token=MQNG1IU5GD)](https://codecov.io/github/ilijaNL/pg-task)

A SQS like solution build on top of Postgres and NodeJS.

## Features

- At least once or **exactly once** delivery
- Typesafety
- Ability to schedule tasks in far future
- Postgres client agnostic
- Backpressure-compatible polling workers
- Task execution log
- Unique tasks per queue (one at a time enqueued)
- Exposed query plans for deep integration
  - no outbox tables needed anymore
- Multi-master compatible
- Automatic creation and migration of storage tables
- Automatic maintenance operations to manage table growth

## Requirements

- Node 18 or higher
- PostgreSQL 11 or higher

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
const workerId = await manager.register<MyTask>({
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
