import { createBatcher } from 'node-batcher';
import { createPlans } from './plans';
import { JsonValue } from './utils/common';
import { Pool, createQueryExecutor } from './utils/sql';
import { TaskResult } from './task';
import { TaskContext, TaskWorkerConfig, createTaskWorker, defaultTaskWorkerConfig } from './task-worker';

export type TaskHandler<TData = JsonValue> = (data: TData, context: TaskContext) => Promise<unknown> | unknown;

export type QueueWorkerConfig<TData> = {
  queue: string;
  handler: TaskHandler<TData>;
  options?: Partial<TaskWorkerConfig>;
};

export const createQueueWorker = <TData = JsonValue>(
  props: {
    schema: string;
    queue: string;
    handler: TaskHandler<TData>;
    client: Pool;
  },
  settings?: Partial<TaskWorkerConfig>
) => {
  const sqlPlans = createPlans(props.schema);

  const queryExecutor = createQueryExecutor(props.client);

  const resolveTaskBatcher = createBatcher<TaskResult>({
    async onFlush(batch) {
      await queryExecutor(sqlPlans.resolveTasks(batch.map((i) => i.data)));
    },
    // dont make to big since payload can be big
    maxSize: 25,
    // keep it low latency
    maxTimeInMs: 20,
  });

  const config = Object.assign({}, settings, defaultTaskWorkerConfig);

  return createTaskWorker(
    {
      async popTasks(amount) {
        return queryExecutor(sqlPlans.popTasks(props.queue, amount));
      },
      async resolveTask(task) {
        await resolveTaskBatcher.add(task);
      },
      async handler(task, handlers) {
        return props.handler(task.data as TData, {
          fail: handlers.reject,
          attempt: task.attempt,
          singleton_key: task.singleton_key,
          created_on: task.created_on,
          expire_in: task.expire_in,
          id: task.id,
          max_attempts: task.max_attempts,
          meta_data: task.meta_data,
          queue: task.queue,
        });
      },
    },
    config
  );
};
