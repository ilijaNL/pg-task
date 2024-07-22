import { createPlans, TASK_EXECUTION_TABLE } from './plans';
import { QueueWorkerConfig } from './queue-worker';
import { createTaskQueueFactory, TaskResultStates } from './task';
import { SECONDS_IN_HOUR } from './utils/common';
import { createQueryExecutor, Pool, rawSql, runTransaction, sql } from './utils/sql';

export const maintainceQueue = '__pg_task_maintaince__';

export type MaintainceOptions = {
  clearExecutionLogsAfterSeconds: number;
  clearStalledAfterSeconds: number;
};

export const createMaintainceWorker = async (pool: Pool, schema: string, options: MaintainceOptions) => {
  const plans = createPlans(schema);
  const executor = createQueryExecutor(pool);

  const taskFactory = createTaskQueueFactory(maintainceQueue, {
    maxAttempts: 100,
    expireInSeconds: 300,
  });

  const { clearExecutionLogsAfterSeconds, clearStalledAfterSeconds } = options;

  const expireTaskQuery = plans.expireTasks(clearStalledAfterSeconds);
  const clearExecutionLogsQuery = sql`DELETE FROM ${rawSql(schema)}.${rawSql(TASK_EXECUTION_TABLE)} WHERE recorded_at < (now() - (interval '1s' * ${clearExecutionLogsAfterSeconds}))`;
  const expireTaskKey = 'expire_tasks';
  const executionLogsKey = 'clear_execution_log';

  const queueWorkerConfig: QueueWorkerConfig<any> = {
    queue: maintainceQueue,
    options: {
      maxConcurrency: 1,
      refillThresholdPct: 0,
      poolInternvalInMs: 60000,
      onResolve(_, err, _result) {
        if (err) {
          console.error('failed maintaince task', err);
        }
      },
    },
    async handler(data, { id, singleton_key }) {
      await runTransaction(pool, async (trx) => {
        const transactionExecutor = createQueryExecutor(trx);

        switch (singleton_key) {
          case executionLogsKey: {
            await transactionExecutor(clearExecutionLogsQuery);
            break;
          }
          case expireTaskKey: {
            await transactionExecutor(expireTaskQuery);
            break;
          }
        }

        // complete this task, and reschedule it in future
        await transactionExecutor(plans.resolveTasks([{ task_id: id, result: data, state: TaskResultStates.success }]));
        await transactionExecutor(
          plans.enqueueTasks(
            taskFactory([
              {
                data: null,
                singletonKey: singleton_key,
                startAfterSeconds: SECONDS_IN_HOUR * 12,
              },
            ])
          )
        );
      });
    },
  };

  // ensure we try to create the maintaince tasks always
  await executor(
    plans.enqueueTasks(
      taskFactory([
        {
          data: null,
          singletonKey: executionLogsKey,
          startAfterSeconds: 0,
        },
        {
          data: null,
          singletonKey: expireTaskKey,
          startAfterSeconds: 0,
        },
      ])
    )
  );

  return queueWorkerConfig;
};
