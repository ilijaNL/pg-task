import { createPlans, TASK_EXECUTION_TABLE } from './plans';
import { QueueWorkerConfig } from './queue-worker';
import { createTaskQueueFactory, TaskResultStates } from './task';
import { SECONDS_IN_HOUR } from './utils/common';
import { createQueryExecutor, Pool, rawSql, withTransaction, sql, TypedQuery } from './utils/sql';

export const maintainceQueue = '__pg_task_maintaince__';

export type MaintainceOptions = {
  clearExecutionLogsAfterSeconds: number;
  clearStalledAfterSeconds: number;
  // in seconds
  clearMaxAttemptsInterval: number;
};

export const createMaintainceWorker = async (
  pool: Pool,
  schema: string,
  options: MaintainceOptions
): Promise<QueueWorkerConfig<any>> => {
  const plans = createPlans(schema);
  const executor = createQueryExecutor(pool);

  const taskFactory = createTaskQueueFactory(maintainceQueue, {
    maxAttempts: 100,
    expireInSeconds: 300,
  });

  const maintainceTasks: Array<{
    singletonKey: `__internal_${string}`;
    query: TypedQuery;
    intervalInSec: number;
  }> = [
    {
      singletonKey: '__internal_expire_tasks',
      intervalInSec: SECONDS_IN_HOUR * 12,
      query: plans.removeDanglingTasks(options.clearStalledAfterSeconds),
    },
    {
      singletonKey: '__internal_clear_execution_log',
      intervalInSec: SECONDS_IN_HOUR * 12,
      query: sql`
        DELETE FROM ${rawSql(schema)}.${rawSql(TASK_EXECUTION_TABLE)} 
        WHERE recorded_at < (now() - (interval '1s' * ${options.clearExecutionLogsAfterSeconds}))
      `,
    },
    {
      singletonKey: '__internal_clear_max_attemps',
      intervalInSec: options.clearMaxAttemptsInterval,
      query: plans.failMaxAttemptsTasks(),
    },
  ];

  const queueWorkerConfig: QueueWorkerConfig<any> = {
    queue: maintainceQueue,
    options: {
      maxConcurrency: 1,
      refillThresholdPct: 0,
      poolInternvalInMs: 30_000,
      onResolve(_, err, _result) {
        if (err) {
          console.warn('failed maintaince task', err);
        }
      },
    },
    async handler(data, { id, singleton_key }) {
      if (singleton_key === null) {
        return;
      }

      const taskToExecute = maintainceTasks.find((t) => t.singletonKey === singleton_key);

      if (!taskToExecute) {
        return;
      }

      await withTransaction(pool, async (trx) => {
        const qExecutor = createQueryExecutor(trx);
        await qExecutor(taskToExecute.query);

        // complete this task, and reschedule it in future
        await qExecutor(plans.resolveTasks({ task_id: id, result: data, state: TaskResultStates.success }));
        await qExecutor(
          plans.createTasks(
            ...taskFactory([
              {
                data: data,
                singletonKey: singleton_key,
                startAfterSeconds: taskToExecute.intervalInSec,
              },
            ])
          )
        );
      });
    },
  };

  // ensure we try to create the maintaince tasks always
  await executor(
    plans.createTasks(
      ...taskFactory(
        maintainceTasks.map((t) => ({
          data: null,
          singletonKey: t.singletonKey,
          startAfterSeconds: 0,
        }))
      )
    )
  );

  return queueWorkerConfig;
};
