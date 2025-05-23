import { ConfiguredTask, SelectedTask, TaskResult, TaskResultState } from './task';
import { JsonValue } from './utils/common';
import { sql, rawSql, TypedQuery } from './utils/sql';

export const TASK_TABLE = 'task';
export const TASK_EXECUTION_TABLE = 'task_execution';

type KeysToArr<T extends Record<string, any>> = {
  [K in keyof T]: Array<T[K]>;
};

const itemsToKeys = <T extends Record<string, any>>(items: T[], init: KeysToArr<T>) =>
  items.reduce((agg, curr, idx) => {
    (Object.keys(curr) as Array<keyof T>).forEach((key) => {
      agg[key][idx] = curr[key];
    });
    return agg;
  }, init);

export const createPlans = (schema: string) => ({
  enqueueTasks: (...tasks: ConfiguredTask[]) => {
    const payload = itemsToKeys(tasks, {
      data: new Array(tasks.length),
      expireInSeconds: new Array(tasks.length),
      maxAttempts: new Array(tasks.length),
      metaData: new Array(tasks.length),
      queue: new Array(tasks.length),
      retryBackoff: new Array(tasks.length),
      retryDelayInSeconds: new Array(tasks.length),
      singletonKey: new Array(tasks.length),
      startAfterSeconds: new Array(tasks.length),
    });

    return sql<{
      task_id: string;
    }>`
SELECT
  task_id 
FROM ${rawSql(schema)}.create_tasks(
  ${payload.queue}::text[],
  ${payload.data}::jsonb[],
  ${payload.metaData}::jsonb[],
  ${payload.retryDelayInSeconds}::integer[],
  ${payload.maxAttempts}::smallint[],
  ${payload.retryBackoff}::boolean[],
  ${payload.singletonKey}::text[],
  ${payload.startAfterSeconds}::integer[],
  ${payload.expireInSeconds}::integer[]
)
`;
  },
  popTasks: (queue: string, amount: number): TypedQuery<SelectedTask> => {
    return sql<{
      id: string;
      data: JsonValue;
      meta_data: JsonValue;
      created_on: Date;
      expire_in: number;
      attempt: number;
      queue: string;
      max_attempts: number;
      singleton_key: string | null;
    }>`
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
FROM ${rawSql(schema)}.get_tasks(${queue}, ${amount}::integer)`;
  },
  /**
   * Resolve tasks
   * @param results
   * @returns
   */
  resolveTasks: (...taskResult: Array<TaskResult>) => {
    const payload = itemsToKeys(taskResult, {
      result: new Array(taskResult.length),
      state: new Array(taskResult.length),
      task_id: new Array(taskResult.length),
    });

    return sql<{
      task_id: string;
    }>`
SELECT 
  task_id 
FROM ${rawSql(schema)}.resolve_tasks(
  ${payload.task_id}::bigint[],
  ${payload.state}::smallint[],
  ${payload.result}::jsonb[]
)`;
  },
  /**
   * Removes tasks that are probably will not be picked up because it is already visible for atleast `afterSeconds`.
   */
  removeDanglingTasks: (afterSeconds: number) =>
    sql`SELECT ${rawSql(schema)}.remove_dangling_tasks(${afterSeconds}::integer)`,
  failMaxAttemptsTasks: () => sql`SELECT ${rawSql(schema)}.fail_max_attempts()`,
  getTaskExecutionLog: (taskId: string) => sql<{
    id: string;
    task_id: string;
    started_on: Date;
    task_created_on: Date;
    recorded_at: Date;
    attempt: number;
    version: number;
    state: TaskResultState;
    queue: string;
    config: JsonValue;
    data: JsonValue;
    meta_data: JsonValue;
    result: unknown;
  }>`
  SELECT 
    id,
    task_id,
    started_on,
    task_created_on,
    recorded_at,
    attempt,
    _version as version,
    state,
    queue,
    config,
    data,
    meta_data,
    result
  FROM ${rawSql(`${schema}.${TASK_EXECUTION_TABLE}`)}
  WHERE task_id = ${taskId}
  ORDER BY version ASC
  `,
});
