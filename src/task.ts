import { JsonValue } from './utils/common';

export const TaskResultStates = Object.freeze({
  success: 0,
  failed: 1,
  // cancelled: 2,
});

export type TaskResultState = (typeof TaskResultStates)[keyof typeof TaskResultStates];

export type SelectedTask = {
  id: string;
  data: JsonValue;
  meta_data: JsonValue;
  created_on: Date;
  expire_in: number;
  attempt: number;
  queue: string;
  max_attempts: number;
  singleton_key: string | null;
};

export type Task = {
  queue?: string;
  /**
   * Data
   */
  data: JsonValue;
  /**
   * Meta data
   */
  metaData?: JsonValue;
  /**
   * Max attempts
   */
  maxAttempts?: number;
  /**
   * Retry delay in seconds
   */
  retryDelayInSeconds?: number;
  /**
   * Retry backoff
   */
  retryBackoff?: boolean;
  /**
   * Singleton key
   */
  singletonKey?: string | null;
  /**
   * Start after seconds
   */
  startAfterSeconds?: number;
  /**
   * Expire in seconds
   */
  expireInSeconds?: number;
};

export type ConfiguredTask = {
  queue: string;
  data: JsonValue;
  maxAttempts: number;
  metaData: JsonValue;
  retryBackoff: boolean;
  retryDelayInSeconds: number;
  startAfterSeconds: number;
  singletonKey: string | null;
  expireInSeconds: number;
};

/**
 * Creates a task factory with following defaults:
 * @maxAttempts: 3
 * @retryBackoff: false
 * @retryDelayInSeconds: 10
 * @startAfterSeconds: 0
 * @singletonKey: null
 * @expireInSeconds: 120
 */
export const createTaskQueueFactory = (
  queue: string,
  taskConfig?: Partial<{
    maxAttempts: number;
    retryBackoff: boolean;
    retryDelayInSeconds: number;
    startAfterSeconds: number;
    expireInSeconds: number;
  }>
) => {
  const defaultMapper = (task: Task): ConfiguredTask => ({
    queue: task.queue ?? queue,
    data: task.data,
    maxAttempts: task.maxAttempts ?? taskConfig?.maxAttempts ?? 3,
    metaData: task.metaData ?? {},
    retryBackoff: task.retryBackoff ?? taskConfig?.retryBackoff ?? false,
    retryDelayInSeconds: task.retryDelayInSeconds ?? taskConfig?.retryDelayInSeconds ?? 10,
    startAfterSeconds: task.startAfterSeconds ?? taskConfig?.startAfterSeconds ?? 0,
    singletonKey: task.singletonKey ?? null,
    expireInSeconds: task.expireInSeconds ?? taskConfig?.expireInSeconds ?? 120,
  });
  return function create(tasks: Task[]): ConfiguredTask[] {
    return tasks.map(defaultMapper);
  };
};

export type TaskResult = {
  task_id: string;
  state: TaskResultState;
  // result
  result: JsonValue;
};
