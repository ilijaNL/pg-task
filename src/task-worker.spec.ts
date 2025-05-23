import EventEmitter, { once } from 'events';
import { setTimeout } from 'node:timers/promises';

import { createTaskWorker } from './task-worker';
import { resolveWithinSeconds } from './utils/common';
import { SelectedTask, TaskResultStates } from './task';

const generateTasks = (amount: number, data: SelectedTask['data'] = {}) =>
  new Array(amount)
    .fill({
      attempt: 1,
      created_on: new Date().toISOString(),
      data: data,
      expire_in: 10,
      id: '1',
      max_attempts: 10,
      meta_data: {},
      queue: 'q',
      retry_delay: 100,
    })
    .map((i, idx) => ({ ...i, id: `${idx + 1}` }));

describe('task-worker', () => {
  it('happy path', async () => {
    expect.assertions(3);
    const ee = new EventEmitter();
    const worker = createTaskWorker(
      {
        async popTasks() {
          return generateTasks(1, { test: 'abc' });
        },
        async resolveTask(task) {
          expect(task.task_id).toBe('1');
          expect(task.result).toStrictEqual({ test: 'abc' });
          expect(task.state).toBe(TaskResultStates.success);
          ee.emit('resolved');
        },
        async handler(task) {
          return task.data;
        },
      },
      { maxConcurrency: 20, poolInternvalInMs: 100, refillThresholdPct: 0.33 }
    );

    worker.start();

    await once(ee, 'resolved');

    await worker.stop();
  });

  it('happy path when erroring', async () => {
    expect.assertions(3);
    const ee = new EventEmitter();
    const worker = createTaskWorker(
      {
        async popTasks() {
          return generateTasks(1, { test: 'abc' });
        },
        async resolveTask(task) {
          expect(task.task_id).toBe('1');
          expect((task.result as any)?.message).toBe('throwed');
          expect(task.state).toBe(TaskResultStates.failed);
          ee.emit('resolved');
        },
        async handler() {
          throw new Error('throwed');
        },
      },
      { maxConcurrency: 20, poolInternvalInMs: 100, refillThresholdPct: 0.33 }
    );

    worker.start();

    await once(ee, 'resolved');

    await worker.stop();
  });

  it("doesn't execute handler when no tasks", async () => {
    let hasPopped = false;
    const worker = createTaskWorker(
      {
        async popTasks() {
          hasPopped = true;
          return [];
        },
        async resolveTask() {
          fail('should not be called');
        },
        async handler() {
          fail('should not be called');
        },
      },
      { maxConcurrency: 20, poolInternvalInMs: 100, refillThresholdPct: 0.33 }
    );

    worker.start();

    await setTimeout(200);

    await worker.stop();
    expect(hasPopped).toBe(true);
  });

  it('stops fetching maxConcurrency is reached', async () => {
    const ee = new EventEmitter();

    let handlerCalls = 0;
    let queryCalls = 0;
    const amountOfTasks = 50;

    const worker = createTaskWorker(
      {
        async popTasks(amount) {
          queryCalls += 1;
          return generateTasks(amount);
        },
        async resolveTask() {
          //
        },
        async handler() {
          handlerCalls += 1;
          await setTimeout(80);
          expect(worker.isIdle()).toBe(false);
          if (handlerCalls === amountOfTasks) {
            ee.emit('completed');
          }
        },
      },
      { maxConcurrency: amountOfTasks, poolInternvalInMs: 80, refillThresholdPct: 0.33 }
    );

    worker.start();

    await once(ee, 'completed');
    // wait for last item to be resolved
    await worker.stop();
    expect(worker.isIdle()).toBe(true);
    expect(queryCalls).toBe(1);
    expect(handlerCalls).toBe(amountOfTasks);
  });

  it('refills', async () => {
    let handlerCalls = 0;
    let popCalls = 0;
    const amountOfTasks = 100;
    const ee = new EventEmitter();
    const tasks = generateTasks(amountOfTasks);

    const worker = createTaskWorker(
      {
        async resolveTask() {
          //
        },
        async popTasks(amount) {
          popCalls = popCalls + 1;
          const result = tasks.splice(0, amount);
          if (tasks.length === 0) {
            ee.emit('drained');
          }

          return result;
        },
        async handler() {
          handlerCalls += 1;
          // resolves between 10-30ms
          const delay = 10 + Math.random() * 20;
          await setTimeout(delay);
          return null;
        },
      },
      { maxConcurrency: 10, refillThresholdPct: 0.33, poolInternvalInMs: 100 }
    );

    const promise = once(ee, 'drained').then(() => true);

    worker.start();

    await expect(resolveWithinSeconds(promise, 0.76)).resolves.toBeTruthy();

    // wait for last item to be resolved
    await worker.stop();

    expect(popCalls).toBeGreaterThan(10);
    expect(popCalls).toBeLessThan(20);

    expect(handlerCalls).toBe(amountOfTasks);
  });

  it('expires the task', async () => {
    expect.assertions(1);
    const ee = new EventEmitter();

    const amountOfTasks = 50;

    const worker = createTaskWorker(
      {
        async popTasks() {
          return [
            {
              attempt: 1,
              created_on: new Date(),
              data: {},
              expire_in: 0.1,
              id: '1',
              max_attempts: 10,
              meta_data: {},
              queue: 'q',
              singleton_key: null,
              retry_delay: 100,
            },
          ];
        },
        async resolveTask(task) {
          // should fail
          expect(task.state).toBe(TaskResultStates.failed);
          ee.emit('completed');
        },
        async handler() {
          await setTimeout(200);
        },
      },
      { maxConcurrency: amountOfTasks, poolInternvalInMs: 100, refillThresholdPct: 0.33 }
    );

    worker.start();

    await once(ee, 'completed');
    // wait for last item to be resolved
    await worker.stop();
  });

  it('reject manually by calling reject handler', async () => {
    expect.assertions(2);
    const ee = new EventEmitter();

    const amountOfTasks = 50;

    const worker = createTaskWorker(
      {
        async popTasks() {
          return [
            {
              attempt: 1,
              created_on: new Date(),
              data: {},
              expire_in: 100,
              id: '1',
              max_attempts: 10,
              meta_data: {},
              queue: 'q',
              singleton_key: null,
              retry_delay: 100,
            },
          ];
        },
        async resolveTask(task) {
          // should fail
          expect(task.state).toBe(TaskResultStates.failed);
          expect(task.result).toEqual({ value: 'works' });
          ee.emit('completed');
        },
        async handler(_, { reject }) {
          await setTimeout(10);
          reject('works');

          return {
            notHappening: false,
          };
        },
      },
      { maxConcurrency: amountOfTasks, poolInternvalInMs: 100, refillThresholdPct: 0.33 }
    );

    const promise = once(ee, 'completed');
    worker.start();

    await promise;
    // wait for last item to be resolved
    await worker.stop();
  });
});
