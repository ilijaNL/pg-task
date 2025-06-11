import { TASK_EXECUTION_TABLE, TASK_TABLE } from './plans';
import { TaskResultStates } from './task';

// calculates the next visibility
const visibilityOnFail = (taskTable: string) => `now() + (
  CASE WHEN ${taskTable}.retry_backoff 
    then 
      interval '1s' * (${taskTable}.retry_delay * (2 ^ LEAST(10, GREATEST(0, ${taskTable}.attempt - 1))))
    else (interval '1s' * ${taskTable}.retry_delay)
  end
)`;

export const createMigrations = (schema: string) => [
  `
  CREATE TABLE ${schema}.${TASK_TABLE} (
    -- 8 bytes
    id BIGSERIAL PRIMARY KEY,

    -- 8 bytes each
    -- when the task is ready to be picked up
    visible_at timestamptz not null default now(),
    started_on timestamptz,
    created_on timestamptz not null default now(),
    updated_at timestamptz not null default now(),

    -- 8 bytes
    retry_delay integer not null default(2),

    -- 8 bytes, in seconds
    expire_in integer not null default(120),

    -- 2 bytes each
    attempt smallint not null default 0,
    max_attempts smallint not null default(3),
    _version smallint not null default 1,

    -- 1 byte
    retry_backoff boolean not null default false,

    -- out of band
    queue text not null,
    "data" jsonb,
    meta_data jsonb,
    singleton_key text default null
  ) -- https://www.cybertec-postgresql.com/en/what-is-fillfactor-and-how-does-it-affect-postgresql-performance/
  WITH (fillfactor=90);

  -- fetching tasks
  CREATE INDEX ON ${schema}.${TASK_TABLE} (queue, visible_at) INCLUDE (id, attempt, max_attempts);

  -- singleton task, which is per queue
  CREATE UNIQUE INDEX ON ${schema}.${TASK_TABLE} (queue, singleton_key) where singleton_key is not null;

  CREATE TABLE ${schema}.${TASK_EXECUTION_TABLE} (
    -- 8 bytes
    id BIGSERIAL PRIMARY KEY,

    -- 8 bytes
    task_id bigint not null,

    -- 8 bytes each
    started_on timestamptz,
    task_created_on timestamptz not null default now(),
    recorded_at timestamptz not null default now(),

    -- 2 bytes each
    attempt smallint not null default 0,
    _version smallint not null default 1,
    state smallint not null,

    -- out of band
    queue text not null,
    config jsonb,
    "data" jsonb,
    meta_data jsonb,
    result jsonb
  );

  CREATE INDEX ON ${schema}.${TASK_EXECUTION_TABLE} (task_id);
`,
  `
  -- 
  CREATE OR REPLACE FUNCTION ${schema}.get_tasks(target_q text, amount integer, v_at timestamptz DEFAULT now())
    RETURNS SETOF ${schema}.${TASK_TABLE} AS $$
    BEGIN
      RETURN QUERY
      with visible_tasks as (
        SELECT
          _tasks.id as id
        FROM ${schema}.${TASK_TABLE} _tasks
        WHERE _tasks.queue = target_q 
          AND _tasks.visible_at <= v_at
          AND _tasks.attempt < _tasks.max_attempts
        ORDER BY _tasks.visible_at ASC
        LIMIT amount
        FOR UPDATE SKIP LOCKED
      ) UPDATE ${schema}.${TASK_TABLE} t 
        SET
          -- this is total of expire and retry delay
          visible_at = ${visibilityOnFail('t')} + (interval '1s' * t.expire_in),
          started_on = now(),
          updated_at = now(),
          _version = t._version + 1,
          attempt = t.attempt + 1
        FROM visible_tasks
        WHERE t.id = visible_tasks.id
        RETURNING t.*;
    END
  $$ LANGUAGE 'plpgsql';

  CREATE OR REPLACE FUNCTION ${schema}.peek_tasks(target_q text, amount integer, visible_after timestamptz)
    RETURNS SETOF ${schema}.${TASK_TABLE} AS $$
    BEGIN
      RETURN QUERY
      SELECT * FROM ${schema}.${TASK_TABLE} _tasks
        WHERE _tasks.queue = target_q 
          AND _tasks.visible_at >= visible_after
          AND _tasks.attempt < _tasks.max_attempts
        ORDER BY _tasks.visible_at ASC
        LIMIT amount;
    END
  $$ LANGUAGE 'plpgsql';

  CREATE FUNCTION ${schema}.create_tasks(
      q text[], d jsonb[],  md jsonb[], r_d integer[], m_a smallint[], r_b boolean[], skey text[], saf integer[], eis integer[]
    )
    RETURNS TABLE (task_id BIGINT) AS $$
    BEGIN
      RETURN QUERY
      INSERT INTO ${schema}.${TASK_TABLE} (
        queue,
        "data",
        meta_data,
        retry_delay,
        max_attempts,
        retry_backoff,
        singleton_key,
        visible_at,
        expire_in
      )
      SELECT
        unnest(q) queue,
        unnest(d) "data",
        unnest(md) meta_data,
        unnest(r_d) retry_delay,
        unnest(m_a) max_attempts,
        unnest(r_b) retry_backoff,
        unnest(skey) singleton_key,
        (now() + (unnest(saf) * interval '1s'))::timestamptz visible_at,
        unnest(eis)  expire_in
      ON CONFLICT DO NOTHING
      RETURNING ${TASK_TABLE}.id;
    END
  $$ LANGUAGE 'plpgsql' VOLATILE; 
`,
  `
  CREATE FUNCTION ${schema}.resolve_tasks(
    task_ids bigint[], states smallint[], results jsonb[]
  )
    RETURNS TABLE (task_id BIGINT) AS $$
  BEGIN
    RETURN QUERY
    WITH incoming as (
      SELECT 
        unnest(task_ids) id,
        unnest(states) state,
        unnest(results) result
    ), log_execution AS (
      INSERT INTO ${schema}.${TASK_EXECUTION_TABLE} (
        task_id,
        started_on,
        task_created_on,
        attempt,
        _version,
        state,
        queue,
        config,
        "data",
        meta_data,
        result
      ) 
      SELECT 
        tb.id,
        tb.started_on,
        tb.created_on,
        tb.attempt,
        tb._version + 1,
        incoming.state,
        tb.queue,
        json_build_object(
          'max_attempts', tb.max_attempts,
          'expire_in', tb.expire_in,
          'retry_delay', tb.retry_delay,
          'retry_backoff', tb.retry_backoff,
          'singleton_key', tb.singleton_key
        ) config,
        tb.data,
        tb.meta_data,
        incoming.result
      FROM ${schema}.${TASK_TABLE} tb
        INNER JOIN incoming ON incoming.id = tb.id
    ), rescheduled AS ( -- reschedule tasks that fail or are expired
      UPDATE ${schema}.${TASK_TABLE} tt
      SET 
        visible_at = ${visibilityOnFail('tt')},
        updated_at = now(),
        _version = tt._version + 1
      FROM incoming
      WHERE incoming.state = ${TaskResultStates.failed}
        AND tt.id = incoming.id 
        AND tt.attempt < tt.max_attempts
      RETURNING tt.id
    ), deleted AS (
      DELETE FROM ${schema}.${TASK_TABLE} dt
      WHERE id in (
        select id from incoming 
          where incoming.id not in (select id from rescheduled)
      )
      returning dt.id
    ) SELECT deleted.id from deleted;
  END;
  $$ LANGUAGE 'plpgsql' VOLATILE;
`,
  `
-- Cleanup function 
CREATE FUNCTION ${schema}.remove_dangling_tasks(after_seconds integer)
  RETURNS void AS $$
    BEGIN
    PERFORM ${schema}.resolve_tasks(
      ARRAY_AGG(_expired.id), 
      ARRAY_AGG(_expired.s), 
      ARRAY_AGG(_expired.d)
    ) FROM (
      SELECT
        _tasks.id id,
        ${TaskResultStates.failed}::smallint s,
        '{ "message": "task expired because no poll happened." }'::jsonb d
      FROM ${schema}.${TASK_TABLE} _tasks
      WHERE _tasks.visible_at < (now() - (interval '1s' * after_seconds))
      -- lock the rows which are going to be resolved
      FOR UPDATE SKIP LOCKED
    ) _expired;
    END;
  $$ LANGUAGE 'plpgsql' VOLATILE;

-- Cleanup function 
CREATE FUNCTION ${schema}.fail_max_attempts()
  RETURNS void AS $$
    BEGIN
    PERFORM ${schema}.resolve_tasks(
      ARRAY_AGG(failed_tasks.id), 
      ARRAY_AGG(failed_tasks.s), 
      ARRAY_AGG(failed_tasks.d)
    ) FROM (
      SELECT
        _tasks.id id,
        ${TaskResultStates.failed}::smallint s,
        '{ "message": "max attempts reached" }'::jsonb d
      FROM ${schema}.${TASK_TABLE} _tasks
      WHERE _tasks.visible_at < now()
        AND _tasks.attempt >= _tasks.max_attempts
      -- lock the rows which are going to be resolved
      FOR UPDATE SKIP LOCKED
    ) failed_tasks;
    END;
  $$ LANGUAGE 'plpgsql' VOLATILE;
`,
];
