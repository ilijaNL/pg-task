export interface QueryResultRow {
  [column: string]: unknown;
}

const rowTypeSymbol = Symbol('rowTypeSymbol');

export type TypedQuery<TRow = QueryResultRow> = {
  text: string;
  values: unknown[];
  // used to keep the type definition and is always undefined
  [rowTypeSymbol]: TRow;
};

export interface QueryClient {
  query(query: string, values?: any[]): Promise<any>;
  query<T = unknown>(props: {
    text: string;
    values: unknown[];
    name?: string;
  }): Promise<{
    rows: T[];
    rowCount: number;
  }>;
}

export interface Pool extends QueryClient {
  connect(): Promise<ClientFromPool>;
}

export interface ClientFromPool extends QueryClient {
  release(err?: boolean | Error | undefined): void;
}

export async function executeQuery<TRowResult extends QueryResultRow>(
  client: QueryClient,
  query: TypedQuery<TRowResult>
) {
  const r = await client.query<TRowResult>({
    text: query.text,
    values: query.values,
  });

  return r.rows;
}

export const createQueryExecutor =
  (client: QueryClient) =>
  <TRowResult extends QueryResultRow>(query: TypedQuery<TRowResult>) =>
    executeQuery(client, query);

export async function runTransaction<T>(pool: Pool, handler: (client: ClientFromPool) => Promise<T>) {
  let client: ClientFromPool | null = await pool.connect();
  let result: T;
  try {
    await client.query('BEGIN');
    result = await handler(client);
    await client.query('COMMIT');
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
    client = null;
  }

  return result;
}

/**
 * Values supported by SQL engine.
 */
export type Value = unknown;

/**
 * Supported value or SQL instance.
 */
export type RawValue = Value | Sql;

/**
 * A SQL instance can be nested within each other to build SQL strings.
 */
class Sql {
  readonly values: Value[];
  readonly strings: string[];

  constructor(rawStrings: readonly string[], rawValues: readonly RawValue[]) {
    if (rawStrings.length - 1 !== rawValues.length) {
      if (rawStrings.length === 0) {
        throw new TypeError('Expected at least 1 string');
      }

      throw new TypeError(`Expected ${rawStrings.length} strings to have ${rawStrings.length - 1} values`);
    }

    const valuesLength = rawValues.reduce<number>(
      (len, value) => len + (value instanceof Sql ? value.values.length : 1),
      0
    );

    this.values = new Array(valuesLength);
    this.strings = new Array(valuesLength + 1);

    this.strings[0] = rawStrings[0]!;

    // Iterate over raw values, strings, and children. The value is always
    // positioned between two strings, e.g. `index + 1`.
    let i = 0,
      pos = 0;
    while (i < rawValues.length) {
      const child = rawValues[i++];
      const rawString = rawStrings[i];

      // Check for nested `sql` queries.
      if (child instanceof Sql) {
        // Append child prefix text to current string.
        this.strings[pos]! += child.strings[0]!;

        let childIndex = 0;
        while (childIndex < child.values.length) {
          this.values[pos++] = child.values[childIndex++];
          this.strings[pos] = child.strings[childIndex]!;
        }

        // Append raw string to current string.
        this.strings[pos]! += rawString!;
      } else {
        this.values[pos++] = child;
        this.strings[pos] = rawString!;
      }
    }
  }

  get text() {
    const len = this.strings.length;
    let i = 1;
    let value = this.strings[0]!;
    while (i < len) value += `$${i}${this.strings[i++]}`;
    return value;
  }
}

export function rawSql(sql: string) {
  return new Sql([sql], []);
}

export function sql<TRow extends QueryResultRow = QueryResultRow>(
  sqlFragments: ReadonlyArray<string>,
  ...parameters: unknown[]
): TypedQuery<TRow> {
  const _sql = new Sql(sqlFragments, parameters);

  return {
    text: _sql.text,
    values: _sql.values,
  } as TypedQuery<TRow>;
}
