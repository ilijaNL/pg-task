import { QueryClient } from '../src';

export async function cleanupSchema(pool: QueryClient, schema: string) {
  await pool.query(`DROP SCHEMA ${schema} CASCADE`);
}

export function createRandomSchema() {
  const schema = 's_' + (Math.random() + 1).toString(36).substring(6);
  return schema;
}
