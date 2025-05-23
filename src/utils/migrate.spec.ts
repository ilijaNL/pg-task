import { PostgreSqlContainer, StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { Pool } from 'pg';
import { migrate } from './migrate';

describe('migrate', () => {
  jest.setTimeout(30000);
  let pool: Pool;
  let container: StartedPostgreSqlContainer;

  beforeAll(async () => {
    container = await new PostgreSqlContainer('postgres:16.7-alpine').start();
  });

  afterAll(async () => {
    await container.stop();
  });

  beforeEach(async () => {
    pool = new Pool({
      connectionString: container.getConnectionUri(),
    });
  });

  afterEach(async () => {
    await pool?.end();
  });

  it('happy path', async () => {
    await expect(migrate(pool, 'happy_path', ['SELECT 1', 'SELECT 2'], 'table')).resolves.toBeUndefined();
  });

  it('applies latest migration', async () => {
    const initialMigrations = ['SELECT 1', 'SELECT 2'];
    await expect(migrate(pool, 'latest_migration', initialMigrations, 'table')).resolves.toBeUndefined();
    await expect(
      migrate(pool, 'latest_migration', [...initialMigrations, 'SELECT 3'], 'table')
    ).resolves.toBeUndefined();
  });

  it('applies latest migration only once (concurrency)', async () => {
    const initialMigrations = ['SELECT 1', 'SELECT 2'];
    const schema = 'concurrency';
    await expect(migrate(pool, schema, initialMigrations, 'table')).resolves.toBeUndefined();
    await expect(
      Promise.all([
        new Array(100).fill(() => migrate(pool, schema, [...initialMigrations, 'SELECT 3'], 'table')).map((fn) => fn()),
      ])
    ).resolves.toBeTruthy();
  });

  it('throws when migrations have been modified', async () => {
    const schema = 'throws';
    await expect(migrate(pool, schema, ['SELECT 1', 'SELECT 2'], 'table')).resolves.toBeUndefined();
    await expect(migrate(pool, schema, ['SELECT 2', 'SELECT 2'], 'table')).rejects.toBeTruthy();
  });
});
