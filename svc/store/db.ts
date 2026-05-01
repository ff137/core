import pg from "pg";
import knex from "knex";
import config from "../../config.ts";
import { convert64to32, getAnonymousAccountId } from "../util/utility.ts";
import util from "node:util";

// remember: all values returned from the server are either NULL or a string
pg.types.setTypeParser(20, (val) => (val === null ? null : parseInt(val, 10)));

/** pg application_name is limited to 63 bytes; keep ASCII-safe for pg_stat_activity. */
export function pgApplicationName(role: string): string {
  const app = config.APP_NAME || "unknown";
  const safeApp = app.replace(/[^\w.-]/g, "_").substring(0, 40);
  const safeRole = role.replace(/[^\w.-]/g, "_").substring(0, 20);
  const name = `odota:${safeApp}:${safeRole}`;
  return name.substring(0, 63);
}

function parsePositiveMsEnv(key: string): string | null {
  const raw = (config as Record<string, string | undefined>)[key];
  if (raw === undefined || raw === "") {
    return null;
  }
  const n = Number(raw);
  if (!Number.isFinite(n) || n < 0) {
    return null;
  }
  return `${Math.floor(n)}ms`;
}

function knexPoolSessionStatements(): string[] {
  const stmts: string[] = [];
  const st = parsePositiveMsEnv("POSTGRES_STATEMENT_TIMEOUT_MS");
  const lt = parsePositiveMsEnv("POSTGRES_LOCK_TIMEOUT_MS");
  const idle = parsePositiveMsEnv(
    "POSTGRES_IDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS",
  );
  if (st) {
    stmts.push(`SET statement_timeout TO '${st}'`);
  }
  if (lt) {
    stmts.push(`SET lock_timeout TO '${lt}'`);
  }
  if (idle) {
    stmts.push(`SET idle_in_transaction_session_timeout TO '${idle}'`);
  }
  return stmts;
}

const knexAppName = pgApplicationName("knex");
const sessionStmts = knexPoolSessionStatements();

console.log(
  "[POSTGRES] knex connecting role=%s max=%s application_name=%s session_timeouts=%s",
  config.APP_NAME || "(unset)",
  config.POSTGRES_MAX_CONNECTIONS,
  knexAppName,
  sessionStmts.length
    ? {
        statement_ms: config.POSTGRES_STATEMENT_TIMEOUT_MS || "(off)",
        lock_ms: config.POSTGRES_LOCK_TIMEOUT_MS || "(off)",
        idle_in_tx_ms:
          config.POSTGRES_IDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS || "(off)",
      }
    : "(none)",
);

export const db = knex({
  client: "pg",
  connection: {
    connectionString: config.POSTGRES_URL,
    application_name: knexAppName,
  },
  pool: {
    min: 0,
    max: Number(config.POSTGRES_MAX_CONNECTIONS),
    afterCreate(conn: pg.PoolClient, done: (err?: Error) => void) {
      if (!sessionStmts.length) {
        return done();
      }
      conn.query(sessionStmts.join("; "), (err) => done(err));
    },
  },
});

const columns: Record<string, any> = {};

export async function upsert(
  db: Knex,
  table: string,
  insert: AnyDict,
  conflict: NumberDict,
) {
  if (!columns[table]) {
    const result = await db(table).columnInfo();
    columns[table] = result;
  }
  const tableColumns = columns[table];
  const row = { ...insert };
  // Remove extra properties
  Object.keys(row).forEach((key) => {
    if (!tableColumns[key]) {
      delete row[key];
    }
  });
  const values = Object.keys(row).map(() => "?");
  const update = Object.keys(row).map((key) =>
    util.format("%s=%s", key, `EXCLUDED.${key}`),
  );
  const query = util.format(
    "INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
    table,
    Object.keys(row).join(","),
    values.join(","),
    Object.keys(conflict).join(","),
    update.join(","),
  );
  return db.raw(
    query,
    Object.keys(row).map((key) => row[key]),
  );
}

export async function upsertPlayer(db: Knex, player: Partial<User>) {
  if (player.steamid && !player.account_id) {
    // convert steamid to accountid
    player.account_id = Number(convert64to32(player.steamid));
  }
  if (!player.account_id || player.account_id === getAnonymousAccountId()) {
    return;
  }
  return upsert(db, "players", player, {
    account_id: player.account_id,
  });
}

export default db;
