#!/usr/bin/env node
/**
 * ActivityWatch SQLite -> Neon Postgres 增量备份
 *
 * 用法:
 *   node sync.mjs            增量同步
 *   node sync.mjs --full     清空远端后全量重传
 *   node sync.mjs --verify   比对本地与远端行数、最大 ID
 *   node sync.mjs --quiet    静默模式，仅写日志
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { DatabaseSync } from "node:sqlite";
import { createClient } from "./lib/neon-http.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const LOG_DIR = path.join(__dirname, "logs");
const LOG_FILE = path.join(LOG_DIR, "sync.log");
const LOCK_FILE = path.join(__dirname, "sync.lock");

const DEFAULT_AW_DB = path.join(
  process.env.LOCALAPPDATA || "",
  "activitywatch",
  "activitywatch",
  "aw-server",
  "peewee-sqlite.v2.db"
);

const BATCH_SIZE = 2000;
const WATERMARK_OVERLAP = 50;
const LOCK_STALE_MS = 2 * 60 * 60 * 1000;

const args = new Set(process.argv.slice(2));
const FULL = args.has("--full");
const VERIFY = args.has("--verify");
const QUIET = args.has("--quiet");

fs.mkdirSync(LOG_DIR, { recursive: true });

function rotateLog() {
  try {
    const st = fs.statSync(LOG_FILE);
    if (st.size > 5 * 1024 * 1024) {
      fs.renameSync(LOG_FILE, path.join(LOG_DIR, `sync-${Date.now()}.log`));
    }
  } catch {
    /* 文件不存在，忽略 */
  }
}

function log(msg, force = false) {
  const line = `[${new Date().toISOString()}] ${msg}`;
  if (!QUIET || force) console.log(line);
  try {
    fs.appendFileSync(LOG_FILE, line + "\n", "utf8");
  } catch {
    /* 日志写不进去不影响主流程 */
  }
}

function loadEnv() {
  const envPath = path.join(__dirname, ".env");
  const text = fs.readFileSync(envPath, "utf8");
  const out = {};
  for (const line of text.split(/\r?\n/)) {
    const m = line.match(/^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$/);
    if (!m) continue;
    let v = m[2];
    if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) {
      v = v.slice(1, -1);
    }
    out[m[1]] = v;
  }
  return out;
}

function toIsoUtc(ts) {
  if (!ts) return ts;
  let s = String(ts).replace(" ", "T");
  if (!/[Zz]|[+-]\d{2}:?\d{2}$/.test(s)) s += "+08:00";
  return s;
}

function parseData(raw) {
  try {
    const v = JSON.parse(raw);
    return v && typeof v === "object" ? v : { _raw: v };
  } catch {
    return { _raw: raw };
  }
}

async function withRetry(fn, label, attempts = 8) {
  let lastErr;
  for (let i = 0; i < attempts; i++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      if (i === attempts - 1) break;
      const wait = Math.min(5000, 500 * 2 ** i);
      log(`${label} 失败，${wait}ms 后重试: ${err.message}`);
      await new Promise((r) => setTimeout(r, wait));
    }
  }
  throw lastErr;
}

const SCHEMA = [
  `CREATE TABLE IF NOT EXISTS aw_bucket (
     bucket_key integer PRIMARY KEY,
     bucket_id text NOT NULL UNIQUE,
     created timestamptz NOT NULL,
     name text,
     type text NOT NULL,
     client text NOT NULL,
     hostname text NOT NULL,
     data jsonb NOT NULL DEFAULT '{}'::jsonb
   )`,
  `CREATE TABLE IF NOT EXISTS aw_event (
     event_id bigint PRIMARY KEY,
     bucket_key integer NOT NULL REFERENCES aw_bucket(bucket_key),
     ts timestamptz NOT NULL,
     duration double precision NOT NULL,
     data jsonb NOT NULL
   )`,
  `CREATE INDEX IF NOT EXISTS aw_event_ts_brin ON aw_event USING brin (ts)`,
  `CREATE TABLE IF NOT EXISTS aw_sync_state (
     id boolean PRIMARY KEY DEFAULT true CHECK (id),
     last_event_id bigint NOT NULL DEFAULT 0,
     updated_at timestamptz,
     source_path text,
     source_size bigint,
     source_mtime timestamptz,
     rows_synced bigint NOT NULL DEFAULT 0,
     full_syncs integer NOT NULL DEFAULT 0
   )`,
  `INSERT INTO aw_sync_state (id) VALUES (true) ON CONFLICT (id) DO NOTHING`,
  `CREATE OR REPLACE VIEW aw_event_v AS
     SELECT e.event_id, b.bucket_id, b.type, b.hostname, e.ts, e.duration,
            e.data->>'app' AS app, e.data->>'title' AS title, e.data->>'url' AS url,
            e.data
     FROM aw_event e JOIN aw_bucket b ON b.bucket_key = e.bucket_key`,
];

function acquireLock() {
  try {
    const st = fs.statSync(LOCK_FILE);
    if (Date.now() - st.mtimeMs > LOCK_STALE_MS) {
      fs.unlinkSync(LOCK_FILE);
    }
  } catch {
    /* 无锁 */
  }
  try {
    const fd = fs.openSync(LOCK_FILE, "wx");
    fs.writeSync(fd, String(process.pid));
    fs.closeSync(fd);
    return true;
  } catch {
    return false;
  }
}

function releaseLock() {
  try {
    fs.unlinkSync(LOCK_FILE);
  } catch {
    /* 忽略 */
  }
}

async function main() {
  rotateLog();
  const started = Date.now();
  const env = loadEnv();
  const dbPath = env.AW_DB_PATH || DEFAULT_AW_DB;

  if (!fs.existsSync(dbPath)) {
    throw new Error(`找不到 ActivityWatch 数据库: ${dbPath}`);
  }

  if (!acquireLock()) {
    log("已有同步进程在运行，本次跳过");
    return;
  }

  const sql = createClient(env.DATABASE_URL);
  const stat = fs.statSync(dbPath);
  const db = new DatabaseSync(dbPath, { readOnly: true });

  try {
    if (VERIFY) {
      await verify(sql, db);
      return;
    }

    log(`源库 ${dbPath}，${(stat.size / 1048576).toFixed(1)} MiB`);

    const hasSchema = (
      await withRetry(
        () =>
          sql.query(
            "SELECT to_regclass('public.aw_sync_state') IS NOT NULL AS ok",
            []
          ),
        "检查 schema"
      )
    )[0].ok;

    if (!hasSchema) {
      log("首次运行，创建表结构");
      for (const stmt of SCHEMA) {
        await withRetry(() => sql.query(stmt, []), "建表");
      }
    }

    const bucketRows = db.prepare("SELECT key,id,created,name,type,client,hostname,datastr FROM bucketmodel").all();
    const bucketsPayload = bucketRows.map((b) => ({
      bucket_key: Number(b.key),
      bucket_id: b.id,
      created: toIsoUtc(b.created),
      name: b.name,
      type: b.type,
      client: b.client,
      hostname: b.hostname,
      data: parseData(b.datastr),
    }));

    await withRetry(
      () =>
        sql.query(
          `INSERT INTO aw_bucket (bucket_key, bucket_id, created, name, type, client, hostname, data)
           SELECT bucket_key, bucket_id, created, name, type, client, hostname, data
           FROM jsonb_to_recordset($1::jsonb)
             AS x(bucket_key int, bucket_id text, created timestamptz, name text, type text,
                  client text, hostname text, data jsonb)
           ON CONFLICT (bucket_key) DO UPDATE SET
             bucket_id = EXCLUDED.bucket_id,
             created = EXCLUDED.created,
             name = EXCLUDED.name,
             type = EXCLUDED.type,
             client = EXCLUDED.client,
             hostname = EXCLUDED.hostname,
             data = EXCLUDED.data`,
          [JSON.stringify(bucketsPayload)]
        ),
      "同步 bucket"
    );
    log(`bucket 已同步 ${bucketsPayload.length} 个`);

    const stateBefore = (await withRetry(
      () => sql.query("SELECT last_event_id FROM aw_sync_state WHERE id = true", []),
      "读取水位"
    ))[0];
    let lastId = Number(stateBefore?.last_event_id ?? 0);

    const maxId = Number(db.prepare("SELECT COALESCE(MAX(id),0) m FROM eventmodel").get().m);
    if (lastId > maxId) {
      log(`远端水位 ${lastId} 大于本地最大 ID ${maxId}，源库被裁剪过，回退到全量`);
      lastId = 0;
    }

    if (FULL) {
      log("全量模式：清空 aw_event 与水位");
      await withRetry(() => sql.query("TRUNCATE aw_event", []), "清空事件");
      lastId = 0;
    }

    let cursor = Math.max(0, lastId - WATERMARK_OVERLAP);
    let synced = 0;
    let syncedTo = cursor;
    const select = db.prepare(
      "SELECT id,bucket_id,timestamp,duration,datastr FROM eventmodel WHERE id > ? ORDER BY id LIMIT ?"
    );

    while (true) {
      const rows = select.all(cursor, BATCH_SIZE);
      if (rows.length === 0) break;

      const payload = rows.map((r) => ({
        event_id: Number(r.id),
        bucket_key: Number(r.bucket_id),
        ts: toIsoUtc(r.timestamp),
        duration: Number(r.duration),
        data: parseData(r.datastr),
      }));

      await withRetry(
        () =>
          sql.query(
            `INSERT INTO aw_event (event_id, bucket_key, ts, duration, data)
             SELECT event_id, bucket_key, ts, duration, data
             FROM jsonb_to_recordset($1::jsonb)
               AS x(event_id bigint, bucket_key int, ts timestamptz, duration float8, data jsonb)
             ON CONFLICT (event_id) DO UPDATE SET
               bucket_key = EXCLUDED.bucket_key,
               ts = EXCLUDED.ts,
               duration = EXCLUDED.duration,
               data = EXCLUDED.data`,
            [JSON.stringify(payload)]
          ),
        `写入事件 ${payload[0].event_id}-${payload[payload.length - 1].event_id}`
      );

      cursor = Number(rows[rows.length - 1].id);
      syncedTo = cursor;
      synced += rows.length;
      if (synced % (BATCH_SIZE * 10) === 0) {
        log(`已同步 ${synced} 条，当前 ID ${cursor}`);
      }
    }

    const total = Number(
      (
        await withRetry(
          () =>
            sql.query(
              "SELECT GREATEST(reltuples, 0)::bigint c FROM pg_class WHERE oid = 'public.aw_event'::regclass",
              []
            ),
          "读取行数估计"
        )
      )[0].c
    );

    await withRetry(
      () =>
        sql.query(
          `UPDATE aw_sync_state
           SET last_event_id = $1, updated_at = now(), source_path = $2,
               source_size = $3, source_mtime = to_timestamp($4), rows_synced = $5,
               full_syncs = full_syncs + $6
           WHERE id = true`,
          [syncedTo, dbPath, Number(stat.size), stat.mtimeMs / 1000, total, FULL ? 1 : 0]
        ),
      "更新水位"
    );

    const secs = ((Date.now() - started) / 1000).toFixed(1);
    log(
      `完成: 本次写入 ${synced} 条，远端约 ${total} 条，同步水位 ${syncedTo}，本地最大 ID ${maxId}，耗时 ${secs}s`,
      true
    );
  } finally {
    db.close();
    releaseLock();
  }
}

async function verify(sql, db) {
  const localTotal = Number(db.prepare("SELECT COUNT(*) c FROM eventmodel").get().c);
  const localMax = Number(db.prepare("SELECT COALESCE(MAX(id),0) m FROM eventmodel").get().m);

  const remote = (await withRetry(
    () => sql.query("SELECT count(*) c, COALESCE(max(event_id),0) m FROM aw_event", []),
    "远端统计"
  ))[0];
  const remoteTotal = Number(remote.c);
  const remoteMax = Number(remote.m);

  const localBuckets = db
    .prepare(
      `SELECT b.id AS bucket_id, COUNT(*) c
       FROM eventmodel e JOIN bucketmodel b ON b.key = e.bucket_id
       GROUP BY b.id`
    )
    .all();
  const remoteBuckets = await withRetry(
    () =>
      sql.query(
        `SELECT b.bucket_id, count(*) c
         FROM aw_event e JOIN aw_bucket b ON b.bucket_key = e.bucket_key
         GROUP BY b.bucket_id`,
        []
      ),
    "远端分桶统计"
  );
  const remoteMap = new Map(remoteBuckets.map((r) => [r.bucket_id, Number(r.c)]));

  let ok = true;
  for (const r of localBuckets) {
    const local = Number(r.c);
    const rem = remoteMap.get(r.bucket_id) ?? 0;
    const diff = local - rem;
    if (diff !== 0) ok = false;
    log(`  ${r.bucket_id}: 本地 ${local}, 远端 ${rem}, 差 ${diff}`);
  }
  for (const [id, rem] of remoteMap) {
    if (!localBuckets.some((r) => r.bucket_id === id)) {
      ok = false;
      log(`  ${id}: 远端多出 ${rem} 条`);
    }
  }

  log(
    `校验: 本地 ${localTotal} 条 / 最大 ID ${localMax}，远端 ${remoteTotal} 条 / 最大 ID ${remoteMax}，` +
      (ok && localTotal === remoteTotal && localMax === remoteMax ? "一致" : "不一致"),
    true
  );
  if (!(ok && localTotal === remoteTotal && localMax === remoteMax)) process.exitCode = 1;
}

main().catch((err) => {
  log(`同步失败: ${err && err.stack ? err.stack : err}`, true);
  process.exitCode = 1;
});
