#!/usr/bin/env node
/**
 * 一次性迁移：把 aw_event 上的两个时间 btree 索引换成 BRIN。
 * 事件按时间顺序写入，物理相关性强，BRIN 体积只有几 KB。
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createClient } from "./lib/neon-http.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const env = Object.fromEntries(
  fs
    .readFileSync(path.join(__dirname, ".env"), "utf8")
    .split(/\r?\n/)
    .map((l) => l.match(/^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$/))
    .filter(Boolean)
    .map((m) => [m[1], m[2]])
);

const sql = createClient(env.DATABASE_URL);

const statements = [
  "DROP INDEX IF EXISTS aw_event_ts_idx",
  "DROP INDEX IF EXISTS aw_event_bucket_ts_idx",
  "CREATE INDEX IF NOT EXISTS aw_event_ts_brin ON aw_event USING brin (ts)",
];

for (const stmt of statements) {
  for (let i = 0; i < 6; i++) {
    try {
      await sql.query(stmt, []);
      console.log("OK", stmt);
      break;
    } catch (e) {
      if (i === 5) throw e;
      await new Promise((r) => setTimeout(r, 500 * 2 ** i));
    }
  }
}
