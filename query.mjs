#!/usr/bin/env node
/**
 * 直接在 Neon 上执行 SQL，便于排查。
 * 用法: node query.mjs "SELECT count(*) FROM aw_event"
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
const text = process.argv.slice(2).join(" ");
if (!text) {
  console.error('用法: node query.mjs "SELECT ..."');
  process.exit(1);
}

for (let i = 0; i < 8; i++) {
  try {
    const rows = await sql.query(text, []);
    console.log(JSON.stringify(rows, null, 2));
    process.exit(0);
  } catch (e) {
    if (i === 7) {
      console.error(e.message);
      process.exit(1);
    }
    await new Promise((r) => setTimeout(r, Math.min(5000, 500 * 2 ** i)));
  }
}
