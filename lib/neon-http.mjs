import dns from "node:dns";
import fs from "node:fs";
import https from "node:https";
import net from "node:net";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { neon, neonConfig } from "@neondatabase/serverless";
import { HttpsProxyAgent } from "https-proxy-agent";

neonConfig.fetchEndpoint = (host) => `https://${host}/sql`;

const DIR = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const DEBUG_FILE = path.join(DIR, ".neon-debug.log");

function dbg(...args) {
  if (!process.env.NEON_DEBUG) return;
  try {
    fs.appendFileSync(DEBUG_FILE, `[${new Date().toISOString()}] ${args.join(" ")}\n`, "utf8");
  } catch {
    /* 忽略 */
  }
}

function canConnect(host, port, ms = 800) {
  return new Promise((resolve) => {
    const socket = net.connect({ host, port, family: 4 });
    const timer = setTimeout(() => {
      socket.destroy();
      resolve(false);
    }, ms);
    socket.once("connect", () => {
      clearTimeout(timer);
      socket.destroy();
      resolve(true);
    });
    socket.once("error", () => {
      clearTimeout(timer);
      socket.destroy();
      resolve(false);
    });
  });
}

// 直连兜底：Neon 的 A 记录里常有连不通的节点，按轮询顺序解析。
let rr = 0;
function directLookup(hostname, options, cb) {
  const done = typeof options === "function" ? options : cb;
  const opts = typeof options === "function" ? {} : options || {};
  dns.resolve4(hostname, (err, addrs) => {
    if (err || !addrs || addrs.length === 0) {
      dns.lookup(hostname, { family: 4 }, (e2, address) => {
        if (e2) return done(e2);
        if (opts.all) return done(null, [{ address, family: 4 }]);
        done(null, address, 4);
      });
      return;
    }
    const ip = addrs[rr++ % addrs.length];
    if (opts.all) return done(null, [{ address: ip, family: 4 }]);
    done(null, ip, 4);
  });
}

let transportPromise = null;

async function getTransport() {
  if (transportPromise) return transportPromise;
  transportPromise = (async () => {
    const candidates = [
      process.env.HTTPS_PROXY,
      process.env.HTTP_PROXY,
      "http://127.0.0.1:50033",
    ].filter(Boolean);

    for (const raw of candidates) {
      let u;
      try {
        u = new URL(raw);
      } catch {
        continue;
      }
      if (u.protocol !== "http:" && u.protocol !== "https:") continue;
      if (await canConnect(u.hostname, Number(u.port) || 80)) {
        dbg("使用代理", raw);
        return { agent: new HttpsProxyAgent(raw, { keepAlive: true }), viaProxy: true };
      }
    }

    dbg("直连模式");
    return { agent: new https.Agent({ keepAlive: true, lookup: directLookup }), viaProxy: false };
  })();
  return transportPromise;
}

function resetTransport() {
  transportPromise = null;
}

export async function ipv4Fetch(url, init = {}) {
  const { agent, viaProxy } = await getTransport();
  const u = new URL(url);

  return new Promise((resolve, reject) => {
    let settled = false;
    let hardTimer;

    const finish = (fn, value) => {
      if (settled) return;
      settled = true;
      clearTimeout(hardTimer);
      fn(value);
    };

    const req = https.request(
      {
        hostname: u.hostname,
        port: u.port || 443,
        path: u.pathname + u.search,
        method: init.method || "GET",
        headers: init.headers || {},
        agent,
      },
      (res) => {
        dbg("resp", u.hostname, res.statusCode);
        const chunks = [];
        res.on("data", (c) => chunks.push(c));
        res.on("end", () => {
          finish(
            resolve,
            new Response(Buffer.concat(chunks), {
              status: res.statusCode || 500,
              headers: res.headers,
            })
          );
        });
        res.on("error", (err) => finish(reject, err));
      }
    );

    hardTimer = setTimeout(() => {
      try {
        req.destroy();
      } catch {
        /* 忽略 */
      }
      finish(reject, new Error("请求超时 20s"));
    }, 20_000);

    req.on("error", (err) => {
      dbg("err", u.hostname, err.code || err.message);
      const transient =
        err.code === "ECONNRESET" ||
        err.code === "ETIMEDOUT" ||
        err.code === "ECONNREFUSED" ||
        (err.message || "").includes("超时");
      if (transient) {
        resetTransport();
      }
      finish(reject, err);
    });

    if (init.body) req.write(init.body);
    req.end();
  });
}

neonConfig.fetchFunction = ipv4Fetch;

export function createClient(connectionString) {
  return neon(connectionString);
}
