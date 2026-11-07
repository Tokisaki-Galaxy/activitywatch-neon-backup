# ActivityWatch Neon 备份

把本地 ActivityWatch 的 SQLite 事件库增量同步到 Neon Postgres，每小时一次。

仓库地址：https://github.com/Tokisaki-Galaxy/activitywatch-neon-backup

## 现状

| 项目 | 值 |
|---|---|
| 源库 | `%LOCALAPPDATA%\activitywatch\activitywatch\aw-server\peewee-sqlite.v2.db` |
| Neon 项目 | `activitywatch`，`aws-ap-southeast-1` |
| Neon 数据库 | `activitywatch` |
| 已同步 | 约 78.8 万条事件，库大小约 155 MB |
| 定时任务 | `ActivityWatch Neon Backup`，每小时一次 |
| 日志 | `logs/sync.log`，超过 5 MB 自动轮转 |

## 目录结构

```
sync.mjs            同步主程序，支持 --full / --verify / --quiet
query.mjs           在 Neon 上执行任意 SQL
migrate.mjs         一次性索引迁移，把 btree 换成 BRIN
lib/neon-http.mjs   Neon HTTP 驱动 + 代理与直连兜底
run.vbs             隐藏窗口启动器，任务计划程序调用
install-task.ps1    重新注册或修复定时任务
.env                本地连接串，不提交
.env.example        连接串模板
```

## 数据模型

```
aw_bucket       8 个采集源，主键 bucket_key，保留原始 created/type/client/hostname
aw_event        事件主表，event_id 与 SQLite 主键一一对应
aw_sync_state   单行水位表，记录 last_event_id、源文件大小与时间
aw_event_v      视图，把 data 里的 app/title/url 拆成列
```

`aw_event.data` 保留完整 JSON，没有丢字段。`ts` 是 timestamptz，`duration` 是秒。

常用查询：

```sql
-- 最近 7 天每个应用的使用时长
SELECT e.data->>'app' AS app, round(sum(e.duration) / 3600.0, 2) AS hours
FROM aw_event e JOIN aw_bucket b ON b.bucket_key = e.bucket_key
WHERE b.type = 'currentwindow' AND e.ts > now() - interval '7 days'
GROUP BY 1 ORDER BY 2 DESC;

-- 每天的事件条数
SELECT date_trunc('day', ts) AS day, count(*)
FROM aw_event GROUP BY 1 ORDER BY 1 DESC LIMIT 14;

-- 浏览器访问最多的域名
SELECT split_part(regexp_replace(data->>'url', '^https?://', ''), '/', 1) AS host,
       round(sum(duration) / 3600.0, 2) AS hours
FROM aw_event e JOIN aw_bucket b ON b.bucket_key = e.bucket_key
WHERE b.type = 'web.tab.current'
GROUP BY 1 ORDER BY 2 DESC LIMIT 20;
```

执行方式：

```powershell
node query.mjs "SELECT count(*) FROM aw_event"
```

## 同步机制

1. 只读打开 SQLite，不影响正在运行的 aw-server。
2. 同步 8 个 bucket 的元数据。
3. 从水位 `last_event_id - 50` 开始按主键分批读取，每批 2000 条写入 Postgres，冲突时覆盖。
4. 成功后把水位推进到本批最后一条的 ID，失败不推进，重跑幂等。
5. 如果远端水位大于本地最大 ID，说明源库被裁剪过，自动退回全量。

增量一次通常 5 到 10 秒。

## 本地库兼容性

ActivityWatch 0.14.1 升级时会把本地 SQLite 的索引合并成 `(bucket_id, timestamp)`，并把 journal 模式切到 WAL。同步脚本不依赖索引和 journal 模式，只读取字段和自增 id，所以不需要修改。

脚本启动时会校验 `bucketmodel` 和 `eventmodel` 的必需字段，缺少字段会直接报错退出，避免版本升级后静默同步错误数据。本地库始终以只读方式打开，不会写入。

## 存储优化

事件按时间顺序写入，物理相关性强，时间索引用 BRIN 而不是 btree。`aw_event` 只保留主键 btree 和 `ts` BRIN，索引从 70 MB 降到 21 MB，整库从 205 MB 降到 155 MB。

用旧版本建过库的话执行一次：

```powershell
node migrate.mjs
```

## 常用命令

```powershell
node sync.mjs              # 增量同步
node sync.mjs --verify     # 校验本地与远端行数、最大 ID、分桶数量
node sync.mjs --full       # 清空远端事件表后全量重传
$env:NEON_DEBUG="1"; node sync.mjs   # 输出网络调试日志到 .neon-debug.log
```

定时任务：

```powershell
Get-ScheduledTask -TaskName "ActivityWatch Neon Backup"
Start-ScheduledTask -TaskName "ActivityWatch Neon Backup"
Get-Content Z:\Code\GitHub\activitywatch-neon-backup\logs\sync.log -Tail 20
powershell -ExecutionPolicy Bypass -File install-task.ps1   # 重新注册
Unregister-ScheduledTask -TaskName "ActivityWatch Neon Backup" -Confirm:$false
```

## 网络说明

本地直连 Neon 不稳定，脚本会自动探测 `127.0.0.1:50033` 代理并走 CONNECT 隧道；代理不可用时退回直连，并在多个 A 记录之间轮询重试。单次请求硬超时 20 秒。

代理地址会按顺序尝试：`HTTPS_PROXY`、`HTTP_PROXY`、`http://127.0.0.1:50033`。

## 注意事项

- 备份是只增不改的镜像。SQLite 里被删掉的事件不会从 Neon 删除；需要严格一致时先 `--full`。
- 任务以交互式登录运行，注销或关机期间不会执行，恢复后由 StartWhenAvailable 补跑一次。
- `--verify` 在 watcher 持续写入时可能出现 1 条差异，属于正常现象；需要精确校验可以先停掉 watcher 再运行。
- Neon 免费套餐每项目 1 GB 存储，账户总计 20 GB。当前占用约 15%，按每月 18 MB 的增速还能用四年以上。
- `.env` 里有数据库密码，已在 `.gitignore` 中排除，不要提交到仓库。
