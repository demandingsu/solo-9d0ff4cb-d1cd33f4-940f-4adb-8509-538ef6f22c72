# 沿海潮位卫星短报文采集

沿海潮位站经卫星短报文回传分钟潮位，网关侧会留下**拆段、乱序、重投**的 NDJSON
记录。本服务（FastAPI + SQLite）按 `站号 + 设备启动号 + 报文号` 归组报文段，
收齐后按段号拼接字节、校验整报 SHA-256，解析其中的 UTC 分钟潮位 CSV 并发布，
供浏览器查询与下载；采集网页还提供**小时核定**：某站某 UTC 小时的 60 个整分钟
全部确认后封存为批次，提供专属 CSV。

## 快速启动

### Docker Compose（推荐）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

数据库持久化在命名卷 `tide-data`（容器内 `/data/tide.db`），重启不丢状态。

### 本地运行

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload          # 默认库文件 ./data/tide.db，可用 DB_PATH 覆盖
```

### 运行测试

```bash
pip install -r requirements-dev.txt
pytest
```

## 使用流程

1. 浏览器打开 `http://localhost:8000`，在「1. 上传」选择 NDJSON 文件；
2. 「2. 报文状态」列出每个报文的已收/总段数、**缺段清单**、状态与**待核原因**；
3. 「3. 小时核定与封存批次」输入站号与 UTC 小时（如 `2026-11-14T22:00:00Z`）
   点「核定并封存」：该小时 60 个整分钟均有已确认潮位才封存并给出专属 CSV；
   下方表格列出全部批次的状态、分钟数、失效时间与原因，失效批次不可下载；
4. 「4. 已确认观测」预览并可按站号过滤、下载 CSV —— 只含组装校验通过的完整报文；
5. 待核报文处理完后，点「复位重采」清除该报文，等待岸台重新上送。

命令行等价操作：

```bash
curl -F "file=@samples/sample_upload.ndjson" http://localhost:8000/api/upload
curl http://localhost:8000/api/messages
curl -X POST -H 'Content-Type: application/json' \
     -d '{"station_id":101,"hour":"2026-11-14T22:00:00Z"}' \
     http://localhost:8000/api/hourly-batches
curl http://localhost:8000/api/hourly-batches
curl -OJ http://localhost:8000/api/hourly-batches/1/csv
curl -OJ "http://localhost:8000/api/observations.csv?station_id=101"
```

## NDJSON 记录格式

每行一个 JSON 对象：

| 字段 | 类型 | 说明 |
|---|---|---|
| `station_id` | int | 站号 |
| `boot_id` | int | 设备启动号 |
| `msg_id` | int | 报文号 |
| `seg_no` | int | 段号，1..seg_total |
| `seg_total` | int | 总段数（≥1） |
| `payload_b64` | string | 本段正文的 Base64 |
| `sha256` | string | **整报**（全部段按段号拼接）的 SHA-256 十六进制 |

整报正文为 UTF-8 的分钟潮位 CSV：每行 `YYYY-MM-DDTHH:MM:00Z,<潮位米>`，
允许空行与 `#` 注释行（样例首行即注释）。观测按 `(站号, 分钟)` 唯一。

## 一致性与并发语义

- **重投幂等**：完全相同的段只入库一次；观测按 `(站号, 分钟)` 去重，
  重复上传同一文件不会产生重复数据。
- **乱序**：段按归组键随意到达，收齐即组装。
- **待核（pending_review）**：同一报文出现以下任一情况即转待核，并**立即撤下
  该报文已发布的观测**——段内容冲突（同段号不同正文）、总段数冲突、整报摘要
  冲突、段号越界、组装后摘要与登记摘要不符、CSV 解析失败。待核报文后续到达
  的段一律忽略，复位后方可重新采集。
- **并发**：上传在写锁 + 单事务（`BEGIN IMMEDIATE`）内串行落库，WAL 模式 +
  `busy_timeout` 保证多请求/多进程下不错乱；一批上传要么全部生效要么回滚。
- **重启**：全部状态在 SQLite 中，观测只在报文校验通过后才写入，因此重启后
  查询天然只显示完整报文的观测，重投依旧幂等。

## 小时核定与批次失效

- **封存条件**：站号 + 对齐到整点的 UTC 小时，该小时 60 个整分钟都在
  `observations` 中（即均来自已确认发布的报文）才封存；缺任一分钟返回
  `409`，并列出缺失分钟，不产生批次。重复核定同一小时返回既有批次（幂等）。
- **封存快照**：封存时把 60 分钟复制到 `sealed_observations`；批次 CSV 只读
  快照，与该小时之后观测如何变化解耦。
- **永久失效**：每次上传或复位提交后，在同一写事务内、按整批的**最终状态**
  比较快照与当前观测；任一分钟缺失或潮位值变化，批次即 `invalid`，专属 CSV
  永久返回 `410`。失效只允许 `sealed -> invalid`，即使补传恢复原值也不自动恢复。
- **批内中间态不误杀**：同一批上传中某分钟先被撤下、随后又补回相同值（或
  被另一报文以相同值覆盖）时，最终值未变化，批次保持可下载。比较只看
  `(站号, 分钟, 潮位值)`，不看来源报文。
- **并发与一致性**：上传、复位、核定都经同一把写锁串行执行，各自是
  `BEGIN IMMEDIATE` 单事务；核定的快照内容与批次状态对应已提交数据，并发
  上传不会让批次内容与观测错位。批次与快照持久化于 SQLite，重启后仍可查看。

## 样例文件

- `samples/sample_upload.ndjson` —— 乱序 + 重投的正常样例：报文 1001（4 段）、
  1002（3 段）可发布；1003 缺第 2 段，网页「缺段」列显示 `2`。
- `samples/sample_conflicts.ndjson` —— 冲突样例：段内容冲突（2001）、总段数
  冲突（2002）、组装摘要校验不符（2003）、**先发布后收到篡改段而撤下观测**
  （2004）、整报摘要冲突（2005）。
- `python3 samples/generate_samples.py` 可重新生成以上两个文件。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` | 采集网页 |
| POST | `/api/upload` | 上传 NDJSON（multipart 文件 `file` 或裸文本），返回逐行结果 |
| GET | `/api/messages` | 全部报文：已收段、缺段、状态、待核原因、观测数 |
| GET | `/api/observations?station_id=&limit=` | 已确认观测 JSON |
| GET | `/api/observations.csv?station_id=` | 已确认观测 CSV 下载 |
| DELETE | `/api/messages/{station}/{boot}/{msg}` | 复位报文（删除其段与观测），返回因此失效的批次 |
| POST | `/api/hourly-batches` | 小时核定：`{"station_id":101,"hour":"2026-11-14T22:00:00Z"}`；齐全 `201` 封存，不齐 `409`，格式错 `422`，已存在 `200` |
| GET | `/api/hourly-batches?station_id=` | 全部批次状态、分钟数、失效时间与原因 |
| GET | `/api/hourly-batches/{id}/csv` | 批次专属 CSV；不存在或已失效返回 `410` |

## 目录结构

```
app/
  main.py            FastAPI 路由与生命周期
  core.py            归组、组装、校验、解析、待核/撤下、小时核定与批次失效
  db.py              SQLite 连接与 schema（含批次与封存快照表）
  static/index.html  采集网页（上传 / 报文状态 / 小时核定 / 观测与批次 CSV）
samples/             样例生成器与样例 NDJSON
tests/               端到端测试（乱序/重投/冲突/撤下/重启/并发/小时核定）
Dockerfile, docker-compose.yml
```
