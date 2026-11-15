"""报文段归组、组装、校验与观测解析的核心逻辑。

归组键：(station_id 站号, boot_id 设备启动号, msg_id 报文号)。

每条 NDJSON 记录字段：
    station_id  站号（整数）
    boot_id     设备启动号（整数）
    msg_id      报文号（整数）
    seg_no      段号，1..seg_total（整数）
    seg_total   总段数（整数 >= 1）
    payload_b64 本段正文的 Base64
    sha256      整报（全部段按段号拼接后的字节）的 SHA-256 十六进制

一致性规则：
  * 完全相同的段重投 -> 幂等忽略（duplicate），只入库一次；
  * 同一报文的段内容 / 总段数 / 整报摘要出现冲突，或段号越界，
    或组装后摘要与登记摘要不符，或 CSV 解析失败
    -> 报文标记 pending_review（待核），并立即撤下该报文已发布的观测；
  * 待核报文后续的段一律忽略，直到监测员复位（DELETE）后重新采集。
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import math
import sqlite3
from datetime import datetime, timedelta, timezone

REQUIRED_FIELDS = (
    "station_id",
    "boot_id",
    "msg_id",
    "seg_no",
    "seg_total",
    "payload_b64",
    "sha256",
)

MSG_KEY = "station_id=? AND boot_id=? AND msg_id=?"


class LineError(ValueError):
    """单行记录本身不合法（无法归属到某个报文）。"""


def _as_int(value, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise LineError(f"字段 {name} 必须是整数")
    return value


def validate_record(obj) -> dict:
    """校验并规范化一条 NDJSON 记录，返回字典（payload 为 bytes）。"""
    if not isinstance(obj, dict):
        raise LineError("记录必须是 JSON 对象")
    missing = [f for f in REQUIRED_FIELDS if f not in obj]
    if missing:
        raise LineError(f"缺少字段: {', '.join(missing)}")

    station_id = _as_int(obj["station_id"], "station_id")
    boot_id = _as_int(obj["boot_id"], "boot_id")
    msg_id = _as_int(obj["msg_id"], "msg_id")
    seg_no = _as_int(obj["seg_no"], "seg_no")
    seg_total = _as_int(obj["seg_total"], "seg_total")
    if seg_total < 1:
        raise LineError("seg_total 必须 >= 1")
    if seg_no < 1:
        raise LineError("seg_no 必须 >= 1")

    payload_b64 = obj["payload_b64"]
    if not isinstance(payload_b64, str):
        raise LineError("payload_b64 必须是字符串")
    try:
        payload = base64.b64decode(payload_b64, validate=True)
    except (binascii.Error, ValueError):
        raise LineError("payload_b64 不是合法的 Base64")

    sha256 = obj["sha256"]
    if not isinstance(sha256, str):
        raise LineError("sha256 必须是字符串")
    sha256 = sha256.strip().lower()
    if len(sha256) != 64 or any(c not in "0123456789abcdef" for c in sha256):
        raise LineError("sha256 必须是 64 位十六进制")

    return {
        "station_id": station_id,
        "boot_id": boot_id,
        "msg_id": msg_id,
        "seg_no": seg_no,
        "seg_total": seg_total,
        "payload": payload,
        "sha256": sha256,
    }


def parse_tide_csv(data: bytes) -> list[tuple[str, float]]:
    """解析整报正文：UTC 分钟潮位 CSV。

    每行：`YYYY-MM-DDTHH:MM:00Z,<潮位米>`；允许空行与 `#` 开头的注释行。
    返回 [(observed_at, level_m), ...]，任一数据行非法即抛 ValueError。
    """
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"正文不是合法 UTF-8: {exc}") from exc

    obs: list[tuple[str, float]] = []
    for lineno, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = [p.strip() for p in line.split(",")]
        if len(parts) != 2 or not parts[0] or not parts[1]:
            raise ValueError(f"第 {lineno} 行格式错误（应为 时间,潮位）: {line!r}")
        try:
            dt = datetime.strptime(parts[0], "%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            raise ValueError(
                f"第 {lineno} 行时间格式错误（应为 YYYY-MM-DDTHH:MM:00Z）: {parts[0]!r}"
            ) from None
        if dt.second != 0:
            raise ValueError(f"第 {lineno} 行不是整分钟: {parts[0]!r}")
        try:
            level = float(parts[1])
        except ValueError:
            raise ValueError(f"第 {lineno} 行潮位不是数值: {parts[1]!r}") from None
        if not math.isfinite(level):
            raise ValueError(f"第 {lineno} 行潮位不是有限数值: {parts[1]!r}")
        obs.append((dt.strftime("%Y-%m-%dT%H:%M:00Z"), level))

    if not obs:
        raise ValueError("报文不含任何观测行")
    return obs


def _flag_pending(con: sqlite3.Connection, key: tuple, reason: str) -> None:
    """标记报文待核，并撤下该报文已发布的全部观测。"""
    con.execute(
        f"UPDATE messages SET status='pending_review', reason=?, "
        f"updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE {MSG_KEY}",
        (reason, *key),
    )
    con.execute(f"DELETE FROM observations WHERE {MSG_KEY}", key)


def _batch_first_problem(con: sqlite3.Connection, station_ids: list[int]) -> dict[int, str]:
    """比较封存快照与当前已确认观测，返回 {batch_id: 首个问题}。

    只有 sealed 批次会被返回；invalid 批次永不修复。
    """
    ids = sorted({s for s in station_ids})
    if not ids:
        return {}
    marks = ",".join("?" * len(ids))
    rows = con.execute(
        "SELECT b.id, s.observed_at, s.level_m AS sealed_level,"
        "       o.level_m AS current_level"
        " FROM hourly_batches b"
        " JOIN sealed_observations s ON s.batch_id=b.id"
        " LEFT JOIN observations o"
        "   ON o.station_id=b.station_id AND o.observed_at=s.observed_at"
        f" WHERE b.status='sealed' AND b.station_id IN ({marks})"
        " ORDER BY b.id, s.observed_at",
        ids,
    ).fetchall()
    first: dict[int, str] = {}
    for row in rows:
        if row["current_level"] is None:
            reason = f"封存分钟 {row['observed_at']} 缺失"
        elif row["current_level"] != row["sealed_level"]:
            # SQLite REAL 与 Python float 往返无损，按精确值比较
            reason = (f"封存分钟 {row['observed_at']} 潮位变化："
                      f"{row['sealed_level']!r} -> {row['current_level']!r}")
        else:
            continue
        first.setdefault(row["id"], reason)
    return first


def _invalidate_batches(con: sqlite3.Connection, first: dict[int, str]) -> list[dict]:
    """把批次置为永久失效；仅 sealed -> invalid，不回退、不自动恢复。"""
    out = []
    for batch_id, reason in first.items():
        con.execute(
            "UPDATE hourly_batches SET status='invalid', invalid_reason=?,"
            " invalidated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')"
            " WHERE id=? AND status='sealed'",
            (reason, batch_id),
        )
        out.append({"batch_id": batch_id, "reason": reason})
    return out


def _validate_after_write(con: sqlite3.Connection, station_ids) -> list[dict]:
    """写事务提交前统一核对批次。

    在整批上传的最终状态上比较，而不是逐行比较：同一批上传里某分钟
    先被撤下、后又被相同值补回时，封存批次不受影响。
    """
    return _invalidate_batches(con, _batch_first_problem(con, list(station_ids)))


def parse_hour(value: str) -> tuple[str, str]:
    """把小时输入规范化为 (hour_start, hour_end)，均为 ISO UTC 整点字符串。

    接受 `2026-11-14T22:00:00Z`、`2026-11-14T22:00Z`、`2026-11-14 22:00`
    等写法；带时区偏移时先换算到 UTC。分钟/秒必须为 0。
    """
    if not isinstance(value, str):
        raise ValueError("小时必须是字符串")
    text = value.strip()
    if not text:
        raise ValueError("小时不能为空")

    iso = text[:-1] + "+00:00" if text.endswith(("Z", "z")) else text
    try:
        dt = datetime.fromisoformat(iso)
    except ValueError:
        raise ValueError("小时格式错误，应为 YYYY-MM-DDTHH:00:00Z") from None
    # 必须有明确的小时部分（拒绝只有日期的输入）
    time_part = text[10:].lstrip(" T")
    if not time_part:
        raise ValueError("小时格式错误，请同时给出整点小时")
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc)
    # 换算到 UTC 后再检查整点：+05:45 等偏移换算后可能不再对齐整点
    if dt.minute or dt.second or dt.microsecond:
        raise ValueError("小时必须对齐到 UTC 整点（分、秒为 0）")
    start = dt.strftime("%Y-%m-%dT%H:00:00Z")
    end = (dt + timedelta(hours=1)).strftime("%Y-%m-%dT%H:00:00Z")
    return start, end


def _hour_minutes(hour_start: str) -> list[str]:
    start = datetime.strptime(hour_start, "%Y-%m-%dT%H:%M:%SZ")
    return [(start + timedelta(minutes=i)).strftime("%Y-%m-%dT%H:%M:00Z")
            for i in range(60)]


def _batch_dict(row) -> dict:
    return {
        "id": row["id"],
        "station_id": row["station_id"],
        "hour": row["hour_start"],
        "status": row["status"],
        "created_at": row["created_at"],
        "invalidated_at": row["invalidated_at"],
        "invalid_reason": row["invalid_reason"],
        "minutes": row["minutes"],
        "download_url": f"/api/hourly-batches/{row['id']}/csv",
    }


_BATCH_SELECT = (
    "SELECT b.*,"
    " (SELECT COUNT(*) FROM sealed_observations s WHERE s.batch_id=b.id) AS minutes"
    " FROM hourly_batches b"
)


def seal_hourly_batch(con: sqlite3.Connection, station_id: int, hour: str) -> dict:
    """小时内 60 个整分钟均有已确认观测时封存批次；否则不产生批次。"""
    hour_start, hour_end = parse_hour(hour)
    con.execute("BEGIN IMMEDIATE")
    batch_id = None
    try:
        existing = con.execute(
            _BATCH_SELECT + " WHERE b.station_id=? AND b.hour_start=?",
            (station_id, hour_start),
        ).fetchone()
        if existing is not None:
            con.execute("ROLLBACK")
            return {"status": "exists", "batch": _batch_dict(existing)}

        rows = con.execute(
            "SELECT observed_at, level_m FROM observations"
            " WHERE station_id=? AND observed_at>=? AND observed_at<?"
            " ORDER BY observed_at",
            (station_id, hour_start, hour_end),
        ).fetchall()
        levels = {r["observed_at"]: r["level_m"] for r in rows}
        expected = _hour_minutes(hour_start)
        missing = [ts for ts in expected if ts not in levels]
        if missing:
            con.execute("ROLLBACK")
            return {"status": "incomplete", "hour": hour_start,
                    "present": len(levels), "required": len(expected),
                    "missing": missing}

        cur = con.execute(
            "INSERT INTO hourly_batches (station_id, hour_start, hour_end)"
            " VALUES (?, ?, ?)",
            (station_id, hour_start, hour_end),
        )
        batch_id = cur.lastrowid
        con.executemany(
            "INSERT INTO sealed_observations (batch_id, observed_at, level_m)"
            " VALUES (?, ?, ?)",
            [(batch_id, ts, levels[ts]) for ts in expected],
        )
        con.execute("COMMIT")
    except sqlite3.IntegrityError:
        # 另一进程刚封存了同一小时：回滚后返回既有批次（幂等）
        if con.in_transaction:
            con.execute("ROLLBACK")
        existing = con.execute(
            _BATCH_SELECT + " WHERE b.station_id=? AND b.hour_start=?",
            (station_id, hour_start),
        ).fetchone()
        if existing is None:
            raise
        return {"status": "exists", "batch": _batch_dict(existing)}
    except BaseException:
        if con.in_transaction:
            con.execute("ROLLBACK")
        raise
    row = con.execute(_BATCH_SELECT + " WHERE b.id=?", (batch_id,)).fetchone()
    return {"status": "sealed", "batch": _batch_dict(row)}


def list_hourly_batches(con: sqlite3.Connection, station_id: int | None = None) -> list[dict]:
    sql = _BATCH_SELECT
    args: list = []
    if station_id is not None:
        sql += " WHERE b.station_id=?"
        args.append(station_id)
    sql += " ORDER BY b.station_id, b.hour_start"
    return [_batch_dict(row) for row in con.execute(sql, args).fetchall()]


def hourly_batch_csv(con: sqlite3.Connection, batch_id: int) -> dict | None:
    """可下载批次的 CSV 内容；失效或不存在时返回 None。

    只读取封存快照，绝不回读当前观测：批次失效后内容不再提供。
    """
    rows = con.execute(
        "SELECT b.id, b.station_id, b.hour_start, b.status, b.created_at,"
        "       b.invalidated_at, b.invalid_reason, s.observed_at, s.level_m,"
        "       (SELECT COUNT(*) FROM sealed_observations x"
        "         WHERE x.batch_id=b.id) AS minutes"
        " FROM hourly_batches b"
        " LEFT JOIN sealed_observations s ON s.batch_id=b.id"
        " WHERE b.id=? AND b.status='sealed'"
        " ORDER BY s.observed_at",
        (batch_id,),
    ).fetchall()
    if not rows:
        return None
    first = rows[0]
    if len(rows) != first["minutes"] or any(r["observed_at"] is None for r in rows):
        return None
    lines = ["station_id,observed_at,level_m"]
    lines += [f"{first['station_id']},{r['observed_at']},{r['level_m']}" for r in rows]
    name = ("hourly_%s_%s.csv"
            % (first["station_id"], first["hour_start"].replace(":", "").replace("-", "")))
    return {"batch": _batch_dict(first), "filename": name,
            "csv": "\n".join(lines) + "\n"}


def ingest_record(con: sqlite3.Connection, rec: dict) -> dict:
    """在调用方的事务内处理一条已校验记录，返回该行的处理结果。"""
    key = (rec["station_id"], rec["boot_id"], rec["msg_id"])
    base = {
        "station_id": key[0],
        "boot_id": key[1],
        "msg_id": key[2],
        "seg_no": rec["seg_no"],
    }

    row = con.execute(
        f"SELECT seg_total, sha256, status FROM messages WHERE {MSG_KEY}", key
    ).fetchone()
    if row is None:
        con.execute(
            "INSERT INTO messages (station_id, boot_id, msg_id, seg_total, sha256)"
            " VALUES (?, ?, ?, ?, ?)",
            (*key, rec["seg_total"], rec["sha256"]),
        )
        row = {"seg_total": rec["seg_total"], "sha256": rec["sha256"], "status": "collecting"}

    if row["status"] == "pending_review":
        return {**base, "status": "ignored", "detail": "报文待核，段未入库"}

    # ---- 冲突检测：总段数 / 整报摘要 / 段号越界 / 同段号不同内容 ----
    if rec["seg_total"] != row["seg_total"]:
        reason = f"总段数冲突：已登记 {row['seg_total']}，新记录为 {rec['seg_total']}"
        _flag_pending(con, key, reason)
        return {**base, "status": "conflict", "detail": reason}
    if rec["sha256"] != row["sha256"]:
        reason = "整报 SHA-256 摘要冲突：同报文出现不同摘要"
        _flag_pending(con, key, reason)
        return {**base, "status": "conflict", "detail": reason}
    if rec["seg_no"] > row["seg_total"]:
        reason = f"段号越界：段 {rec['seg_no']} 超出总段数 {row['seg_total']}"
        _flag_pending(con, key, reason)
        return {**base, "status": "conflict", "detail": reason}

    existing = con.execute(
        f"SELECT payload FROM segments WHERE {MSG_KEY} AND seg_no=?",
        (*key, rec["seg_no"]),
    ).fetchone()
    if existing is not None:
        if existing["payload"] == rec["payload"]:
            return {**base, "status": "duplicate", "detail": "相同段重投，幂等忽略"}
        reason = f"段 {rec['seg_no']} 内容冲突：同段号不同正文"
        _flag_pending(con, key, reason)
        return {**base, "status": "conflict", "detail": reason}

    con.execute(
        "INSERT INTO segments (station_id, boot_id, msg_id, seg_no, payload)"
        " VALUES (?, ?, ?, ?, ?)",
        (*key, rec["seg_no"], rec["payload"]),
    )

    # ---- 收齐则组装、校验、解析、发布 ----
    rows = con.execute(
        f"SELECT seg_no, payload FROM segments WHERE {MSG_KEY} ORDER BY seg_no", key
    ).fetchall()
    if len(rows) < row["seg_total"]:
        return {**base, "status": "stored", "detail": "段已入库"}

    data = b"".join(r["payload"] for r in rows)
    digest = hashlib.sha256(data).hexdigest()
    if digest != row["sha256"]:
        reason = f"组装后摘要校验不符：登记 {row['sha256'][:12]}…，实算 {digest[:12]}…"
        _flag_pending(con, key, reason)
        return {**base, "status": "conflict", "detail": reason}

    try:
        obs = parse_tide_csv(data)
    except ValueError as exc:
        reason = f"潮位 CSV 解析失败：{exc}"
        _flag_pending(con, key, reason)
        return {**base, "status": "conflict", "detail": reason}

    con.executemany(
        "INSERT INTO observations"
        " (station_id, observed_at, level_m, boot_id, msg_id)"
        " VALUES (?, ?, ?, ?, ?)"
        " ON CONFLICT (station_id, observed_at) DO UPDATE SET"
        " level_m=excluded.level_m, boot_id=excluded.boot_id, msg_id=excluded.msg_id"
        " WHERE observations.level_m IS NOT excluded.level_m",
        [(key[0], ts, level, key[1], key[2]) for ts, level in obs],
    )
    con.execute(
        f"UPDATE messages SET status='published', reason=NULL, "
        f"updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE {MSG_KEY}",
        key,
    )
    return {**base, "status": "published", "detail": f"报文完整，发布 {len(obs)} 条观测"}


def ingest_ndjson(con: sqlite3.Connection, text: str) -> dict:
    """处理一整段 NDJSON 文本。调用方须持有写锁；本函数自成一个事务。"""
    results = []
    counts = {"stored": 0, "published": 0, "duplicate": 0,
              "conflict": 0, "ignored": 0, "error": 0}
    con.execute("BEGIN IMMEDIATE")
    try:
        for lineno, raw in enumerate(text.splitlines(), 1):
            line = raw.strip()
            if not line:
                continue
            try:
                rec = validate_record(json.loads(line))
                outcome = ingest_record(con, rec)
            except LineError as exc:
                outcome = {"status": "error", "detail": str(exc)}
            except Exception as exc:  # 单行异常不拖垮整批
                outcome = {"status": "error", "detail": f"记录解析失败: {exc}"}
            outcome["line"] = lineno
            results.append(outcome)
            counts[outcome["status"]] = counts.get(outcome["status"], 0) + 1
        # 在整批的最终状态上核对封存批次：批内中间态（如先撤后补）不算失效
        impacted = _validate_after_write(
            con, {r["station_id"] for r in results if "station_id" in r}
        )
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise
    return {"lines": len(results), "counts": counts, "results": results,
            "invalidated_batches": impacted}


def list_messages(con: sqlite3.Connection) -> list[dict]:
    """全部报文的状态，含已收段数、缺段清单、待核原因、观测数。"""
    msgs = con.execute(
        "SELECT m.*, "
        " (SELECT COUNT(*) FROM observations o WHERE o.station_id=m.station_id"
        "   AND o.boot_id=m.boot_id AND o.msg_id=m.msg_id) AS obs_count "
        "FROM messages m ORDER BY m.station_id, m.boot_id, m.msg_id"
    ).fetchall()
    out = []
    for m in msgs:
        key = (m["station_id"], m["boot_id"], m["msg_id"])
        have = {
            r["seg_no"]
            for r in con.execute(
                f"SELECT seg_no FROM segments WHERE {MSG_KEY}", key
            )
        }
        missing = sorted(set(range(1, m["seg_total"] + 1)) - have)
        out.append({
            "station_id": m["station_id"],
            "boot_id": m["boot_id"],
            "msg_id": m["msg_id"],
            "seg_total": m["seg_total"],
            "received": len(have),
            "missing_segments": missing,
            "status": m["status"],
            "reason": m["reason"],
            "observations": m["obs_count"],
            "updated_at": m["updated_at"],
        })
    return out


def reset_message(con: sqlite3.Connection, station_id: int, boot_id: int, msg_id: int) -> dict:
    """删除一个报文及其段与观测（待核处理后重新采集）。

    返回 {"deleted": 是否存在, "invalidated_batches": [...]}。删观测后若有
    封存分钟缺失，相应批次在同一事务内永久失效。
    """
    key = (station_id, boot_id, msg_id)
    con.execute("BEGIN IMMEDIATE")
    try:
        cur = con.execute(f"DELETE FROM messages WHERE {MSG_KEY}", key)
        con.execute(f"DELETE FROM segments WHERE {MSG_KEY}", key)
        con.execute(f"DELETE FROM observations WHERE {MSG_KEY}", key)
        impacted = _validate_after_write(con, [station_id])
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise
    return {"deleted": cur.rowcount > 0, "invalidated_batches": impacted}
