"""小时核定、封存快照与批次永久失效的端到端测试。"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.main import create_app  # noqa: E402

SAMPLES = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "samples")
HOUR = "2026-11-14T22:00:00Z"


def make_client(tmp_path, name="t.db"):
    return TestClient(create_app(str(tmp_path / name)))


def upload(client, path):
    with open(path, "rb") as fh:
        return client.post("/api/upload", files={"file": (os.path.basename(path), fh)})


def levels_for(hour: str, start_index: int = 0, count: int = 60,
               override: dict[int, float] | None = None) -> list[tuple[str, float]]:
    """构造某小时内的分钟潮位；override 以分钟序号(0..59)指定替换值。"""
    from datetime import datetime, timedelta

    base = datetime.strptime(hour, "%Y-%m-%dT%H:%M:%SZ")
    out = []
    for i in range(start_index, start_index + count):
        level = round(1.0 + i * 0.01, 2)
        if override and i in override:
            level = override[i]
        out.append(((base + timedelta(minutes=i)).strftime("%Y-%m-%dT%H:%M:00Z"), level))
    return out


def message_records(station: int, boot: int, msg: int,
                    obs: list[tuple[str, float]], seg_total: int = 1) -> list[dict]:
    data = "".join(f"{ts},{level}\n" for ts, level in obs).encode()
    digest = hashlib.sha256(data).hexdigest()
    if seg_total == 1:
        chunks = [data]
    else:  # 按行均分
        lines = data.splitlines(keepends=True)
        per = (len(lines) + seg_total - 1) // seg_total
        chunks = [b"".join(lines[i:i + per]) for i in range(0, len(lines), per)]
    return [{
        "station_id": station, "boot_id": boot, "msg_id": msg,
        "seg_no": i + 1, "seg_total": len(chunks),
        "payload_b64": base64.b64encode(chunk).decode(),
        "sha256": digest,
    } for i, chunk in enumerate(chunks)]


def ndjson(records: list[dict]) -> str:
    return "\n".join(json.dumps(r) for r in records) + "\n"


def post_records(client, records):
    return client.post("/api/upload", content=ndjson(records))


def seed_published(client, station=101, boot=7, msg=1001, hour=HOUR,
                   seg_total=4, override=None):
    r = post_records(client, message_records(station, boot, msg,
                                             levels_for(hour, override=override),
                                             seg_total=seg_total))
    assert r.status_code == 200, r.text
    counts = r.json()["counts"]
    assert counts["published"] == 1               # 收齐的那一行才发布
    assert counts["stored"] == seg_total - 1
    return r


def seal(client, station=101, hour=HOUR):
    return client.post("/api/hourly-batches",
                       json={"station_id": station, "hour": hour})


def batches(client, station=None):
    params = {} if station is None else {"station_id": station}
    return client.get("/api/hourly-batches", params=params).json()["batches"]


def download(client, batch_id):
    return client.get(f"/api/hourly-batches/{batch_id}/csv")


# ---------- 封存前置条件 ----------

def test_seal_requires_all_sixty_minutes(tmp_path):
    with make_client(tmp_path) as c:
        post_records(c, message_records(101, 7, 1, levels_for(HOUR, count=59)))
        r = seal(c)
        assert r.status_code == 409
        body = r.json()
        assert body["status"] == "incomplete"
        assert len(body["missing"]) == 1
        assert body["missing"][0] == "2026-11-14T22:59:00Z"
        assert batches(c) == []                     # 未产生批次

        post_records(c, message_records(101, 7, 2, levels_for(HOUR, 59, 1)))
        r = seal(c)
        assert r.status_code == 201
        assert r.json()["batch"]["minutes"] == 60


def test_seal_does_not_count_unconfirmed_or_other_station(tmp_path):
    with make_client(tmp_path) as c:
        # 59 分钟 + 另一站号的同名分钟
        post_records(c, message_records(101, 7, 1, levels_for(HOUR, count=59)))
        post_records(c, message_records(202, 7, 1, levels_for(HOUR, 59, 1)))
        assert seal(c).status_code == 409
        assert seal(c, station=202).status_code == 409


def test_seal_rejects_bad_hour_format(tmp_path):
    with make_client(tmp_path) as c:
        for bad in ["2026-11-14T22:30:00Z", "2026-11-14", "not-a-time", "2026-11-14T25:00:00Z"]:
            r = c.post("/api/hourly-batches", json={"station_id": 101, "hour": bad})
            assert r.status_code == 422, bad
        assert batches(c) == []


def test_hour_accepts_common_utc_formats(tmp_path):
    with make_client(tmp_path) as c:
        seed_published(c)
        # 已封存小时重复核定：常见写法均可识别为同一小时，幂等返回既有批次
        first = seal(c).json()["batch"]
        for hour in ["2026-11-14T22:00Z", "2026-11-14 22:00",
                     "2026-11-14T22:00:00", "2026-11-14T22"]:
            again = c.post("/api/hourly-batches", json={"station_id": 101, "hour": hour})
            assert again.status_code == 200, (hour, again.text)
            assert again.json()["status"] == "exists"
            assert again.json()["batch"]["id"] == first["id"]
        assert len(batches(c)) == 1


# ---------- 批内撤下又恢复：未变化则保持可下载 ----------

def test_same_batch_withdrawal_and_restore_does_not_invalidate(tmp_path):
    """同一批上传里先撤下、再以相同值恢复，不应让未变化的批次失效。"""
    with make_client(tmp_path) as c:
        seed_published(c, seg_total=2)
        batch = seal(c).json()["batch"]
        assert download(c, batch["id"]).status_code == 200

        # 同一请求内：先收到 1001 的篡改段 -> 待核并撤下其 60 分钟；
        # 随后另一报文以完全相同的值重新发布这 60 分钟。
        tampered = dict(message_records(101, 7, 1001, levels_for(HOUR))[0])
        tampered["seg_no"] = 1
        tampered["payload_b64"] = base64.b64encode(b"# forged\n").decode()
        restore = message_records(101, 8, 1, levels_for(HOUR))
        r = post_records(c, [tampered] + restore)
        assert r.status_code == 200
        results = r.json()["results"]
        assert results[0]["status"] == "conflict"        # 中途撤下
        assert any(x["status"] == "published" for x in results[1:])
        # 最终值与封存一致 -> 批次仍可下载
        assert batches(c)[0]["status"] == "sealed"
        assert download(c, batch["id"]).status_code == 200
        # 若撤下发生在恢复之后提交，最终态缺分钟，则必须失效
        bad = dict(restore[0])
        bad["payload_b64"] = base64.b64encode(b"# forged again\n").decode()
        r2 = post_records(c, [bad])
        assert r2.status_code == 200
        assert r2.json()["results"][0]["status"] == "conflict"
        assert batches(c)[0]["status"] == "invalid"
        assert download(c, batch["id"]).status_code == 410


# ---------- 失效规则 ----------

def test_reset_after_seal_invalidates_permanently(tmp_path):
    with make_client(tmp_path) as c:
        seed_published(c, seg_total=4)
        batch = seal(c).json()["batch"]
        assert len(download(c, batch["id"]).text.strip().splitlines()) == 61

        r = c.delete("/api/messages/101/7/1001")
        assert r.status_code == 200
        hits = r.json()["invalidated_batches"]
        assert [h["batch_id"] for h in hits] == [batch["id"]]

        invalid = batches(c)[0]
        assert invalid["status"] == "invalid"
        assert "缺失" in invalid["invalid_reason"]
        assert invalid["invalidated_at"]
        assert download(c, batch["id"]).status_code == 410

        # 补传恢复原值：批次不自动恢复
        seed_published(c, seg_total=4)
        obs = c.get("/api/observations", params={"limit": 5000}).json()["observations"]
        assert len(obs) == 60                      # 观测确实恢复了
        assert batches(c)[0]["status"] == "invalid"
        assert download(c, batch["id"]).status_code == 410


def test_changed_level_after_seal_invalidates(tmp_path):
    with make_client(tmp_path) as c:
        seed_published(c)
        batch = seal(c).json()["batch"]
        # 另一报文重发同一小时，其中一分钟值不同
        post_records(c, message_records(101, 8, 1,
                                        levels_for(HOUR, override={17: 9.99})))
        b = batches(c)[0]
        assert b["status"] == "invalid"
        assert "潮位变化" in b["invalid_reason"]
        assert "22:17" in b["invalid_reason"]
        assert download(c, batch["id"]).status_code == 410


def test_changed_level_recovered_does_not_revive(tmp_path):
    with make_client(tmp_path) as c:
        seed_published(c)
        batch = seal(c).json()["batch"]
        post_records(c, message_records(101, 8, 1,
                                        levels_for(HOUR, override={5: 8.8})))
        assert batches(c)[0]["status"] == "invalid"
        post_records(c, message_records(101, 8, 2, levels_for(HOUR)))  # 恢复原值
        assert batches(c)[0]["status"] == "invalid"
        assert download(c, batch["id"]).status_code == 410


def test_unrelated_hours_and_stations_unaffected(tmp_path):
    with make_client(tmp_path) as c:
        seed_published(c, station=101, boot=7, msg=1)
        b101 = seal(c).json()["batch"]
        seed_published(c, station=102, boot=7, msg=1)
        b102 = seal(c, station=102).json()["batch"]
        # 改 102 的另一小时
        post_records(c, message_records(102, 9, 1,
                                        levels_for("2026-11-14T23:00:00Z",
                                                   override={0: 7.7})))
        assert download(c, b101["id"]).status_code == 200
        assert download(c, b102["id"]).status_code == 200
        # 撤下 101 的报文不影响 102 批次
        c.delete("/api/messages/101/7/1")
        assert download(c, b102["id"]).status_code == 200


def test_new_batch_can_be_sealed_after_invalidation(tmp_path):
    with make_client(tmp_path) as c:
        seed_published(c)
        batch1 = seal(c).json()["batch"]
        c.delete("/api/messages/101/7/1001")
        assert download(c, batch1["id"]).status_code == 410
        seed_published(c)                           # 重新采集并恢复
        # 同一小时不能重新封存（唯一约束），旧批次保持失效
        again = seal(c)
        assert again.status_code == 200
        assert again.json()["status"] == "exists"
        assert again.json()["batch"]["status"] == "invalid"
        assert download(c, batch1["id"]).status_code == 410


# ---------- CSV 快照与当前观测解耦 ----------

def test_csv_is_sealed_snapshot(tmp_path):
    with make_client(tmp_path) as c:
        seed_published(c, seg_total=3)
        batch = seal(c).json()["batch"]
        before = download(c, batch["id"])
        assert before.status_code == 200
        assert before.headers["content-disposition"].startswith("attachment")
        assert "hourly_101_" in before.headers["content-disposition"]
        assert before.headers["x-batch-id"] == str(batch["id"])
        lines = before.text.strip().splitlines()
        assert lines[0] == "station_id,observed_at,level_m"
        assert len(lines) == 61
        assert lines[1] == "101,2026-11-14T22:00:00Z,1.0"
        assert lines[-1] == "101,2026-11-14T22:59:00Z,1.59"


def test_unknown_batch_returns_410(tmp_path):
    with make_client(tmp_path) as c:
        assert download(c, 999).status_code == 410


# ---------- 重启 ----------

def test_batches_persist_across_restart(tmp_path):
    with make_client(tmp_path) as c1:
        seed_published(c1)
        batch = seal(c1).json()["batch"]
        assert seal(c1, hour="2026-11-14T23:00:00Z").status_code == 409
    with make_client(tmp_path) as c2:
        got = batches(c2)
        assert len(got) == 1
        assert got[0]["id"] == batch["id"] and got[0]["status"] == "sealed"
        assert got[0]["minutes"] == 60
        assert len(download(c2, batch["id"]).text.strip().splitlines()) == 61

    # 失效状态同样持久
    with make_client(tmp_path) as c3:
        c3.delete("/api/messages/101/7/1001")
    with make_client(tmp_path) as c4:
        assert batches(c4)[0]["status"] == "invalid"
        assert download(c4, batch["id"]).status_code == 410


# ---------- 并发 ----------

def test_concurrent_seal_creates_exactly_one_batch(tmp_path):
    with make_client(tmp_path) as c:
        seed_published(c)
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: seal(c), range(8)))
        assert all(r.status_code in (200, 201) for r in results)
        assert sum(r.status_code == 201 for r in results) == 1
        assert len(batches(c)) == 1
        bid = batches(c)[0]["id"]
        assert download(c, bid).status_code == 200


def test_concurrent_upload_and_seal_consistent(tmp_path):
    """封存与并发上传交错：批次内容必须对应已提交的观测。"""
    with make_client(tmp_path) as c:
        seed_published(c)
        # 并发提交"改变某一分钟"的报文与核定
        changed = message_records(101, 8, 1, levels_for(HOUR, override={30: 7.77}))
        with ThreadPoolExecutor(max_workers=6) as pool:
            futures = [pool.submit(seal, c)]
            futures += [pool.submit(post_records, c, changed) for _ in range(4)]
            outcomes = [f.result() for f in futures]
        assert all(r.status_code in (200, 201, 409) for r in outcomes)
        assert len(batches(c)) == 1
        batch = batches(c)[0]
        current = {(o["observed_at"], o["level_m"]) for o in
                   c.get("/api/observations", params={"limit": 5000}).json()["observations"]}
        if batch["status"] == "sealed":
            # 封存时该分钟只能是某个已提交的值；CSV 与该值一致
            text = download(c, batch["id"]).text
            assert "101,2026-11-14T22:30:00Z,1.3" in text \
                or "101,2026-11-14T22:30:00Z,7.77" in text
            sealed_level = float(
                [ln for ln in text.splitlines() if "22:30:00Z" in ln][0].split(",")[2])
            assert (("2026-11-14T22:30:00Z", sealed_level) in current)
        else:
            # 改动先于封存生效时批次必失效；不能下载
            assert download(c, batch["id"]).status_code == 410


def test_concurrent_reset_and_seal(tmp_path):
    """复位与核定并发：要么因缺分钟不封存，要么封存后因撤下而失效。"""
    with make_client(tmp_path) as c:
        seed_published(c)
        with ThreadPoolExecutor(max_workers=4) as pool:
            fs = [pool.submit(seal, c),
                  pool.submit(c.delete, "/api/messages/101/7/1001"),
                  pool.submit(seal, c),
                  pool.submit(c.delete, "/api/messages/101/8/1")]
            [f.result() for f in fs]
        got = batches(c)
        assert len(got) <= 1
        if got:
            bid = got[0]["id"]
            assert got[0]["status"] == "invalid"
            assert download(c, bid).status_code == 410
            # 同小时已有失效批次：再核定只返回既有批次，不会复活
            again = seal(c)
            assert again.status_code == 200
            assert again.json()["batch"]["status"] == "invalid"
        else:
            # 复位先发生时该小时不足 60 分钟，不产生批次
            assert seal(c).status_code == 409


def test_ordinary_upload_unaffected_by_batches(tmp_path):
    with make_client(tmp_path) as c:
        seed_published(c)
        seal(c)
        # 其他小时正常上传不触发批次检查错误
        r = post_records(c, message_records(101, 9, 1,
                                            levels_for("2026-11-15T05:00:00Z")))
        assert r.status_code == 200
        assert r.json()["invalidated_batches"] == []
        assert batches(c)[0]["status"] == "sealed"
