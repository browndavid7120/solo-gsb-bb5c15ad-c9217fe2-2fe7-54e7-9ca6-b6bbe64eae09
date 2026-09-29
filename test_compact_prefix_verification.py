"""归档前缀校验修复的端到端验证。

覆盖:
- 篡改(内容哈希/前后哈希/序号缺口)先被 /integrity 检出时, /retention/compact
  的实际归档与 dry_run 都返回 409, first_break 的 seq/event_id/reason 与
  /integrity 一致, 且事件、日计数视图、链锚点保持原状;
- 断点不在拟归档前缀内时, 前缀仍可按既有边界归档, 断点在归档后依旧可检出;
- 已覆盖区间保持幂等; 无断点时原有 dry_run / compacted / noop 响应不变。
"""
import json
import os
import sqlite3
import tempfile

_tmp = tempfile.mkdtemp()
os.environ["DB_PATH"] = os.path.join(_tmp, "seed.db")

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402
from app import db  # noqa: E402

_client_ctx = TestClient(app)
client = _client_ctx.__enter__()
ok = 0
fail = 0


def check(name, cond, extra=""):
    global ok, fail
    if cond:
        ok += 1
        print(f"PASS {name}")
    else:
        fail += 1
        print(f"FAIL {name} {extra}")


def use_db(name):
    """切换到一个全新的临时库并建表。"""
    path = os.path.join(_tmp, name)
    db.DB_PATH = path
    db.init_db()
    return path


def raw(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def seed_events(path, n, prefix="evt", consumer="worker"):
    """经正常 API 收 n 条事件, 建全量视图, consumer 整批确认到 n(满足安全边界)。"""
    for i in range(1, n + 1):
        r = client.post("/events", json={
            "event_id": f"{prefix}-{i}",
            "occurred_at": f"2026-09-29T10:{i:02d}:00Z",
            "payload": {"v": i},
        })
        assert r.status_code == 201, r.text
    r = client.post("/views", json={"name": f"all-{prefix}"})
    assert r.status_code == 201, r.text
    r = client.post(f"/consumers/{consumer}/pull", json={"lease_seconds": 300})
    token = r.json()["lease_token"]
    assert len(r.json()["events"]) == n, r.text
    r = client.post(f"/consumers/{consumer}/ack", json={"lease_token": token})
    assert r.status_code == 200 and r.json()["cursor_seq"] == n, r.text


def event_count(path):
    with raw(path) as c:
        return c.execute("SELECT COUNT(*) FROM events").fetchone()[0]


def anchor(path):
    with raw(path) as c:
        row = c.execute("SELECT anchor_seq, anchor_hash FROM chain_anchor WHERE id=1").fetchone()
        return (row[0], row[1]) if row else (0, "")


def integrity_break():
    r = client.get("/integrity")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["verified"] is False, body
    return body["first_break"]


# ---------- A. 内容篡改(hash_mismatch): dry_run 与实际归档都 409, 状态不变 ----------
path = use_db("a.db")
seed_events(path, 4)

# 直接在库中篡改 seq=2 的 payload(绕过 API), /integrity 在 seq=2 检出
with raw(path) as c:
    original_payload = c.execute(
        "SELECT payload FROM events WHERE id=2").fetchone()[0]
    c.execute("UPDATE events SET payload=? WHERE id=2", (json.dumps({"v": 666}),))

br = integrity_break()
check("integrity flags tampered seq 2",
      br == {"seq": 2, "event_id": "evt-2", "reason": "hash_mismatch"}, br)

before_count, before_anchor = event_count(path), anchor(path)
before_view = client.get("/views/all-evt").json()["total"]

# dry_run: 409 + 断点, 不写库
r = client.post("/retention/compact", json={"cutoff_seq": 4, "dry_run": True})
body = r.json()
check("dry_run 409 on tampered prefix", r.status_code == 409, r.text)
check("dry_run break mirrors /integrity",
      body.get("first_break") == br and body.get("cutoff_seq") == 4
      and body.get("dry_run") is True
      and body.get("detail") == "chain verification failed within compact prefix", body)
check("dry_run leaves events/anchor/views untouched",
      event_count(path) == before_count and anchor(path) == before_anchor
      and client.get("/views/all-evt").json()["total"] == before_view,
      (event_count(path), anchor(path), client.get("/views/all-evt").json()))

# 实际归档: 同样 409, 同样不留改动
r = client.post("/retention/compact", json={"cutoff_seq": 4})
body = r.json()
check("actual compact 409 on tampered prefix", r.status_code == 409, r.text)
check("actual break mirrors /integrity",
      body.get("first_break") == br and body.get("dry_run") is False, body)
check("actual compact leaves events/anchor/views untouched",
      event_count(path) == 4 and anchor(path) == (0, "")
      and client.get("/views/all-evt").json()["total"] == 4,
      (event_count(path), anchor(path), client.get("/views/all-evt").json()))

# 断点不在拟归档前缀内: cutoff=1 只归档干净的 seq=1, 仍可成功
r = client.post("/retention/compact", json={"cutoff_seq": 1, "dry_run": True})
check("dry_run clean prefix before break",
      r.status_code == 200 and r.json()["would_delete"] == 1
      and r.json()["through_seq"] == 1, r.text)
r = client.post("/retention/compact", json={"cutoff_seq": 1})
check("archive clean prefix before break",
      r.status_code == 200 and r.json()["status"] == "compacted"
      and r.json()["deleted"] == 1 and r.json()["anchor_seq"] == 1, r.text)
with raw(path) as c:
    hash1 = c.execute("SELECT hash FROM events WHERE id=1").fetchone()
# seq=1 已物理删除, 锚点记录其 hash
check("seq1 deleted and anchor holds its hash",
      event_count(path) == 3 and hash1 is None
      and anchor(path)[0] == 1, (event_count(path), anchor(path)))

# 断点依旧可被 /integrity 检出(归档没有洗白篡改), 此时从锚点 1 起首条即断点
br2 = integrity_break()
check("tamper still detected after partial archive",
      br2 == {"seq": 2, "event_id": "evt-2", "reason": "hash_mismatch"}, br2)

# 已覆盖区间保持幂等(即使其后仍有断点)
r = client.post("/retention/compact", json={"cutoff_seq": 1})
check("covered range is noop", r.status_code == 200
      and r.json()["status"] == "noop" and r.json()["deleted"] == 0
      and r.json()["anchor_seq"] == 1, r.text)
r = client.post("/retention/compact", json={"cutoff_seq": 1, "dry_run": True})
check("covered range dry_run is noop-style", r.status_code == 200
      and r.json()["status"] == "dry_run" and r.json()["would_delete"] == 0
      and r.json()["from_seq"] == 2 and r.json()["through_seq"] == 1, r.text)

# 修复篡改后: 剩余前缀无断点, 原有 dry_run/compacted 响应与 /integrity 约定不变
with raw(path) as c:
    c.execute("UPDATE events SET payload=? WHERE id=2", (original_payload,))
r = client.get("/integrity").json()
check("integrity verified after repair",
      r["verified"] is True and r["checked"] == 3 and r["anchor_seq"] == 1, r)
r = client.post("/retention/compact", json={"cutoff_seq": 4, "dry_run": True})
check("dry_run clean tail", r.status_code == 200 and r.json() == {
    "status": "dry_run", "dry_run": True, "would_delete": 3,
    "from_seq": 2, "through_seq": 4, "safe_through_seq": 4}, r.text)
r = client.post("/retention/compact", json={"cutoff_seq": 4})
check("compact clean tail", r.status_code == 200
      and r.json()["status"] == "compacted" and r.json()["deleted"] == 3
      and r.json()["anchor_seq"] == 4 and r.json()["anchor_hash"]
      and r.json()["from_seq"] == 2 and r.json()["through_seq"] == 4
      and r.json()["safe_through_seq"] == 4, r.text)
r = client.get("/integrity").json()
check("integrity verified empty tail",
      r == {"verified": True, "checked": 0, "first_break": None,
            "anchor_seq": 4}, r)
check("view decremented with tail archive",
      client.get("/views/all-evt").json()["total"] == 0)

# ---------- B. 前后哈希篡改(prev_hash_mismatch) ----------
path = use_db("b.db")
seed_events(path, 3, prefix="b")

with raw(path) as c:
    c.execute("UPDATE events SET prev_hash=? WHERE id=2", ("deadbeef",))

br = integrity_break()
check("integrity flags prev_hash tamper",
      br == {"seq": 2, "event_id": "b-2", "reason": "prev_hash_mismatch"}, br)

for dry in (True, False):
    r = client.post("/retention/compact", json={"cutoff_seq": 3, "dry_run": dry})
    check(f"compact(dry_run={dry}) 409 prev_hash_mismatch",
          r.status_code == 409 and r.json().get("first_break") == br
          and r.json().get("dry_run") is dry, r.text)
check("prev_hash tamper: nothing changed",
      event_count(path) == 3 and anchor(path) == (0, ""),
      (event_count(path), anchor(path)))

r = client.post("/retention/compact", json={"cutoff_seq": 1})
check("archive clean seq1 despite later prev_hash break",
      r.status_code == 200 and r.json()["anchor_seq"] == 1, r.text)
check("prev_hash break survives partial archive", integrity_break() == br, br)

# ---------- C. 序号缺口(seq_gap): 直接物理删除 seq=2 ----------
path = use_db("c.db")
seed_events(path, 3, prefix="c")

with raw(path) as c:
    c.execute("DELETE FROM events WHERE id=2")

br = integrity_break()
check("integrity flags seq gap",
      br == {"seq": 3, "event_id": "c-3", "reason": "seq_gap"}, br)

for dry in (True, False):
    r = client.post("/retention/compact", json={"cutoff_seq": 3, "dry_run": dry})
    check(f"compact(dry_run={dry}) 409 seq_gap",
          r.status_code == 409 and r.json().get("first_break") == br
          and r.json().get("dry_run") is dry, r.text)
check("seq gap: events/anchor unchanged",
      event_count(path) == 2 and anchor(path) == (0, ""),
      (event_count(path), anchor(path)))

r = client.post("/retention/compact", json={"cutoff_seq": 1})
check("archive seq1 across gap boundary",
      r.status_code == 200 and r.json()["deleted"] == 1
      and r.json()["anchor_seq"] == 1, r.text)
check("seq gap still detected after partial archive",
      integrity_break() == {"seq": 3, "event_id": "c-3", "reason": "seq_gap"})

# ---------- D. 断点晚于 cutoff: 按边界归档, 已覆盖区间幂等 ----------
path = use_db("d.db")
seed_events(path, 4, prefix="d")

with raw(path) as c:
    original_payload = c.execute(
        "SELECT payload FROM events WHERE id=4").fetchone()[0]
    c.execute("UPDATE events SET payload=? WHERE id=4", (json.dumps({"v": 404}),))

# 前缀 1..3 干净: 正常归档(视图同步扣减)
r = client.post("/retention/compact", json={"cutoff_seq": 3})
check("archive up to before break", r.status_code == 200
      and r.json()["status"] == "compacted" and r.json()["deleted"] == 3
      and r.json()["anchor_seq"] == 3, r.text)
check("view reflects 3 deletions", client.get("/views/all-d").json()["total"] == 1)
check("break at 4 survives archive",
      integrity_break() == {"seq": 4, "event_id": "d-4", "reason": "hash_mismatch"})

# 幂等: cutoff 落在已覆盖区间
r = client.post("/retention/compact", json={"cutoff_seq": 2, "dry_run": True})
check("idempotent covered cutoff (dry_run)",
      r.status_code == 200 and r.json()["status"] == "dry_run"
      and r.json()["would_delete"] == 0, r.text)
r = client.post("/retention/compact", json={"cutoff_seq": 2, "dry_run": False})
check("idempotent covered cutoff (actual)",
      r.status_code == 200 and r.json()["status"] == "noop"
      and r.json()["deleted"] == 0 and r.json()["anchor_seq"] == 3, r.text)
check("anchor stays after idempotent calls", anchor(path)[0] == 3, anchor(path))

# 修复后归档最后一条: 全链 verified
with raw(path) as c:
    c.execute("UPDATE events SET payload=? WHERE id=4", (original_payload,))
r = client.post("/retention/compact", json={"cutoff_seq": 4})
check("archive last event after repair", r.status_code == 200
      and r.json()["status"] == "compacted" and r.json()["deleted"] == 1
      and r.json()["anchor_seq"] == 4, r.text)
check("fully verified after everything",
      client.get("/integrity").json() == {
          "verified": True, "checked": 0, "first_break": None, "anchor_seq": 4})

# ---------- E. 断点与安全边界同时不满足: 返回链断点 409(优先校验前缀) ----------
path = use_db("e.db")
seed_events(path, 3, prefix="e")  # 游标 3, 安全上界 3

with raw(path) as c:
    c.execute("UPDATE events SET payload=? WHERE id=2", (json.dumps({"v": 9}),))

r = client.post("/retention/compact", json={"cutoff_seq": 5})
check("chain break wins over boundary breach",
      r.status_code == 409 and r.json().get("first_break") ==
      {"seq": 2, "event_id": "e-2", "reason": "hash_mismatch"}, r.text)

# 无断点时越界仍是原有边界 409
path2 = use_db("f.db")
seed_events(path2, 2, prefix="f")  # 游标 2
r = client.post("/retention/compact", json={"cutoff_seq": 5})
check("boundary 409 preserved when chain clean",
      r.status_code == 409 and r.json() == {
          "detail": "cutoff_seq exceeds safe retention boundary",
          "cutoff_seq": 5, "safe_through_seq": 2}, r.text)

print(f"\n{ok} passed, {fail} failed")
raise SystemExit(1 if fail else 0)
