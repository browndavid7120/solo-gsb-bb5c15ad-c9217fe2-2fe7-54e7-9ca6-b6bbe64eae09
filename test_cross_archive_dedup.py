"""跨归档 event_id 幂等判重的端到端验证。"""
import os
import sqlite3
import tempfile

_tmp = tempfile.mkdtemp()
os.environ["DB_PATH"] = os.path.join(_tmp, "test.db")

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402
from app import db  # noqa: E402

# with 触发 startup 事件(init_db 建表/升级), 退出时 shutdown
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


def ev(eid, second, **kw):
    return {"event_id": eid, "occurred_at": f"2026-09-29T09:00:{second:02d}Z",
            "payload": kw or {"v": 1}}


# ---------- 1. 基础幂等(未归档) ----------
r = client.post("/events", json=ev("evt-1", 0, a=1))
check("single first create 201", r.status_code == 201 and r.json()["status"] == "created", r.text)

r = client.post("/events", json=ev("evt-1", 1, a=999))  # 内容不同, 仍是重复 ID
check("single repost 200 duplicate", r.status_code == 200 and r.json() ==
      {"status": "duplicate", "event_id": "evt-1"}, r.text)

r = client.post("/events/batch", json=[
    ev("evt-1", 2), ev("evt-2", 3), ev("evt-1", 4),
])
body = r.json()
check("batch mixed -> 201", r.status_code == 201, r.text)
check("batch counts", body["created"] == 1 and body["duplicate"] == 2, body)
check("batch order preserved with null seq", body["results"] == [
    {"event_id": "evt-1", "status": "duplicate", "seq": None},
    {"event_id": "evt-2", "status": "created", "seq": 2},
    {"event_id": "evt-1", "status": "duplicate", "seq": None},
], body)

r = client.post("/events/batch", json=[ev("evt-1", 5), ev("evt-2", 6)])
body = r.json()
check("batch all duplicate -> 200", r.status_code == 200 and body["created"] == 0
      and body["duplicate"] == 2 and all(x["seq"] is None for x in body["results"]), body)

# ---------- 2. 准备归档: 建 consumer, pull + ack 推进游标 ----------
r = client.post("/consumers/worker/pull", json={"lease_seconds": 60})
token = r.json()["lease_token"]
check("pull got 2 events", len(r.json()["events"]) == 2, r.text)
r = client.post("/consumers/worker/ack", json={"lease_token": token})
check("ack cursor -> 2", r.status_code == 200 and r.json()["cursor_seq"] == 2, r.text)

# 归档前建全量计数视图(回填=2), 用于验证归档扣减与重投不回增
r = client.post("/views", json={"name": "all"})
check("view backfilled 2", r.status_code == 201 and r.json()["total"] == 2, r.text)

r = client.post("/retention/compact", json={"cutoff_seq": 2, "dry_run": True})
check("dry_run would_delete 2", r.json()["would_delete"] == 2, r.text)
r = client.post("/retention/compact", json={"cutoff_seq": 2})
check("compact 2", r.status_code == 200 and r.json()["status"] == "compacted"
      and r.json()["deleted"] == 2 and r.json()["anchor_seq"] == 2, r.text)

# 事件内容已删, 但 ID 占用仍在
conn = sqlite3.connect(os.environ["DB_PATH"])
rows = conn.execute("SELECT event_id, seq FROM received_event_ids ORDER BY seq").fetchall()
check("ID occupancy survives compaction", rows == [("evt-1", 1), ("evt-2", 2)], rows)
n_events = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
check("events content physically deleted", n_events == 0, n_events)
conn.close()

# 视图随归档扣减到 0
check("view decremented to 0 after compact",
      client.get("/views/all").json()["total"] == 0)

# /integrity 锚点仍有效
r = client.get("/integrity").json()
check("integrity ok after full compact", r["verified"] and r["checked"] == 0
      and r["anchor_seq"] == 2, r)

# ---------- 3. 归档后重投: 必须持续判重 ----------
r = client.post("/events", json=ev("evt-1", 7, a=999))
check("POST archived ID -> 200 duplicate (no new seq)",
      r.status_code == 200 and r.json() == {"status": "duplicate", "event_id": "evt-1"}, r.text)

r = client.post("/events/batch", json=[ev("evt-1", 8), ev("evt-3", 9), ev("evt-2", 10)])
body = r.json()
check("batch after archive mixed -> 201", r.status_code == 201, r.text)
check("batch after archive dedup + new seq=3", body["created"] == 1
      and body["duplicate"] == 2 and body["results"] == [
          {"event_id": "evt-1", "status": "duplicate", "seq": None},
          {"event_id": "evt-3", "status": "created", "seq": 3},
          {"event_id": "evt-2", "status": "duplicate", "seq": None},
      ], body)

r = client.post("/events/batch", json=[ev("evt-1", 11), ev("evt-2", 12), ev("evt-3", 13)])
body = r.json()
check("batch all archived/existing dup -> 200", r.status_code == 200
      and body["created"] == 0 and body["duplicate"] == 3, body)

# 重投不回增日计数; 此前批量仅 evt-3 是新 ID, 计数为 1
check("view unaffected by archived duplicates (only evt-3)",
      client.get("/views/all").json()["total"] == 1)
r = client.post("/events", json=ev("evt-4", 14))
check("new ID after archive created seq=4", r.status_code == 201, r.text)
check("view counts only new IDs (evt-3 + evt-4)",
      client.get("/views/all").json()["total"] == 2)

# ---------- 4. 新事件从锚点续链, 完整性通过 ----------
r = client.get("/integrity").json()
check("chain continues from anchor, verified",
      r["verified"] and r["checked"] == 2 and r["anchor_seq"] == 2, r)

# 归档事件的内容接口维持原约定: 列表没有、replay skipped、建关系 404
r = client.get("/events").json()
check("archived ID not in listing", {e["event_id"] for e in r["items"]} == {"evt-3", "evt-4"}, r)
r = client.post("/replay", json={"event_ids": ["evt-1", "evt-3"]}).json()
statuses = {x["event_id"]: x["status"] for x in r["results"]}
check("replay: archived skipped, new replayed",
      statuses == {"evt-1": "skipped", "evt-3": "replayed"}, statuses)
r = client.put("/relations/evt-3/parent", json={"parent_id": "evt-1"})
check("cannot attach relation to archived parent -> 404", r.status_code == 404, r.text)

# ---------- 5. 因果关系: 归档后按 ID 不指向新记录 ----------
r = client.post("/events", json=ev("evt-p", 15))
r = client.post("/events", json=ev("evt-c", 16))
r = client.put("/relations/evt-c/parent", json={"parent_id": "evt-p"})
check("set parent evt-c -> evt-p", r.status_code == 200, r.text)
# ack 到 6 后整体归档
client.post("/consumers/worker/pull", json={"lease_seconds": 60})
tok = client.post("/consumers/worker/pull", json={"lease_seconds": 60}).json()["lease_token"]
client.post("/consumers/worker/ack", json={"lease_token": tok})
r = client.post("/retention/compact", json={"cutoff_seq": 6})
check("compact through 6", r.status_code == 200 and r.json()["anchor_seq"] == 6, r.text)
# 重投父 ID: 判重, 不会冒出一条同 ID 新记录让关系指向它
r = client.post("/events", json=ev("evt-p", 17, other=True))
check("repost archived parent -> duplicate", r.status_code == 200, r.text)
r = client.get("/relations/evt-c/ancestors").json()
check("ancestor link still evt-p by stable ID",
      r["count"] == 1 and r["ancestors"][0]["event_id"] == "evt-p", r)

# ---------- 6. 升级回填: 旧库(无占用表)启动后覆盖当前未归档事件 ----------
old_path = os.path.join(_tmp, "old.db")
oc = sqlite3.connect(old_path)
# 模拟旧版 schema: 只有 events(含链列), 没有 received_event_ids
oc.execute("""CREATE TABLE events (
    id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE,
    occurred_at TEXT NOT NULL, payload TEXT NOT NULL, received_at TEXT NOT NULL,
    prev_hash TEXT, hash TEXT)""")
for i, eid in enumerate(("old-1", "old-2", "old-3"), start=1):
    oc.execute("INSERT INTO events (id,event_id,occurred_at,payload,received_at,prev_hash,hash)"
               " VALUES (?,?,?,?,?,?,?)",
               (i, eid, "2026-09-29T08:00:00Z", '{}', "2026-09-29T08:00:00Z", "x", "y"))
# 模拟旧版已归档删除且无留存 ID 的历史记录(不要求追溯)
oc.execute("DELETE FROM events WHERE event_id = 'old-1'")
oc.commit()
oc.close()

db.DB_PATH = old_path
db.init_db()
oc = sqlite3.connect(old_path)
backfilled = {r[0] for r in oc.execute("SELECT event_id FROM received_event_ids")}
check("migration backfills current (non-archived) IDs",
      backfilled == {"old-2", "old-3"}, backfilled)
oc.close()
db.DB_PATH = os.environ["DB_PATH"]  # 还原

# ---------- 7. 启动可用性 + 健康检查 ----------
r = client.get("/health")
check("health ok", r.status_code == 200 and r.json() == {"status": "ok"}, r.text)

print(f"\n{ok} passed, {fail} failed")
raise SystemExit(1 if fail else 0)
