"""消费流程序号缺口防护的端到端验证。

事件表中间记录意外缺失(库外 DELETE, /integrity 已报 seq_gap)时, 修复目标:
- 首次拉取、未过期租约重拉、过期续租返回批次前都核对拟返回批次从起点到
  末尾的序号连续性; 有缺口一律 409 且 first_break 与 /integrity 一致,
  游标与租约不变(首次不建租约, 过期不换发新令牌);
- ack 与 ack-prefix 推进前核对租约区间, 缺口仍在时拒绝推进(含 through_seq
  落在缺口之前的前缀), 游标/租约不变;
- 缺口修复(记录以原 seq 恢复)后: 首次正常建租, 未过期重拉沿用原 token 与
  过期时间, 过期续租换发新 token, 确认照常推进;
- 无缺口时原有批次、过期换令牌、前缀确认约定不变; 归档锚点仍是可投递起点。
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


def seed(path, n, prefix="gap"):
    """经正常 API 收 n 条事件, 返回各自 seq(从 1 起连续)。"""
    seqs = []
    for i in range(1, n + 1):
        r = client.post("/events", json={
            "event_id": f"{prefix}-{i}",
            "occurred_at": f"2026-09-29T10:{i:02d}:00Z",
            "payload": {"v": i},
        })
        assert r.status_code == 201, r.text
    with raw(path) as c:
        rows = c.execute(
            "SELECT id FROM events WHERE event_id LIKE ? ORDER BY id",
            (f"{prefix}-%",),
        ).fetchall()
        seqs = [r[0] for r in rows]
    assert seqs == list(range(seqs[0], seqs[0] + n)), seqs
    return seqs


def consumer_state(path, consumer):
    with raw(path) as c:
        r = c.execute(
            "SELECT cursor_seq, lease_token, lease_from_seq, lease_to_seq,"
            " lease_expires_at FROM consumers WHERE consumer_id=?",
            (consumer,),
        ).fetchone()
        return dict(r) if r is not None else None


def snapshot_event(path, seq):
    with raw(path) as c:
        r = c.execute("SELECT * FROM events WHERE id=?", (seq,)).fetchone()
        return dict(r)


def delete_event(path, seq):
    with raw(path) as c:
        c.execute("DELETE FROM events WHERE id=?", (seq,))


def restore_event(path, row):
    with raw(path) as c:
        c.execute(
            "INSERT INTO events (id, event_id, occurred_at, payload, received_at,"
            " prev_hash, hash) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (row["id"], row["event_id"], row["occurred_at"], row["payload"],
             row["received_at"], row["prev_hash"], row["hash"]),
        )


def integrity_break():
    r = client.get("/integrity")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["verified"] is False, body
    return body["first_break"]


def expire_lease(path, consumer):
    """把租约过期时间直接改到过去(模拟租约过期), 返回改前状态。"""
    before = consumer_state(path, consumer)
    with raw(path) as c:
        c.execute(
            "UPDATE consumers SET lease_expires_at='2000-01-01T00:00:00+00:00'"
            " WHERE consumer_id=?",
            (consumer,),
        )
    return before


PULL = {"limit": 100, "lease_seconds": 300}

# ---------- A. 首次拉取遇缺口: 409/first_break 与 /integrity 一致, 不建租约不发令牌 ----------
path = use_db("a.db")
seqs = seed(path, 5)
s1, s2, s3 = seqs[0], seqs[1], seqs[2]
row_s2 = snapshot_event(path, s2)
delete_event(path, s2)

br = integrity_break()
check("A integrity reports seq_gap at first event after gap",
      br == {"seq": s3, "event_id": "gap-3", "reason": "seq_gap"}, br)

r = client.post("/consumers/c-a/pull", json=PULL)
body = r.json()
check("A fresh pull across gap is 409", r.status_code == 409, r.text)
check("A fresh pull first_break mirrors /integrity",
      body.get("first_break") == br
      and body.get("detail") == "pull batch is not seq-continuous"
      and body.get("cursor_seq") == 0 and body.get("anchor_seq") == 0
      and body.get("batch_from_seq") == s1 and body.get("batch_to_seq") == seqs[-1],
      body)

st = consumer_state(path, "c-a")
check("A no lease created and cursor untouched",
      st["cursor_seq"] == 0 and st["lease_token"] is None
      and st["lease_from_seq"] is None and st["lease_to_seq"] is None
      and st["lease_expires_at"] is None, st)

# 无租约: ack / ack-prefix 仍是原有的 no active lease 409
r = client.post("/consumers/c-a/ack", json={"lease_token": "anything"})
check("A ack with no lease still 409 no active lease",
      r.status_code == 409 and r.json()["detail"] == "no active lease", r.text)
r = client.post("/consumers/c-a/ack-prefix",
                json={"lease_token": "anything", "through_seq": s1})
check("A ack-prefix with no lease still 409 no active lease",
      r.status_code == 409 and r.json()["detail"] == "no active lease", r.text)

# 缺口之前的干净单条仍可按边界正常租用/确认(消费停在缺口边缘, 不越过缺口)
r = client.post("/consumers/c-a/pull", json={"limit": 1, "lease_seconds": 300})
check("A edge-only batch before gap can be leased",
      r.status_code == 200 and [e["seq"] for e in r.json()["events"]] == [s1], r.text)
edge_token = r.json()["lease_token"]
r = client.post("/consumers/c-a/ack", json={"lease_token": edge_token})
check("A edge batch can be acknowledged",
      r.status_code == 200 and r.json()["cursor_seq"] == s1, r.text)
r = client.post("/consumers/c-a/pull", json=PULL)
check("A pull stops again at the same gap",
      r.status_code == 409 and r.json().get("first_break") == br, r.text)

# 缺口修复: 同请求正常建租, 返回缺口之后的完整连续批次
restore_event(path, row_s2)
check("A integrity verified after restore", client.get("/integrity").json()["verified"],
      client.get("/integrity").text)
r = client.post("/consumers/c-a/pull", json=PULL)
check("A fresh pull after repair leases continuous tail",
      r.status_code == 200 and [e["seq"] for e in r.json()["events"]] == seqs[1:],
      r.text)
token_a = r.json()["lease_token"]
r = client.post("/consumers/c-a/ack", json={"lease_token": token_a})
check("A ack after repair advances cursor over restored records",
      r.status_code == 200 and r.json()["cursor_seq"] == seqs[-1], r.text)

# ---------- B. 未过期租约重拉遇缺口: 409 且 token/过期时间/游标不变, ack 被拒 ----------
path = use_db("b.db")
seqs = seed(path, 4, prefix="b")
r = client.post("/consumers/c-b/pull", json=PULL)
check("B initial pull leases full batch",
      r.status_code == 200 and [e["seq"] for e in r.json()["events"]] == seqs, r.text)
token1 = r.json()["lease_token"]
expires1 = r.json()["lease_expires_at"]
st_before = consumer_state(path, "c-b")

row_b2 = snapshot_event(path, seqs[1])
delete_event(path, seqs[1])
br = integrity_break()

r = client.post("/consumers/c-b/pull", json=PULL)
check("B active re-pull across gap is 409", r.status_code == 409, r.text)
check("B re-pull first_break mirrors /integrity",
      r.json().get("first_break") == br
      and r.json().get("detail") == "leased batch is not seq-continuous"
      and r.json().get("lease_from_seq") == seqs[0]
      and r.json().get("lease_to_seq") == seqs[-1], r.json())
st = consumer_state(path, "c-b")
check("B lease token/expiry/range and cursor unchanged on re-pull",
      st == st_before, (st, st_before))

# ack 与 ack-prefix(含落在缺口之前的前缀)都拒绝推进
r = client.post("/consumers/c-b/ack", json={"lease_token": token1})
check("B ack across gap rejected",
      r.status_code == 409 and r.json().get("first_break") == br, r.text)
r = client.post("/consumers/c-b/ack-prefix",
                json={"lease_token": token1, "through_seq": seqs[0]})
check("B ack-prefix before gap also rejected",
      r.status_code == 409 and r.json().get("first_break") == br, r.text)
check("B cursor/lease unchanged after rejected acks",
      consumer_state(path, "c-b") == st_before, consumer_state(path, "c-b"))

# 修复: 未过期重拉沿用原 token 与过期时间, 原样返回完整批次
restore_event(path, row_b2)
r = client.post("/consumers/c-b/pull", json=PULL)
check("B active re-pull after repair returns same token and expiry",
      r.status_code == 200 and r.json()["lease_token"] == token1
      and r.json()["lease_expires_at"] == expires1
      and [e["seq"] for e in r.json()["events"]] == seqs, r.text)
r = client.post("/consumers/c-b/ack", json={"lease_token": token1})
check("B ack after repair advances cursor",
      r.status_code == 200 and r.json()["cursor_seq"] == seqs[-1], r.text)

# ---------- C. 过期续租遇缺口: 不换发新令牌; 修复后才换发, 旧令牌失效 ----------
path = use_db("c.db")
seqs = seed(path, 3, prefix="c")
r = client.post("/consumers/c-c/pull", json={"limit": 100, "lease_seconds": 300})
old_token = r.json()["lease_token"]
st_before = consumer_state(path, "c-c")

row_c2 = snapshot_event(path, seqs[1])
delete_event(path, seqs[1])
br = integrity_break()
expire_lease(path, "c-c")

r = client.post("/consumers/c-c/pull", json={"limit": 100, "lease_seconds": 300})
check("C expired re-lease across gap is 409", r.status_code == 409, r.text)
check("C re-lease first_break mirrors /integrity",
      r.json().get("first_break") == br, r.json())
st = consumer_state(path, "c-c")
check("C no new token on gap; old lease row and cursor unchanged",
      st["lease_token"] == old_token and st["cursor_seq"] == 0
      and st["lease_from_seq"] == seqs[0] and st["lease_to_seq"] == seqs[-1]
      and st["lease_expires_at"] == "2000-01-01T00:00:00+00:00",
      st)
# 过期 + 缺口: 旧 token 仍按原有的 lease expired 拒绝(优先于缺口判定)
r = client.post("/consumers/c-c/ack", json={"lease_token": old_token})
check("C expired ack still reports lease expired",
      r.status_code == 409 and r.json()["detail"] == "lease expired", r.text)

# 修复: 续租才换发新 token 与新过期时间, 区间沿用原批次
restore_event(path, row_c2)
r = client.post("/consumers/c-c/pull", json={"limit": 100, "lease_seconds": 300})
new_token = r.json()["lease_token"]
check("C re-lease after repair issues new token and full batch",
      r.status_code == 200 and new_token != old_token
      and [e["seq"] for e in r.json()["events"]] == seqs
      and r.json()["lease_expires_at"] > "2001-01-01", r.text)
r = client.post("/consumers/c-c/ack", json={"lease_token": old_token})
check("C old token invalid after re-lease",
      r.status_code == 409 and r.json()["detail"] == "invalid lease token", r.text)
r = client.post("/consumers/c-c/ack", json={"lease_token": new_token})
check("C new token acks full batch",
      r.status_code == 200 and r.json()["cursor_seq"] == seqs[-1], r.text)

# ---------- D. 租约期间出现缺口: 前缀确认被拒; 修复后前缀确认与重租约定不变 ----------
path = use_db("d.db")
seqs = seed(path, 5, prefix="d")
r = client.post("/consumers/c-d/pull", json=PULL)
token_d = r.json()["lease_token"]
st_before = consumer_state(path, "c-d")

row_d4 = snapshot_event(path, seqs[3])
delete_event(path, seqs[3])
br = integrity_break()

r = client.post("/consumers/c-d/ack-prefix",
                json={"lease_token": token_d, "through_seq": seqs[1]})
check("D ack-prefix rejected while gap remains in lease range",
      r.status_code == 409 and r.json().get("first_break") == br
      and r.json().get("through_seq") == seqs[1], r.text)
r = client.post("/consumers/c-d/ack", json={"lease_token": token_d})
check("D full ack rejected while gap remains",
      r.status_code == 409 and r.json().get("first_break") == br, r.text)
check("D cursor/lease unchanged after rejected confirms",
      consumer_state(path, "c-d") == st_before, consumer_state(path, "c-d"))

restore_event(path, row_d4)
r = client.post("/consumers/c-d/ack-prefix",
                json={"lease_token": token_d, "through_seq": seqs[2]})
check("D prefix ack after repair advances and clears lease",
      r.status_code == 200 and r.json() == {
          "consumer_id": "c-d", "acknowledged": True,
          "through_seq": seqs[2], "cursor_seq": seqs[2]}, r.text)
r = client.post("/consumers/c-d/pull", json=PULL)
check("D re-pull leases remaining tail with new token",
      r.status_code == 200 and r.json()["lease_token"] != token_d
      and [e["seq"] for e in r.json()["events"]] == seqs[3:], r.text)
r = client.post("/consumers/c-d/ack", json={"lease_token": r.json()["lease_token"]})
check("D tail ack reaches end",
      r.status_code == 200 and r.json()["cursor_seq"] == seqs[-1], r.text)
# 旧 token 立即作废(原有约定)
r = client.post("/consumers/c-d/ack", json={"lease_token": token_d})
check("D old token rejected after prefix ack",
      r.status_code == 409 and r.json()["detail"] == "no active lease", r.text)

# ---------- E. 游标之后的缺口 + 归档锚点作为可投递起点 ----------
path = use_db("e.db")
seqs = seed(path, 3, prefix="e")
# 另一个 consumer 全量确认并归档, 制造非零链锚点
r = client.post("/consumers/keeper/pull", json={"limit": 100, "lease_seconds": 300})
tk = r.json()["lease_token"]
client.post("/consumers/keeper/ack", json={"lease_token": tk})
r = client.post("/retention/compact", json={"cutoff_seq": seqs[-1]})
assert r.status_code == 200, r.text
with raw(path) as c:
    anchor = c.execute("SELECT anchor_seq FROM chain_anchor WHERE id=1").fetchone()[0]
check("E setup: anchor at last archived seq", anchor == seqs[-1], anchor)

# 新 consumer: 已归档区间不可投递, 空队列仍是空批次(不建租约)
r = client.post("/consumers/c-e/pull", json=PULL)
check("E archived events are not deliverable; empty batch stays",
      r.status_code == 200 and r.json()["events"] == []
      and r.json()["lease_token"] is None, r.text)

# 锚点之后新写入 seq+1, seq+2
for i in (4, 5):
    rr = client.post("/events", json={
        "event_id": f"e-{i}",
        "occurred_at": f"2026-09-29T11:0{i}:00Z",
        "payload": {"v": i},
    })
    assert rr.status_code == 201, rr.text
with raw(path) as c:
    new_seqs = [r[0] for r in c.execute(
        "SELECT id FROM events WHERE event_id LIKE 'e-%' ORDER BY id").fetchall()]
r = client.post("/consumers/c-e/pull", json=PULL)
check("E delivery starts after anchor; clean tail leases normally",
      r.status_code == 200 and [e["seq"] for e in r.json()["events"]] == new_seqs,
      r.text)
tk = r.json()["lease_token"]
client.post("/consumers/c-e/ack", json={"lease_token": tk})

# 锚点之后再制造缺口: first_break 仍与 /integrity 一致, 响应附非零 anchor
for i in (6, 7):
    rr = client.post("/events", json={
        "event_id": f"e-{i}",
        "occurred_at": f"2026-09-29T12:0{i}:00Z",
        "payload": {"v": i},
    })
    assert rr.status_code == 201, rr.text
with raw(path) as c:
    gap_pair = [r[0] for r in c.execute(
        "SELECT id FROM events WHERE event_id IN ('e-6','e-7') ORDER BY id").fetchall()]
row_gap = snapshot_event(path, gap_pair[0])
delete_event(path, gap_pair[0])
br = integrity_break()
r = client.post("/consumers/c-e/pull", json=PULL)
check("E post-anchor gap: 409 with same first_break and anchor",
      r.status_code == 409 and r.json().get("first_break") == br
      and r.json().get("anchor_seq") == anchor, r.json())
restore_event(path, row_gap)
r = client.post("/consumers/c-e/pull", json=PULL)
check("E post-anchor gap repaired: normal lease",
      r.status_code == 200
      and [e["seq"] for e in r.json()["events"]] == gap_pair, r.text)

# ---------- F. 无缺口时原有约定不变: 批次/重复拉取/过期换令牌/前缀确认 ----------
path = use_db("f.db")
seqs = seed(path, 4, prefix="f")
r1 = client.post("/consumers/c-f/pull", json={"limit": 4, "lease_seconds": 300})
b1 = r1.json()
check("F first pull batch and fields unchanged",
      r1.status_code == 200 and [e["seq"] for e in b1["events"]] == seqs
      and b1["lease_token"] and b1["lease_expires_at"], r1.text)
r2 = client.post("/consumers/c-f/pull", json={"limit": 1, "lease_seconds": 1})
b2 = r2.json()
check("F active re-pull is identical batch/token/expiry",
      r2.status_code == 200 and b2["lease_token"] == b1["lease_token"]
      and b2["lease_expires_at"] == b1["lease_expires_at"]
      and [e["seq"] for e in b2["events"]] == seqs, r2.text)

expire_lease(path, "c-f")
r3 = client.post("/consumers/c-f/pull", json={"limit": 4, "lease_seconds": 300})
b3 = r3.json()
check("F expired re-lease keeps range and changes token",
      r3.status_code == 200 and b3["lease_token"] != b1["lease_token"]
      and [e["seq"] for e in b3["events"]] == seqs, r3.text)

# 越界响应约定不变
r = client.post("/consumers/c-f/ack-prefix",
                json={"lease_token": b3["lease_token"], "through_seq": seqs[-1] + 5})
check("F beyond-end 409 shape unchanged",
      r.status_code == 409 and r.json() == {
          "detail": "through_seq beyond batch end",
          "through_seq": seqs[-1] + 5,
          "lease_from_seq": seqs[0], "lease_to_seq": seqs[-1]}, r.text)
r = client.post("/consumers/c-f/ack-prefix",
                json={"lease_token": b3["lease_token"], "through_seq": seqs[1]})
check("F prefix ack unchanged",
      r.status_code == 200 and r.json()["cursor_seq"] == seqs[1], r.text)
r = client.post("/consumers/c-f/pull", json=PULL)
check("F next pull starts after prefix",
      r.status_code == 200 and [e["seq"] for e in r.json()["events"]] == seqs[2:],
      r.text)
r = client.post("/consumers/c-f/ack", json={"lease_token": r.json()["lease_token"]})
check("F final ack", r.status_code == 200 and r.json()["cursor_seq"] == seqs[-1], r.text)
r = client.post("/consumers/c-f/pull", json=PULL)
check("F drained queue returns empty batch",
      r.status_code == 200 and r.json()["events"] == []
      and r.json()["lease_token"] is None, r.text)

# ---------- G. 租约批次末尾记录缺失(其后无现存记录): 同样拦截, event_id 为 None ----------
path = use_db("g.db")
seqs = seed(path, 3, prefix="g")
r = client.post("/consumers/c-g/pull", json=PULL)
token_g = r.json()["lease_token"]
st_before = consumer_state(path, "c-g")

row_g3 = snapshot_event(path, seqs[-1])
delete_event(path, seqs[-1])
# /integrity 口径下末尾缺失之后没有现存记录, 不会报 first_break(锚点不前进);
# 但已承诺的租约区间不再完整, 消费流程必须在租约边界上检出并拒绝推进
tail_break = {"seq": seqs[-1], "event_id": None, "reason": "seq_gap"}

r = client.post("/consumers/c-g/pull", json=PULL)
check("G active re-pull flags missing batch tail",
      r.status_code == 409 and r.json().get("first_break") == tail_break, r.text)
r = client.post("/consumers/c-g/ack", json={"lease_token": token_g})
check("G ack across missing tail rejected",
      r.status_code == 409 and r.json().get("first_break") == tail_break, r.text)
r = client.post("/consumers/c-g/ack-prefix",
                json={"lease_token": token_g, "through_seq": seqs[0]})
check("G ack-prefix rejected while batch tail missing",
      r.status_code == 409 and r.json().get("first_break") == tail_break, r.text)
check("G state unchanged while tail missing",
      consumer_state(path, "c-g") == st_before, consumer_state(path, "c-g"))

restore_event(path, row_g3)
r = client.post("/consumers/c-g/ack", json={"lease_token": token_g})
check("G ack succeeds after tail restored",
      r.status_code == 200 and r.json()["cursor_seq"] == seqs[-1], r.text)

print(f"\n{ok} passed, {fail} failed")
raise SystemExit(1 if fail else 0)
