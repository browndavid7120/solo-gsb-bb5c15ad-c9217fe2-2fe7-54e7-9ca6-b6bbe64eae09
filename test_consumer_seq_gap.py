"""消费流程序号缺口防护修复的端到端验证。

背景: 事件表中间记录意外缺失时, /integrity 已能报告 seq_gap, 但旧的消费
拉取用 LIMIT 直接取行, 会把缺口两侧事件合为一批; ack 后游标越过缺口,
记录即使恢复也不再投递。

覆盖:
- 首次拉取: 拟返回批次含缺口 -> 409 + 与 /integrity 一致的 first_break,
  不建租约/不发令牌/游标不变; 缺口在批次之后时先正常投递连续前缀,
  游标到缺口前停住, 绝不会越过缺口;
- 未过期租约重拉: 租约区间内出现缺口 -> 409, 原批次/token/过期时间/游标
  全部保持原状(不续租); 恢复后重拉原样返回, 同一 token 可正常 ack;
- 过期续租: 旧租约区间有缺口 -> 409 且不换发新令牌、不重置过期时间;
  恢复后才按原区间换发新 token(沿用 lease_seconds)续租, ack 成功;
- ack / ack-prefix: 推进前核对租约区间, 缺口仍在时拒绝推进(409,
  first_break 与 /integrity 一致), 游标/租约/令牌不变; 缺口修复后沿用
  原租约时效与令牌成功确认;
- 批次末尾缺失(/integrity 因无后续记录而不可见): 消费侧仍以首个缺失 seq
  返回 seq_gap 409, 拒绝越过;
- 无缺口时原有批次内容、未过期重拉同 token、过期换新 token、前缀确认、
  空批次(null token)等约定保持不变;
- 归档锚点仍是可投递起点: 压缩后新 consumer 从 anchor_seq+1 拉取,
  锚点之后紧邻的缺口同样 409。
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


def post_events(path, n, prefix="evt"):
    """经正常 API 收 n 条连续事件。"""
    for i in range(1, n + 1):
        r = client.post("/events", json={
            "event_id": f"{prefix}-{i}",
            "occurred_at": f"2026-09-29T10:{i:02d}:00Z",
            "payload": {"v": i},
        })
        assert r.status_code == 201, r.text


def delete_event(path, seq):
    """库外物理删除一条事件(模拟中间记录意外缺失), 返回该行完整内容供恢复。"""
    with raw(path) as c:
        row = c.execute(
            "SELECT id, event_id, occurred_at, payload, received_at, prev_hash, hash"
            " FROM events WHERE id=?",
            (seq,),
        ).fetchone()
        assert row is not None, seq
        c.execute("DELETE FROM events WHERE id=?", (seq,))
        return dict(row)


def restore_event(path, row):
    """按原 seq 完整恢复被删除的事件(模拟记录恢复)。"""
    with raw(path) as c:
        c.execute(
            "INSERT INTO events (id, event_id, occurred_at, payload, received_at,"
            " prev_hash, hash) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (row["id"], row["event_id"], row["occurred_at"], row["payload"],
             row["received_at"], row["prev_hash"], row["hash"]),
        )


def lease_row(path, consumer="worker"):
    with raw(path) as c:
        return c.execute(
            "SELECT cursor_seq, lease_token, lease_from_seq, lease_to_seq,"
            " lease_expires_at FROM consumers WHERE consumer_id=?",
            (consumer,),
        ).fetchone()


def expire_lease(path, consumer="worker"):
    """把租约过期时间直接改到过去(模拟租约过期, 无需等待)。"""
    with raw(path) as c:
        c.execute(
            "UPDATE consumers SET lease_expires_at='2000-01-01T00:00:00+00:00'"
            " WHERE consumer_id=?",
            (consumer,),
        )


def integrity_break():
    r = client.get("/integrity")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["verified"] is False, body
    return body["first_break"]


# ---------- A. 首次拉取遇缺口: 409 + first_break, 不建租约/不发令牌 ----------
path = use_db("a.db")
post_events(path, 5)
deleted = delete_event(path, 2)

br = integrity_break()
check("integrity reports gap at seq 3",
      br == {"seq": 3, "event_id": "evt-3", "reason": "seq_gap"}, br)

r = client.post("/consumers/w1/pull", json={"limit": 100, "lease_seconds": 60})
body = r.json()
check("first pull across gap -> 409", r.status_code == 409, r.text)
check("first pull first_break mirrors /integrity",
      body.get("first_break") == br
      and body.get("detail") == "seq gap detected while leasing next batch"
      and body.get("lease_from_seq") == 1 and body.get("lease_to_seq") == 5,
      body)
check("409 carries no lease token", "lease_token" not in body, body)

lease = lease_row(path, "w1")
check("no lease created and cursor unchanged",
      lease["cursor_seq"] == 0 and lease["lease_token"] is None
      and lease["lease_from_seq"] is None and lease["lease_to_seq"] is None,
      tuple(lease))

# 缺口未修复时反复拉取仍是 409, 始终不发令牌
r2 = client.post("/consumers/w1/pull", json={"limit": 100, "lease_seconds": 60})
check("repeated pull stays 409", r2.status_code == 409
      and r2.json()["first_break"] == br, r2.text)
check("still no lease after repeated 409",
      lease_row(path, "w1")["lease_token"] is None)

# 恢复后首次拉取正常: 完整批次 + 新令牌, ack 可推进到末尾
restore_event(path, deleted)
r = client.post("/consumers/w1/pull", json={"limit": 100, "lease_seconds": 60})
check("pull after repair returns full batch",
      r.status_code == 200 and [e["seq"] for e in r.json()["events"]] == [1, 2, 3, 4, 5]
      and r.json()["lease_token"], r.text)
token = r.json()["lease_token"]
r = client.post("/consumers/w1/ack", json={"lease_token": token})
check("ack after repair advances to 5",
      r.status_code == 200 and r.json()["cursor_seq"] == 5, r.text)
# 恢复的 seq=2 已在上述完整批次中投递并确认, 没有丢事件; 再用重放验证其存在
r = client.post("/replay", json={"event_ids": ["evt-2"]})
check("recovered evt-2 replayable",
      r.json()["replayed"] == 1 and r.json()["results"][0]["event"]["seq"] == 2, r.text)

# ---------- B. 缺口在 LIMIT 批次之后: 先投递连续前缀, 到缺口前停住 ----------
path = use_db("b.db")
post_events(path, 10)
delete_event(path, 5)

r = client.post("/consumers/w2/pull", json={"limit": 3, "lease_seconds": 60})
check("batch before gap leased normally",
      r.status_code == 200 and [e["seq"] for e in r.json()["events"]] == [1, 2, 3]
      and r.json()["lease_token"], r.text)
token = r.json()["lease_token"]
r = client.post("/consumers/w2/ack", json={"lease_token": token})
check("clean prefix acked to 3", r.status_code == 200
      and r.json()["cursor_seq"] == 3, r.text)
# 下一批 LIMIT 3 取到 4,6,7(缺口两侧被合成一批) -> 409, 游标停在 3
r = client.post("/consumers/w2/pull", json={"limit": 3, "lease_seconds": 60})
check("next pull hits gap and refuses to merge",
      r.status_code == 409
      and r.json()["first_break"] == {"seq": 6, "event_id": "evt-6", "reason": "seq_gap"}
      and r.json()["lease_from_seq"] == 4 and r.json()["lease_to_seq"] == 7, r.text)
check("cursor never jumps over gap", lease_row(path, "w2")["cursor_seq"] == 3)

# ---------- C. 未过期租约期间出现缺口: 重拉 409, 租约/令牌/时效不变 ----------
path = use_db("c.db")
post_events(path, 5)
r = client.post("/consumers/w3/pull", json={"limit": 100, "lease_seconds": 300})
check("initial pull ok", r.status_code == 200 and len(r.json()["events"]) == 5, r.text)
orig_token = r.json()["lease_token"]
orig_expires = r.json()["lease_expires_at"]

deleted = delete_event(path, 3)
br = integrity_break()
check("integrity gap at 4", br == {"seq": 4, "event_id": "evt-4", "reason": "seq_gap"}, br)

# 未过期重拉: 409, 不返回批次、不续租
r = client.post("/consumers/w3/pull", json={"limit": 100, "lease_seconds": 300})
check("active lease re-pull with gap -> 409",
      r.status_code == 409 and r.json()["first_break"] == br, r.text)
lease = lease_row(path, "w3")
check("active lease untouched on 409 (token/expiry/cursor)",
      lease["lease_token"] == orig_token
      and lease["lease_expires_at"] == orig_expires
      and lease["lease_from_seq"] == 1 and lease["lease_to_seq"] == 5
      and lease["cursor_seq"] == 0, tuple(lease))

# 缺口期间 ack / ack-prefix 同样被拒(令牌仍有效, 但禁止越过缺口)
r = client.post("/consumers/w3/ack", json={"lease_token": orig_token})
check("ack refused while gap open", r.status_code == 409
      and r.json()["first_break"] == br, r.text)
r = client.post("/consumers/w3/ack-prefix",
                json={"lease_token": orig_token, "through_seq": 2})
check("ack-prefix refused while gap open", r.status_code == 409
      and r.json()["first_break"] == br, r.text)
check("cursor still 0 after refused confirms",
      lease_row(path, "w3")["cursor_seq"] == 0)

# 恢复后: 未过期重拉沿用原 token/过期时间原样返回, ack 成功(原租约时效不变)
restore_event(path, deleted)
r = client.post("/consumers/w3/pull", json={"limit": 100, "lease_seconds": 300})
check("re-pull after repair returns same lease",
      r.status_code == 200 and [e["seq"] for e in r.json()["events"]] == [1, 2, 3, 4, 5]
      and r.json()["lease_token"] == orig_token
      and r.json()["lease_expires_at"] == orig_expires, r.text)
r = client.post("/consumers/w3/ack", json={"lease_token": orig_token})
check("same original token acks after repair",
      r.status_code == 200 and r.json()["cursor_seq"] == 5, r.text)

# ---------- D. 过期续租遇缺口: 409 不换令牌; 修复后才换发新 token ----------
path = use_db("d.db")
post_events(path, 5)
r = client.post("/consumers/w4/pull", json={"limit": 100, "lease_seconds": 300})
old_token = r.json()["lease_token"]
old_expires = r.json()["lease_expires_at"]
expire_lease(path, "w4")

deleted = delete_event(path, 3)
# 过期续租: 409, 且数据库中的 token / 过期时间原样保留(不换发、不重置)
r = client.post("/consumers/w4/pull", json={"limit": 100, "lease_seconds": 120})
check("expired renewal with gap -> 409", r.status_code == 409
      and r.json()["first_break"] ==
      {"seq": 4, "event_id": "evt-4", "reason": "seq_gap"}, r.text)
lease = lease_row(path, "w4")
check("no new token issued on expired-lease 409",
      lease["lease_token"] == old_token
      and lease["lease_expires_at"] == "2000-01-01T00:00:00+00:00"
      and lease["cursor_seq"] == 0, tuple(lease))

# 缺口修复后过期续租: 同一区间换发新 token, 新时效按本次 lease_seconds 计
restore_event(path, deleted)
r = client.post("/consumers/w4/pull", json={"limit": 100, "lease_seconds": 120})
check("renewal after repair issues new token for same range",
      r.status_code == 200 and [e["seq"] for e in r.json()["events"]] == [1, 2, 3, 4, 5]
      and r.json()["lease_token"] != old_token and r.json()["lease_token"], r.text)
new_token = r.json()["lease_token"]
# 旧 token 已失效, 新 token 可确认
r = client.post("/consumers/w4/ack", json={"lease_token": old_token})
check("old token invalid after renewal", r.status_code == 409, r.text)
r = client.post("/consumers/w4/ack", json={"lease_token": new_token})
check("new token acks renewed batch",
      r.status_code == 200 and r.json()["cursor_seq"] == 5, r.text)

# ---------- E. 批次末尾缺失: /integrity 不可见, 消费侧仍拒绝越过 ----------
path = use_db("e.db")
post_events(path, 5)
r = client.post("/consumers/w5/pull", json={"limit": 100, "lease_seconds": 300})
token = r.json()["lease_token"]
delete_event(path, 5)

# /integrity: 现存 1..4 连续, 没有"缺口后的现存记录", 故 verified=true
integ = client.get("/integrity").json()
check("integrity cannot see dangling tail gap",
      integ["verified"] is True and integ["checked"] == 4, integ)

# 消费侧: 租约区间 [1,5] 最后一条缺失, 仍按首个缺失 seq 报 seq_gap
r = client.post("/consumers/w5/pull", json={"limit": 100, "lease_seconds": 300})
check("active re-pull flags missing tail",
      r.status_code == 409 and r.json()["first_break"] ==
      {"seq": 5, "event_id": "evt-5", "reason": "seq_gap"}
      and r.json()["lease_to_seq"] == 5, r.text)
r = client.post("/consumers/w5/ack", json={"lease_token": token})
check("ack refuses to jump over missing tail", r.status_code == 409
      and r.json()["first_break"]["seq"] == 5, r.text)
r = client.post("/consumers/w5/ack-prefix",
                json={"lease_token": token, "through_seq": 4})
check("ack-prefix refuses while tail of lease missing", r.status_code == 409
      and r.json()["first_break"]["seq"] == 5, r.text)
check("cursor stays 0 with missing tail", lease_row(path, "w5")["cursor_seq"] == 0)

# ---------- F. ack-prefix 缺口在批次后半: 仍拒绝(核对整个租约区间) ----------
path = use_db("f.db")
post_events(path, 5)
r = client.post("/consumers/w6/pull", json={"limit": 100, "lease_seconds": 300})
token = r.json()["lease_token"]
delete_event(path, 4)
r = client.post("/consumers/w6/ack-prefix",
                json={"lease_token": token, "through_seq": 2})
check("prefix ack refused even gap beyond through_seq",
      r.status_code == 409 and r.json()["first_break"] ==
      {"seq": 5, "event_id": "evt-5", "reason": "seq_gap"}, r.text)

# 恢复后同一 token 正常前缀确认, 租约清除、游标到 2
# 重新删除再恢复不太合适, 直接用新库: 上面已验证过"修复后沿用原租约"的 C/D 场景
path = use_db("g.db")
post_events(path, 5)
r = client.post("/consumers/g/pull", json={"limit": 100, "lease_seconds": 300})
token = r.json()["lease_token"]
saved = delete_event(path, 4)
r = client.post("/consumers/g/ack-prefix",
                json={"lease_token": token, "through_seq": 2})
check("g: prefix ack 409 with gap", r.status_code == 409
      and r.json()["first_break"]["seq"] == 5, r.text)
restore_event(path, saved)
r = client.post("/consumers/g/ack-prefix",
                json={"lease_token": token, "through_seq": 2})
check("g: same token prefix ack after repair",
      r.status_code == 200 and r.json()["cursor_seq"] == 2, r.text)
check("g: lease cleared after prefix ack",
      lease_row(path, "g")["lease_token"] is None)
# 下次 pull 从 3 重新租用剩余事件(新 token)
r = client.post("/consumers/g/pull", json={"limit": 100, "lease_seconds": 300})
check("g: re-pull starts after prefix",
      r.status_code == 200 and [e["seq"] for e in r.json()["events"]] == [3, 4, 5]
      and r.json()["lease_token"] != token, r.text)

# ---------- H. 无缺口时原有约定全部保持 ----------
path = use_db("h.db")
post_events(path, 5)

# 首次拉取: 正常批次 + token
r1 = client.post("/consumers/h/pull", json={"limit": 2, "lease_seconds": 300})
check("normal first pull batch",
      r1.status_code == 200 and [e["seq"] for e in r1.json()["events"]] == [1, 2]
      and r1.json()["lease_token"], r1.text)
tok1, exp1 = r1.json()["lease_token"], r1.json()["lease_expires_at"]
# 未过期重拉: 同批次/同 token/同过期时间(不续租), 请求参数被忽略
r2 = client.post("/consumers/h/pull", json={"limit": 99, "lease_seconds": 1})
check("active re-pull identical",
      r2.status_code == 200 and [e["seq"] for e in r2.json()["events"]] == [1, 2]
      and r2.json()["lease_token"] == tok1
      and r2.json()["lease_expires_at"] == exp1, r2.text)
# 过期后: 换发新 token, 同区间
expire_lease(path, "h")
r3 = client.post("/consumers/h/pull", json={"limit": 2, "lease_seconds": 60})
check("expired pull renews with new token",
      r3.status_code == 200 and [e["seq"] for e in r3.json()["events"]] == [1, 2]
      and r3.json()["lease_token"] != tok1 and r3.json()["lease_token"], r3.text)
tok3 = r3.json()["lease_token"]
# 旧 token 确认 409, 新 token 确认成功
check("old token ack 409 after renewal",
      client.post("/consumers/h/ack", json={"lease_token": tok1}).status_code == 409)
r = client.post("/consumers/h/ack", json={"lease_token": tok3})
check("new token ack ok", r.status_code == 200 and r.json()["cursor_seq"] == 2, r.text)
# 剩余事件继续消费干净(3..5), 队列才空
r = client.post("/consumers/h/pull", json={"limit": 10, "lease_seconds": 60})
check("pull remainder 3-5",
      [e["seq"] for e in r.json()["events"]] == [3, 4, 5], r.text)
r = client.post("/consumers/h/ack", json={"lease_token": r.json()["lease_token"]})
check("ack remainder to 5", r.status_code == 200
      and r.json()["cursor_seq"] == 5, r.text)
# 空队列: null token, 不建租约
r = client.post("/consumers/h/pull", json={"limit": 10, "lease_seconds": 60})
check("empty queue null token",
      r.status_code == 200 and r.json()["events"] == []
      and r.json()["lease_token"] is None and r.json()["lease_expires_at"] is None, r.text)
check("empty pull leaves no lease", lease_row(path, "h")["lease_token"] is None)
# 新事件到达后继续拉取
post_events(path, 1, prefix="extra")
r = client.post("/consumers/h/pull", json={"limit": 10})
check("pull resumes after new arrival",
      [e["seq"] for e in r.json()["events"]] == [6], r.text)
client.post("/consumers/h/ack", json={"lease_token": r.json()["lease_token"]})

# ---------- I. 归档锚点仍是可投递起点, 锚点后缺口同样防护 ----------
path = use_db("i.db")
post_events(path, 5)
# 由 w-anchor 消费到 5 后压缩归档前 3 条
r = client.post("/consumers/w-anchor/pull", json={"limit": 100, "lease_seconds": 300})
client.post("/consumers/w-anchor/ack", json={"lease_token": r.json()["lease_token"]})
r = client.post("/retention/compact", json={"cutoff_seq": 3})
check("compact first 3", r.status_code == 200
      and r.json()["anchor_seq"] == 3, r.text)

# 压缩后才建档的 consumer(游标 0): 从 anchor_seq+1 = 4 开始投递
r = client.post("/consumers/late/pull", json={"limit": 100, "lease_seconds": 60})
check("new consumer starts after anchor",
      r.status_code == 200 and [e["seq"] for e in r.json()["events"]] == [4, 5]
      and r.json()["lease_token"], r.text)
client.post("/consumers/late/ack", json={"lease_token": r.json()["lease_token"]})

# 锚点之后紧邻缺口(删 seq=4): 新 consumer 首次拉取 409, first_break 为现存的 seq=5
delete_event(path, 4)
r = client.post("/consumers/late2/pull", json={"limit": 100, "lease_seconds": 60})
check("gap right after anchor -> 409 from anchor start",
      r.status_code == 409 and r.json()["first_break"] ==
      {"seq": 5, "event_id": "evt-5", "reason": "seq_gap"}
      and r.json()["lease_from_seq"] == 4, r.text)
check("late2 no lease on 409", lease_row(path, "late2")["lease_token"] is None)

print(f"\n{ok} passed, {fail} failed")
raise SystemExit(1 if fail else 0)
