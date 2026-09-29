import json
import secrets
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator, Optional, Sequence

from fastapi import Body, FastAPI, Path, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator

from .chain import record_hash
from .db import get_chain_anchor, get_conn, init_db

app = FastAPI(title="Webhook Event Receiver & Replay")


@app.on_event("startup")
def startup() -> None:
    init_db()


# ---------- models ----------

class EventIn(BaseModel):
    event_id: str = Field(..., min_length=1, max_length=255)
    occurred_at: datetime
    payload: dict[str, Any]


class ReplayIn(BaseModel):
    event_ids: list[str] = Field(..., min_length=1, max_length=1000)


class PullIn(BaseModel):
    limit: int = Field(100, ge=1, le=1000)
    lease_seconds: int = Field(30, ge=1, le=86400)


class AckIn(BaseModel):
    lease_token: str = Field(..., min_length=1, max_length=255)


class AckPrefixIn(BaseModel):
    lease_token: str = Field(..., min_length=1, max_length=255)
    # 不加下界约束: 超出租约区间(含 <=0)统一走 409, 而不是 422
    through_seq: int


class RewindIn(BaseModel):
    # 不加下界约束: 低于链锚点(含负数)统一走 409, 而不是 422
    target_seq: int = Field(...)


class CompactIn(BaseModel):
    cutoff_seq: int = Field(..., ge=0)
    dry_run: bool = False


class ViewIn(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    # payload 顶层字符串字段的等值条件; 空对象(缺省)表示不过滤, 统计全部事件
    filters: dict[str, str] = Field(default_factory=dict, max_length=50)

    @field_validator("name")
    @classmethod
    def name_not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("name must not be blank")
        return v

    @field_validator("filters")
    @classmethod
    def filter_keys_not_blank(cls, v: dict[str, str]) -> dict[str, str]:
        for k in v:
            if not k.strip():
                raise ValueError("filter keys must not be blank")
        return v


class QuarantineIn(BaseModel):
    lease_token: str = Field(..., min_length=1, max_length=255)
    # 非空失败原因; 只允许空白也视为非法(422), 409 只保留给租约状态冲突
    reason: str = Field(..., min_length=1, max_length=2000)

    @field_validator("reason")
    @classmethod
    def reason_not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("reason must not be blank")
        return v


class ParentIn(BaseModel):
    parent_id: str = Field(..., min_length=1, max_length=255)


# ---------- helpers ----------

def normalize_dt(dt: datetime) -> str:
    """归一化为 UTC ISO8601 字符串, 保证可按字典序做时间范围比较。"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def lease_conflict(detail: str) -> JSONResponse:
    return JSONResponse(status_code=409, content={"detail": detail})


def seq_gap_conflict(
    detail: str,
    first_break: dict[str, Any],
    *,
    from_seq: int,
    to_seq: int,
) -> JSONResponse:
    """批次/租约区间存在序号缺口时的 409。

    响应体附与 /integrity 同形的 first_break(seq/event_id/reason="seq_gap")
    及拟返回批次(当前租约)的区间边界; 调用方须保证游标与租约保持原状,
    且本响应不发放任何新令牌。
    """
    return JSONResponse(
        status_code=409,
        content={
            "detail": detail,
            "first_break": first_break,
            "lease_from_seq": from_seq,
            "lease_to_seq": to_seq,
        },
    )


def row_to_event(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "seq": row["id"],  # 首次接收顺序号
        "event_id": row["event_id"],
        "occurred_at": row["occurred_at"],
        "payload": json.loads(row["payload"]),
        "received_at": row["received_at"],
    }


def row_to_quarantine(row: sqlite3.Row) -> dict[str, Any]:
    """隔离记录视图: 自带原事件完整内容副本, 原记录被压缩归档后仍可查看。"""
    return {
        "id": row["id"],
        "consumer_id": row["consumer_id"],
        "event": {
            "seq": row["event_seq"],
            "event_id": row["event_id"],
            "occurred_at": row["occurred_at"],
            "payload": json.loads(row["payload"]),
            "received_at": row["received_at"],
            "hash": row["event_hash"],
        },
        "reason": row["reason"],
        "quarantined_at": row["quarantined_at"],
    }


def row_to_export_event(row: sqlite3.Row) -> dict[str, Any]:
    """快照副本中的事件视图: 含链字段 prev_hash/hash 的完整记录。"""
    return {
        "seq": row["seq"],
        "event_id": row["event_id"],
        "occurred_at": row["occurred_at"],
        "payload": json.loads(row["payload"]),
        "received_at": row["received_at"],
        "prev_hash": row["prev_hash"],
        "hash": row["hash"],
    }


def view_matches(filters: dict[str, str], payload: Any) -> bool:
    """事件是否命中视图: filters 中每个顶层字段的等值条件都满足(仅字符串值参与比较)。"""
    if not filters:
        return True
    if not isinstance(payload, dict):
        return False
    for key, expected in filters.items():
        value = payload.get(key)
        if not isinstance(value, str) or value != expected:
            return False
    return True


def apply_event_to_views(conn: sqlite3.Connection, *, occurred_at: str, payload: Any) -> None:
    """把一条新建事件应用到所有日计数视图(命中的视图对应 UTC 日桶 +1)。调用方须持有写事务。"""
    views = conn.execute("SELECT id, filters FROM count_views").fetchall()
    if not views:
        return
    # occurred_at 已归一化为 UTC ISO8601, 前 10 字符即 UTC 日期
    day = occurred_at[:10]
    for view in views:
        if view_matches(json.loads(view["filters"]), payload):
            conn.execute(
                "INSERT INTO count_view_buckets (view_id, day, count) VALUES (?, ?, 1)"
                " ON CONFLICT(view_id, day) DO UPDATE SET count = count + 1",
                (view["id"], day),
            )


def remove_event_from_views(conn: sqlite3.Connection, *, occurred_at: str, payload: Any) -> None:
    """把一条被删除的事件从所有日计数视图扣减(命中的视图对应 UTC 日桶 -1, 空桶移除)。调用方须持有写事务。"""
    views = conn.execute("SELECT id, filters FROM count_views").fetchall()
    if not views:
        return
    day = occurred_at[:10]
    for view in views:
        if view_matches(json.loads(view["filters"]), payload):
            conn.execute(
                "UPDATE count_view_buckets SET count = count - 1"
                " WHERE view_id = ? AND day = ?",
                (view["id"], day),
            )
            # 空桶移除: 查询结果只保留现存事件支撑的计数
            conn.execute(
                "DELETE FROM count_view_buckets"
                " WHERE view_id = ? AND day = ? AND count <= 0",
                (view["id"], day),
            )


# ---------- field coverage: 快照字段覆盖画像 ----------

# 画像使用的六种 JSON 类型(标量统一四类, 整数与浮点不区分)
FIELD_TYPES: tuple[str, ...] = ("object", "array", "string", "number", "boolean", "null")
# 持久化统计表中的类型列名, 顺序与 FIELD_TYPES 一一对应
FIELD_TYPE_COLUMNS: tuple[str, ...] = tuple(f"t_{t}" for t in FIELD_TYPES)


def json_type_of(value: Any) -> str:
    """JSON 类型名: 注意 bool 是 int 的子类, 必须先判 bool。"""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    return "object"


def escape_field_key(key: str) -> str:
    """对象键的路径段转义: ~ -> ~0, / -> ~1, * -> ~2(~ 最先, 避免二次转义)。"""
    return key.replace("~", "~0").replace("/", "~1").replace("*", "~2")


def iter_payload_paths(payload: Any) -> Iterator[tuple[str, str]]:
    """迭代产出 (path, json_type): 根路径为空串, 对象键按 JSON Pointer 转义,
    数组下标统一为 "*"。空数组不产生元素路径; 同一事件内同一路径可能出现
    多次(数组多个元素), 由调用方按 (path, type) 去重。"""
    # 显式栈的迭代遍历, 避免深层 JSON 触发递归上限
    stack: list[tuple[str, Any]] = [("", payload)]
    while stack:
        path, value = stack.pop()
        yield path, json_type_of(value)
        if isinstance(value, dict):
            # 倒序压栈使遍历顺序无关紧要(最终由集合去重 + 字典序排序)
            for key in reversed(list(value.keys())):
                stack.append((f"{path}/{escape_field_key(key)}", value[key]))
        elif isinstance(value, list) and value:
            # 非空数组才压入元素: 空数组不产生元素路径
            for element in reversed(value):
                stack.append((f"{path}/*", element))


def valid_field_path(path: str) -> bool:
    """游标路径是否合法: 必须是合法的路径转义。

    - 根路径为空串; 非根路径以 "/" 开头;
    - "~" 后只允许 0/1/2; 裸 "*" 是数组下标占位段, 不能出现在键段内部
      (键里的星号必已转义为 ~2)。
    """
    if path == "":
        return True
    if not path.startswith("/"):
        return False
    i, n = 1, len(path)
    while i < n:
        c = path[i]
        if c == "~":
            if i + 1 >= n or path[i + 1] not in "012":
                return False
            i += 2
        elif c == "*":
            # 合法位置仅为整个元素段: 形如 "/*" 或 "/*/"
            if not (i == 1 or path[i - 1] == "/") or (
                i + 1 < n and path[i + 1] != "/"
            ):
                return False
            i += 1
        else:
            i += 1
    return True


def aggregate_field_coverage(payloads: Iterator[Any]) -> tuple[int, dict[str, dict[str, Any]]]:
    """聚合多条事件 payload 的字段覆盖: 返回 (事件总数, {path: stats})。

    同一事件内同一路径只计一次出现, 同一 (路径, 类型) 也只计一次——
    因此混合数组会让类型计数之和超过出现次数; null 计为出现。
    """
    total = 0
    paths: dict[str, dict[str, Any]] = {}
    for payload in payloads:
        total += 1
        # 出现按路径去重(同一路径在一个事件中只计一次), 类型按 (路径, 类型)
        # 去重(每种类型也只计一次); 两者分开, 混合数组的类型数之和才可能超过出现数
        seen_paths: set[str] = set()
        seen_path_types: set[tuple[str, str]] = set()
        for p, t in iter_payload_paths(payload):
            seen_paths.add(p)
            seen_path_types.add((p, t))
        for p in seen_paths:
            stats = paths.get(p)
            if stats is None:
                stats = {"present": 0, "types": {t: 0 for t in FIELD_TYPES}}
                paths[p] = stats
            stats["present"] += 1
        for p, t in seen_path_types:
            paths[p]["types"][t] += 1
    return total, paths


def ensure_field_coverage(conn: sqlite3.Connection, export_id: int) -> Optional[sqlite3.Row]:
    """确保快照的路径统计已持久化, 返回统计头行(不存在的快照返回 None)。

    统计结果惰性生成: 首次查询(含本功能上线前已存在的快照)在单个写事务
    (BEGIN IMMEDIATE)内扫描 export_events 副本一次性聚合, 随后把统计头与
    全部路径行**同一事务**原子写入——行只在事务提交后才可见, 不存在半成品。
    并发首次查询在写锁上串行: 首个查询计算并落库, 其余查询持锁后复查命中,
    不会重复计算或写入。之后的翻页只读路径表, 不再扫描副本, 与事件数无关。
    统计只来自快照副本, 绝不读取当前 events 表。调用方须持有已确认存在的快照。
    """
    header = conn.execute(
        "SELECT total_events, path_count FROM export_field_coverage"
        " WHERE snapshot_id = ?",
        (export_id,),
    ).fetchone()
    if header is not None:
        return header

    # 快速路径未命中: 取写锁后复查, 把并发首次查询串行化
    conn.execute("BEGIN IMMEDIATE")
    # 持锁复查: 可能已被并发的首个查询生成, 也可能快照正被并发删除
    header = conn.execute(
        "SELECT total_events, path_count FROM export_field_coverage"
        " WHERE snapshot_id = ?",
        (export_id,),
    ).fetchone()
    if header is not None:
        return header
    if conn.execute(
        "SELECT 1 FROM export_snapshots WHERE id = ?", (export_id,)
    ).fetchone() is None:
        # 快照在本事务等待写锁期间被删除: 不生成任何统计, 由调用方按 404 处理
        return None

    rows = conn.execute(
        "SELECT payload FROM export_events WHERE snapshot_id = ? ORDER BY seq",
        (export_id,),
    ).fetchall()
    total, path_stats = aggregate_field_coverage(
        json.loads(r["payload"]) for r in rows
    )
    generated_at = utc_now()
    all_paths = sorted(path_stats)
    conn.execute(
        "INSERT INTO export_field_coverage (snapshot_id, total_events, path_count,"
        " generated_at) VALUES (?, ?, ?, ?)",
        (export_id, total, len(all_paths), generated_at),
    )
    if all_paths:
        # executemany 同事务批量落库: 提交前对外完全不可见
        conn.executemany(
            "INSERT INTO export_field_coverage_paths (snapshot_id, path,"
            " present_events, t_object, t_array, t_string, t_number, t_boolean,"
            " t_null) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    export_id,
                    p,
                    path_stats[p]["present"],
                    *(path_stats[p]["types"][t] for t in FIELD_TYPES),
                )
                for p in all_paths
            ],
        )
    return conn.execute(
        "SELECT total_events, path_count FROM export_field_coverage"
        " WHERE snapshot_id = ?",
        (export_id,),
    ).fetchone()


# ---------- endpoints ----------

def current_chain_head(conn: sqlite3.Connection) -> str:
    """读取当前链头哈希: 取最后一条事件的 hash; 事件表为空时从链锚点续链(创世为空串)。"""
    row = conn.execute("SELECT hash FROM events ORDER BY id DESC LIMIT 1").fetchone()
    if row:
        return row["hash"]
    # 事件表为空: 若曾压缩归档则从链锚点续链, 否则为创世空串
    _, prev_hash = get_chain_anchor(conn)
    return prev_hash


def insert_chained_event(
    conn: sqlite3.Connection,
    *,
    event_id: str,
    occurred_at: str,
    payload: Any,
    received_at: str,
    prev_hash: str,
) -> tuple[int, str]:
    """在当前写事务内插入一条事件并续接哈希链, 返回 (新 seq, 本记录哈希)。调用方须持有写事务。"""
    cur = conn.execute(
        "INSERT INTO events (event_id, occurred_at, payload, received_at)"
        " VALUES (?, ?, ?, ?)",
        (event_id, occurred_at, json.dumps(payload, ensure_ascii=False), received_at),
    )
    seq = cur.lastrowid
    h = record_hash(
        seq=seq,
        event_id=event_id,
        occurred_at=occurred_at,
        payload=payload,
        received_at=received_at,
        prev_hash=prev_hash,
    )
    conn.execute(
        "UPDATE events SET prev_hash = ?, hash = ? WHERE id = ?",
        (prev_hash, h, seq),
    )
    return seq, h


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/events", status_code=201)
def create_event(event: EventIn):
    """接收单个事件, 按 event_id 幂等: 重复投递(即使原事件已被 /retention/compact
    归档删除)不产生重复记录、不再分配 seq, 也不改变哈希链与日计数视图。"""
    received_at = utc_now()
    occurred_at = normalize_dt(event.occurred_at)
    try:
        with get_conn() as conn:
            # BEGIN IMMEDIATE 提前获取写锁, 保证"查占用 -> 写事件 + 占 ID"在同一事务内原子完成
            conn.execute("BEGIN IMMEDIATE")
            # 幂等判定只看 ID 占用登记: 事件内容被归档删除后占用仍在, 重投依旧 duplicate
            occupied = conn.execute(
                "SELECT 1 FROM received_event_ids WHERE event_id = ?",
                (event.event_id,),
            ).fetchone()
            if occupied is not None:
                return JSONResponse(
                    status_code=200,
                    content={"status": "duplicate", "event_id": event.event_id},
                )
            prev_hash = current_chain_head(conn)
            seq, _ = insert_chained_event(
                conn,
                event_id=event.event_id,
                occurred_at=occurred_at,
                payload=event.payload,
                received_at=received_at,
                prev_hash=prev_hash,
            )
            # 同事务占用 event_id: 与事件写入、视图更新一起提交或一起回滚,
            # 归档只删事件内容不删占用, 此 ID 此后永久判重
            conn.execute(
                "INSERT INTO received_event_ids (event_id, seq, received_at)"
                " VALUES (?, ?, ?)",
                (event.event_id, seq, received_at),
            )
            # 同一事务内增量更新日计数视图; 重复 event_id 已在上方提前返回, 不会到此
            apply_event_to_views(conn, occurred_at=occurred_at, payload=event.payload)
    except sqlite3.IntegrityError:
        # 并发兜底: 另一写事务已抢先占用/写入同一 event_id -> 幂等返回, 链保持不变
        return JSONResponse(
            status_code=200,
            content={"status": "duplicate", "event_id": event.event_id},
        )
    return {"status": "created", "event_id": event.event_id}


@app.post("/events/batch")
def create_events_batch(
    events: list[EventIn] = Body(..., min_length=1, max_length=1000),
):
    """批量接收 1~1000 条事件(与单条接口同格式的 JSON 数组), 整批在同一写事务中处理。

    - 按请求顺序逐条判定 event_id: ID 占用登记中已有(含事件内容已被
      /retention/compact 归档删除、仅余占用的 ID)、或本批前面已出现过的 ID
      标记 duplicate, 不写入、不消耗 seq; 仅首次出现的 ID 入库。
    - 新事件按请求顺序从当前链头逐条续接哈希链, seq 连续无空洞, 并在同一事务
      内占用各自的 event_id; 占用随写入提交、不随后续归档删除。
    - 任一条格式非法(422, 校验在事务前完成)或写入失败(事务回滚)时,
      整批不留下部分新事件。
    - 响应 results 严格按输入顺序给出每条状态(created 附 seq; duplicate 的 seq 为 null),
      并汇总 created/duplicate 数量。全部重复返回 200, 否则 201。
    """
    # 整批共用同一接收时间(与单条接口一样进入哈希), 保证批内链一致
    received_at = utc_now()
    normalized = [(e.event_id, normalize_dt(e.occurred_at), e.payload) for e in events]

    results: list[dict[str, Any]] = []
    created = 0
    with get_conn() as conn:
        try:
            # BEGIN IMMEDIATE 提前获取写锁: 判定重复 -> 批量写入在同一事务内原子完成
            conn.execute("BEGIN IMMEDIATE")
            # 已占用的 event_id(含已归档仅余占用)一次取齐; 本批内首次出现的 ID 实时加入 seen
            placeholders = ",".join("?" * len(normalized))
            existing = {
                row["event_id"]
                for row in conn.execute(
                    f"SELECT event_id FROM received_event_ids WHERE event_id IN ({placeholders})",
                    [eid for eid, _, _ in normalized],
                ).fetchall()
            }
            seen: set[str] = set(existing)
            prev_hash = current_chain_head(conn)

            for event_id, occurred_at, payload in normalized:
                if event_id in seen:
                    # 已占用(库中现存或已归档)或本批前面出现过: 幂等跳过, 不写入也不推进链头
                    results.append(
                        {"event_id": event_id, "status": "duplicate", "seq": None}
                    )
                    continue
                # 唯一约束兜底(写锁下理论上不会冲突): 冲突即放弃整批
                seq, h = insert_chained_event(
                    conn,
                    event_id=event_id,
                    occurred_at=occurred_at,
                    payload=payload,
                    received_at=received_at,
                    prev_hash=prev_hash,
                )
                # 与事件同事务占用 ID: 提交后即使事件内容被归档, 重投仍判 duplicate
                conn.execute(
                    "INSERT INTO received_event_ids (event_id, seq, received_at)"
                    " VALUES (?, ?, ?)",
                    (event_id, seq, received_at),
                )
                prev_hash = h
                seen.add(event_id)
                created += 1
                # 同一事务内增量更新日计数视图; duplicate 分支已跳过, 不会重复计数
                apply_event_to_views(conn, occurred_at=occurred_at, payload=payload)
                results.append({"event_id": event_id, "status": "created", "seq": seq})
        except sqlite3.IntegrityError:
            # 并发兜底: 批量不接受"部分写入", 回滚整批, 不留下任何新事件
            conn.rollback()
            return JSONResponse(
                status_code=409,
                content={"detail": "concurrent event_id conflict, whole batch rejected"},
            )

    body = {
        "created": created,
        "duplicate": len(events) - created,
        "results": results,
    }
    return JSONResponse(status_code=201 if created else 200, content=body)


@app.get("/events")
def list_events(
    from_: Optional[datetime] = Query(None, alias="from", description="occurred_at 下界(含)"),
    to: Optional[datetime] = Query(None, description="occurred_at 上界(含)"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
):
    """按 occurred_at 时间范围分页查询。"""
    clauses, params = [], []
    if from_ is not None:
        clauses.append("occurred_at >= ?")
        params.append(normalize_dt(from_))
    if to is not None:
        clauses.append("occurred_at <= ?")
        params.append(normalize_dt(to))
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    with get_conn() as conn:
        total = conn.execute(
            f"SELECT COUNT(*) AS c FROM events {where}", params
        ).fetchone()["c"]
        rows = conn.execute(
            f"SELECT * FROM events {where} ORDER BY occurred_at, id LIMIT ? OFFSET ?",
            (*params, page_size, (page - 1) * page_size),
        ).fetchall()

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": [row_to_event(r) for r in rows],
    }


@app.post("/replay")
def replay(req: ReplayIn):
    """批量重放: 命中的事件按首次接收顺序输出, 未命中的标记为 skipped。"""
    # 请求内去重, 保证重复 id 不产生重复输出
    requested = list(dict.fromkeys(req.event_ids))

    found: dict[str, dict[str, Any]] = {}
    with get_conn() as conn:
        placeholders = ",".join("?" * len(requested))
        rows = conn.execute(
            f"SELECT * FROM events WHERE event_id IN ({placeholders}) ORDER BY id",
            requested,
        ).fetchall()
    for r in rows:
        found[r["event_id"]] = row_to_event(r)

    results: list[dict[str, Any]] = []
    # 命中部分: 严格按首次接收顺序(seq)输出
    for event in sorted(found.values(), key=lambda e: e["seq"]):
        results.append({"event_id": event["event_id"], "status": "replayed", "event": event})
    # 未命中部分: 按请求顺序追加, 标记 skipped
    for eid in requested:
        if eid not in found:
            results.append({"event_id": eid, "status": "skipped", "reason": "not_found"})

    replayed = sum(1 for r in results if r["status"] == "replayed")
    return {
        "replayed": replayed,
        "skipped": len(results) - replayed,
        "results": results,
    }


# ---------- consumer cursor: pull / ack / rewind ----------

def first_seq_gap(
    conn: sqlite3.Connection, from_seq: int, to_seq: int
) -> Optional[dict[str, Any]]:
    """核对闭区间 [from_seq, to_seq] 的序号连续性(只核对序号, 不涉及哈希)。

    用于消费流程在发放/复用租约以及确认推进前自检: events.id 即 seq,
    连续意味着区间内每个 seq 都恰好存在一条现存记录。返回首个断点的
    /integrity 同形描述 {"seq", "event_id", "reason": "seq_gap"};
    无缺口(含空区间 from_seq > to_seq)时返回 None。

    断点取值与 /integrity 的 seq_gap 判定保持一致:
    - 区间内部缺号(缺口后仍有现存记录): 断点为缺口后的第一条现存记录;
    - 缺口一直延伸到区间末尾(批次/租约最后一条被删, /integrity 因无后续
      记录而无法定位): 断点为区间内首个缺失的 seq, event_id 按 ID 占用表
      还原(无占用登记时回退为空串)——消费流程据此拒绝越过这类缺口。
    """
    if from_seq > to_seq:
        return None
    rows = conn.execute(
        "SELECT id, event_id FROM events WHERE id BETWEEN ? AND ? ORDER BY id",
        (from_seq, to_seq),
    ).fetchall()
    expected = from_seq
    for row in rows:
        if row["id"] != expected:
            # 缺口之后的第一条现存记录: 与 /integrity 的 seq_gap 断点一致
            return {"seq": row["id"], "event_id": row["event_id"], "reason": "seq_gap"}
        expected += 1
    if expected <= to_seq:
        # 现存记录在到齐前耗尽: 缺口延伸到区间末尾(最后一条/若干条缺失)。
        # /integrity 只能定位"缺口后的现存记录", 此处没有, 故用首个缺失 seq
        occupied = conn.execute(
            "SELECT event_id FROM received_event_ids WHERE seq = ?", (expected,)
        ).fetchone()
        return {
            "seq": expected,
            "event_id": occupied["event_id"] if occupied is not None else "",
            "reason": "seq_gap",
        }
    return None


@app.post("/consumers/{consumer_id}/pull")
def pull(
    consumer_id: str = Path(..., min_length=1, max_length=255),
    req: PullIn = Body(default_factory=PullIn),
):
    """按首次接收顺序拉取尚未确认的连续事件并原子租用。

    - 首次 consumer 的游标为 0, 即从 seq=1 开始投递(归档锚点之后才建档的
      consumer 从 anchor_seq + 1 开始, 归档锚点是可投递起点)。
    - 存在未过期租约时, 重复拉取原样返回同一批次(不续租、不换 token)。
    - 租约过期后同一批次可被重新投递(换发新 token)。
    - 队列为空(没有可投递起点之后的事件)时返回空批次, 不创建租约。
    - 三种会产出/复用批次的路径(首次拉取、未过期租约重拉、过期续租)都先核对
      拟返回批次从起点到末尾的序号连续性; 存在 seq_gap 时返回 409 及与
      /integrity 一致的 first_break, 游标与租约保持原状(不续租、不发新令牌),
      记录恢复后重试即按原约定正常返回。
    """
    now = utc_now()
    # BEGIN IMMEDIATE 与事件写入互斥, 保证"读批次 -> 建租约"原子完成
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        # 首次出现的 consumer 自动建档, 游标从 0 开始
        conn.execute(
            "INSERT OR IGNORE INTO consumers (consumer_id, cursor_seq) VALUES (?, 0)",
            (consumer_id,),
        )
        consumer = conn.execute(
            "SELECT cursor_seq, lease_token, lease_from_seq, lease_to_seq,"
            " lease_expires_at FROM consumers WHERE consumer_id = ?",
            (consumer_id,),
        ).fetchone()

        active = (
            consumer["lease_token"] is not None
            and datetime.fromisoformat(consumer["lease_expires_at"]) > datetime.fromisoformat(now)
        )
        if active:
            # 租约未过期: 重复拉取必须返回同一批次。先核对租约区间的序号连续性:
            # 区间内记录在租用后发生缺失(如库外意外删除)时返回 409, 原批次、
            # token、过期时间与游标全部保持原状(不续租、不换发新令牌)。
            gap = first_seq_gap(
                conn, consumer["lease_from_seq"], consumer["lease_to_seq"]
            )
            if gap is not None:
                return seq_gap_conflict(
                    "seq gap detected within leased batch",
                    gap,
                    from_seq=consumer["lease_from_seq"],
                    to_seq=consumer["lease_to_seq"],
                )
            rows = conn.execute(
                "SELECT * FROM events WHERE id BETWEEN ? AND ? ORDER BY id",
                (consumer["lease_from_seq"], consumer["lease_to_seq"]),
            ).fetchall()
            return {
                "consumer_id": consumer_id,
                "events": [row_to_event(r) for r in rows],
                "lease_token": consumer["lease_token"],
                "lease_expires_at": consumer["lease_expires_at"],
            }

        if consumer["lease_token"] is not None:
            # 上一租约已过期: 同一批次可重新投递。续租前同样核对原区间序号连续性:
            # 缺口未修复时返回 409, 沿用原租约(token 与过期时间原样保留,
            # 不换发新令牌); 缺口修复后再续租才按原区间换发新 token。
            gap = first_seq_gap(
                conn, consumer["lease_from_seq"], consumer["lease_to_seq"]
            )
            if gap is not None:
                return seq_gap_conflict(
                    "seq gap detected within leased batch",
                    gap,
                    from_seq=consumer["lease_from_seq"],
                    to_seq=consumer["lease_to_seq"],
                )
            rows = conn.execute(
                "SELECT * FROM events WHERE id BETWEEN ? AND ? ORDER BY id",
                (consumer["lease_from_seq"], consumer["lease_to_seq"]),
            ).fetchall()
            token = secrets.token_urlsafe(32)
            expires_at = (
                datetime.fromisoformat(now) + timedelta(seconds=req.lease_seconds)
            ).isoformat()
            conn.execute(
                "UPDATE consumers SET lease_token = ?, lease_expires_at = ?"
                " WHERE consumer_id = ?",
                (token, expires_at, consumer_id),
            )
            return {
                "consumer_id": consumer_id,
                "events": [row_to_event(r) for r in rows],
                "lease_token": token,
                "lease_expires_at": expires_at,
            }

        # 从未租用: 从游标之后取一段事件。归档锚点是可投递起点: 锚点之前的事件
        # 已压缩归档, 压缩后才建档的 consumer(游标仍为 0)也从锚点之后开始。
        anchor_seq, _ = get_chain_anchor(conn)
        start_seq = max(consumer["cursor_seq"], anchor_seq) + 1
        rows = conn.execute(
            "SELECT * FROM events WHERE id >= ? ORDER BY id LIMIT ?",
            (start_seq, req.limit),
        ).fetchall()
        if not rows:
            # 空队列: 返回空批次且不创建租约
            return {
                "consumer_id": consumer_id,
                "events": [],
                "lease_token": None,
                "lease_expires_at": None,
            }

        from_seq, to_seq = rows[0]["id"], rows[-1]["id"]
        # 发租约前核对拟返回批次从起点到末尾的序号连续性: 事件表中间记录意外
        # 缺失时, LIMIT 会把缺口两侧事件合为一批。遇缺口返回 409(与 /integrity
        # 一致的 first_break), 不建租约、不发令牌, 游标保持原状; 记录恢复后再
        # 拉取即正常成批。
        gap = first_seq_gap(conn, start_seq, to_seq)
        if gap is not None:
            return seq_gap_conflict(
                "seq gap detected while leasing next batch",
                gap,
                from_seq=start_seq,
                to_seq=to_seq,
            )
        token = secrets.token_urlsafe(32)
        expires_at = (
            datetime.fromisoformat(now) + timedelta(seconds=req.lease_seconds)
        ).isoformat()
        conn.execute(
            "UPDATE consumers SET lease_token = ?, lease_from_seq = ?,"
            " lease_to_seq = ?, lease_expires_at = ? WHERE consumer_id = ?",
            (token, from_seq, to_seq, expires_at, consumer_id),
        )
        return {
            "consumer_id": consumer_id,
            "events": [row_to_event(r) for r in rows],
            "lease_token": token,
            "lease_expires_at": expires_at,
        }


@app.post("/consumers/{consumer_id}/ack")
def ack(
    consumer_id: str = Path(..., min_length=1, max_length=255),
    req: AckIn = Body(...),
):
    """确认当前租约批次: 仅接受未过期且与当前租约一致的 token, 游标推进到批次末尾。

    过期、伪造或旧 token 一律 409; 游标只随整批确认前移, 天然禁止越序确认。
    推进前还会核对整个租约区间的序号连续性: 区间内仍有 seq_gap(如租用期间
    中间记录意外缺失)时拒绝确认, 返回与 /integrity 一致的 first_break,
    游标与租约保持原状、旧 token 继续有效; 缺口修复后同一租约(沿用原时效与
    令牌)即可正常确认。
    """
    now = datetime.now(timezone.utc)
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        consumer = conn.execute(
            "SELECT cursor_seq, lease_token, lease_from_seq, lease_to_seq,"
            " lease_expires_at FROM consumers WHERE consumer_id = ?",
            (consumer_id,),
        ).fetchone()
        # 未知 consumer / 从未拉取过: 没有可确认的租约
        if consumer is None or consumer["lease_token"] is None:
            return lease_conflict("no active lease")
        if consumer["lease_token"] != req.lease_token:
            # 伪造或旧 token(旧批次过期后已被新租约取代)
            return lease_conflict("invalid lease token")
        if datetime.fromisoformat(consumer["lease_expires_at"]) <= now:
            # 租约已过期, 该批次可被重新投递, 旧 token 不得再确认
            return lease_conflict("lease expired")

        # 确认推进前核对租约区间: 缺口仍在则拒绝越过, 游标/租约/令牌不变
        gap = first_seq_gap(conn, consumer["lease_from_seq"], consumer["lease_to_seq"])
        if gap is not None:
            return seq_gap_conflict(
                "seq gap detected within leased batch",
                gap,
                from_seq=consumer["lease_from_seq"],
                to_seq=consumer["lease_to_seq"],
            )

        to_seq = consumer["lease_to_seq"]
        conn.execute(
            "UPDATE consumers SET cursor_seq = ?, lease_token = NULL,"
            " lease_from_seq = NULL, lease_to_seq = NULL, lease_expires_at = NULL"
            " WHERE consumer_id = ?",
            (to_seq, consumer_id),
        )
    return {
        "consumer_id": consumer_id,
        "acknowledged": True,
        "cursor_seq": to_seq,
    }


@app.post("/consumers/{consumer_id}/ack-prefix")
def ack_prefix(
    consumer_id: str = Path(..., min_length=1, max_length=255),
    req: AckPrefixIn = Body(...),
):
    """确认当前租约批次 [from_seq, through_seq] 的连续前缀。

    仅接受未过期且与当前租约一致的 token, through_seq 必须落在
    [lease_from_seq, lease_to_seq] 内; 校验通过后在同一写事务内把游标
    推进到 through_seq 并清除租约, 下次 pull 从 through_seq+1 重新租用,
    旧 token 立即作废(不能再用于 ack 或前缀确认)。

    through_seq == lease_to_seq 时等同整批确认。未知 consumer、无租约、
    token 不符(伪造/旧 token)、租约过期或序号越界一律 409 且状态不变。
    推进前还会核对整个租约区间的序号连续性: 区间内仍有 seq_gap 时拒绝确认
    (与整批 ack 同一套核对), 返回与 /integrity 一致的 first_break,
    游标与租约保持原状、旧 token 继续有效; 缺口修复后同一租约(沿用原时效
    与令牌)即可正常确认前缀。
    """
    now = datetime.now(timezone.utc)
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        consumer = conn.execute(
            "SELECT cursor_seq, lease_token, lease_from_seq, lease_to_seq,"
            " lease_expires_at FROM consumers WHERE consumer_id = ?",
            (consumer_id,),
        ).fetchone()
        # 未知 consumer / 从未拉取过: 没有可确认的租约
        if consumer is None or consumer["lease_token"] is None:
            return lease_conflict("no active lease")
        if consumer["lease_token"] != req.lease_token:
            # 伪造或旧 token(旧批次过期后已被新租约取代, 或已被确认清除)
            return lease_conflict("invalid lease token")
        if datetime.fromisoformat(consumer["lease_expires_at"]) <= now:
            # 租约已过期, 该批次可被重新投递, 旧 token 不得再确认
            return lease_conflict("lease expired")
        if req.through_seq < consumer["lease_from_seq"]:
            return JSONResponse(
                status_code=409,
                content={
                    "detail": "through_seq before batch start",
                    "through_seq": req.through_seq,
                    "lease_from_seq": consumer["lease_from_seq"],
                    "lease_to_seq": consumer["lease_to_seq"],
                },
            )
        if req.through_seq > consumer["lease_to_seq"]:
            return JSONResponse(
                status_code=409,
                content={
                    "detail": "through_seq beyond batch end",
                    "through_seq": req.through_seq,
                    "lease_from_seq": consumer["lease_from_seq"],
                    "lease_to_seq": consumer["lease_to_seq"],
                },
            )

        # 确认推进前核对整个租约区间: 缺口仍在(无论落在前缀内还是批次其余部分)
        # 一律拒绝推进, 游标/租约/令牌保持原状
        gap = first_seq_gap(conn, consumer["lease_from_seq"], consumer["lease_to_seq"])
        if gap is not None:
            return seq_gap_conflict(
                "seq gap detected within leased batch",
                gap,
                from_seq=consumer["lease_from_seq"],
                to_seq=consumer["lease_to_seq"],
            )

        # 同一事务内: 推进游标到 through_seq 并清除原租约
        conn.execute(
            "UPDATE consumers SET cursor_seq = ?, lease_token = NULL,"
            " lease_from_seq = NULL, lease_to_seq = NULL, lease_expires_at = NULL"
            " WHERE consumer_id = ?",
            (req.through_seq, consumer_id),
        )
    return {
        "consumer_id": consumer_id,
        "acknowledged": True,
        "through_seq": req.through_seq,
        "cursor_seq": req.through_seq,
    }


@app.post("/consumers/{consumer_id}/rewind")
def rewind(
    consumer_id: str = Path(..., min_length=1, max_length=255),
    req: RewindIn = Body(...),
):
    """将已登记 consumer 的游标回退到 target_seq, 下一次 pull 从 target_seq+1 重新投递。

    - 仅允许回退到 [anchor_seq, cursor_seq]: 低于链锚点(事件已压缩归档)或
      向前推进(target_seq > 当前游标)一律 409, 状态不变; 未知 consumer 同样 409。
    - 存在未过期租约时 409; 已过期租约在同一事务内清除后再回退,
      避免后续 pull 重投旧批次区间。
    - 与 pull/ack/compact 共用 BEGIN IMMEDIATE 写锁, 并发下只有单一结果;
      回退后游标不低于链锚点, 不会产出指向已压缩事件的租约。
    """
    now = datetime.now(timezone.utc)
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        consumer = conn.execute(
            "SELECT cursor_seq, lease_token, lease_expires_at"
            " FROM consumers WHERE consumer_id = ?",
            (consumer_id,),
        ).fetchone()
        # 未知 consumer: 不像 pull 那样自动建档, 直接 409
        if consumer is None:
            return lease_conflict("unknown consumer")

        if (
            consumer["lease_token"] is not None
            and datetime.fromisoformat(consumer["lease_expires_at"]) > now
        ):
            # 租约未过期: 回退会与在途批次冲突, 拒绝且状态不变
            return JSONResponse(
                status_code=409,
                content={
                    "detail": "consumer has an active lease",
                    "lease_expires_at": consumer["lease_expires_at"],
                },
            )

        anchor_seq, _ = get_chain_anchor(conn)
        cursor_seq = consumer["cursor_seq"]
        target = req.target_seq
        if target < anchor_seq:
            # 目标早于链锚点: 对应事件已压缩归档, 无法重新投递
            return JSONResponse(
                status_code=409,
                content={
                    "detail": "target_seq below chain anchor",
                    "target_seq": target,
                    "anchor_seq": anchor_seq,
                },
            )
        if target > cursor_seq:
            # 只允许回退; 向前推进只能经由 ack 整批确认
            return JSONResponse(
                status_code=409,
                content={
                    "detail": "target_seq would advance cursor",
                    "target_seq": target,
                    "cursor_seq": cursor_seq,
                },
            )

        # 同一事务内: 清除(已过期的)租约并回退游标
        conn.execute(
            "UPDATE consumers SET cursor_seq = ?, lease_token = NULL,"
            " lease_from_seq = NULL, lease_to_seq = NULL, lease_expires_at = NULL"
            " WHERE consumer_id = ?",
            (target, consumer_id),
        )
    return {
        "consumer_id": consumer_id,
        "previous_cursor_seq": cursor_seq,
        "cursor_seq": target,
        "next_seq": target + 1,
    }


# ---------- quarantine: 隔离毒丸事件 ----------

@app.post("/consumers/{consumer_id}/quarantine", status_code=201)
def quarantine(
    consumer_id: str = Path(..., min_length=1, max_length=255),
    req: QuarantineIn = Body(...),
):
    """隔离当前未过期租约批次的首条事件(消费失败的"毒丸"), 只越过这一条继续消费。

    单个写事务(BEGIN IMMEDIATE)内完成全部核对与状态变更:
    - 必须存在与 lease_token 一致且未过期的租约, 且批次起点必须是下一条
      可投递事件(lease_from_seq = max(cursor_seq, anchor_seq) + 1),
      防止旧批次/旧令牌绕过;
    - 复制首条事件的完整内容(event_id/occurred_at/payload/received_at/hash)及
      失败原因到独立隔离记录;
    - 游标仅越过这一条(cursor_seq = lease_from_seq)并清除整批租约;
    - 下一次 pull 重新租用剩余事件(换发新 token), 旧令牌立即作废, 不能再确认。

    未知 consumer、无租约、伪造/旧 token、租约过期或起点不相邻一律 409 且
    状态不变; 失败原因为空或纯空白由请求校验拦截(422)。
    """
    now = datetime.now(timezone.utc)
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        consumer = conn.execute(
            "SELECT cursor_seq, lease_token, lease_from_seq, lease_to_seq,"
            " lease_expires_at FROM consumers WHERE consumer_id = ?",
            (consumer_id,),
        ).fetchone()
        # 未知 consumer / 从未拉取过: 没有可隔离的租约
        if consumer is None or consumer["lease_token"] is None:
            return lease_conflict("no active lease")
        if consumer["lease_token"] != req.lease_token:
            # 伪造或旧 token(旧批次过期后已被新租约取代, 或已被 ack/隔离清除)
            return lease_conflict("invalid lease token")
        if datetime.fromisoformat(consumer["lease_expires_at"]) <= now:
            # 租约已过期, 该批次可被重新投递, 旧 token 不得再隔离
            return lease_conflict("lease expired")

        from_seq = consumer["lease_from_seq"]
        # 租约起点必须与"下一条可投递事件"相邻: 通常即 cursor_seq + 1;
        # 压缩归档后才建档的 consumer 游标仍为 0, 已归档区间不可投递, 起点为 anchor_seq + 1。
        anchor_seq, _ = get_chain_anchor(conn)
        expected_from = max(consumer["cursor_seq"], anchor_seq) + 1
        if from_seq != expected_from:
            # 租约起点不与可投递队首相邻: 状态不自洽的旧批次, 拒绝且状态不变
            return JSONResponse(
                status_code=409,
                content={
                    "detail": "lease start not adjacent to cursor",
                    "cursor_seq": consumer["cursor_seq"],
                    "anchor_seq": anchor_seq,
                    "lease_from_seq": from_seq,
                    "lease_to_seq": consumer["lease_to_seq"],
                },
            )

        # 只允许隔离首条事件: 按批次起点精确取这一条
        row = conn.execute(
            "SELECT id, event_id, occurred_at, payload, received_at, hash"
            " FROM events WHERE id = ?",
            (from_seq,),
        ).fetchone()
        if row is None:
            # 首条事件缺失(如已被压缩归档): 不能安全隔离, 状态不变
            return JSONResponse(
                status_code=409,
                content={
                    "detail": "lease head event unavailable",
                    "lease_from_seq": from_seq,
                },
            )

        quarantined_at = now.isoformat()
        cur = conn.execute(
            "INSERT INTO quarantined_events (consumer_id, event_seq, event_id,"
            " occurred_at, payload, received_at, event_hash, reason,"
            " quarantined_at, lease_token)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                consumer_id,
                row["id"],
                row["event_id"],
                row["occurred_at"],
                row["payload"],
                row["received_at"],
                row["hash"],
                req.reason,
                quarantined_at,
                req.lease_token,
            ),
        )
        quarantine_id = cur.lastrowid

        # 游标仅越过这一条, 整批租约清除: 剩余事件下次 pull 重新租用
        conn.execute(
            "UPDATE consumers SET cursor_seq = ?, lease_token = NULL,"
            " lease_from_seq = NULL, lease_to_seq = NULL, lease_expires_at = NULL"
            " WHERE consumer_id = ?",
            (from_seq, consumer_id),
        )

    return {
        "consumer_id": consumer_id,
        "quarantined": True,
        "quarantine_id": quarantine_id,
        "seq": row["id"],
        "event_id": row["event_id"],
        "cursor_seq": from_seq,
        "quarantined_at": quarantined_at,
    }


@app.get("/consumers/{consumer_id}/quarantine")
def list_quarantine(
    consumer_id: str = Path(..., min_length=1, max_length=255),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
):
    """按 consumer_id 查询该 consumer 的隔离记录(隔离时间顺序分页)。

    每条记录携带原事件的完整内容副本, 因此原事件被 /retention/compact
    安全归档(物理删除)后, 隔离副本仍可查看。未知 consumer 或无记录返回空列表。
    """
    with get_conn() as conn:
        total = conn.execute(
            "SELECT COUNT(*) AS c FROM quarantined_events WHERE consumer_id = ?",
            (consumer_id,),
        ).fetchone()["c"]
        rows = conn.execute(
            "SELECT * FROM quarantined_events WHERE consumer_id = ?"
            " ORDER BY id LIMIT ? OFFSET ?",
            (consumer_id, page_size, (page - 1) * page_size),
        ).fetchall()

    return {
        "consumer_id": consumer_id,
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": [row_to_quarantine(r) for r in rows],
    }


# ---------- retention: 安全归档压缩 ----------

@app.post("/retention/compact")
def compact(req: CompactIn):
    """压缩归档 (当前锚点, cutoff_seq] 的连续事件前缀。

    - 归档前缀校验: 从当前锚点起, 按与 /integrity 完全一致的哈希规范逐条核对
      拟删除区间——序号连续性(首条须为 anchor_seq + 1, 相邻 seq 逐一递增)、
      前后哈希衔接(prev_hash 等于上一条/锚点 hash)、事件内容哈希(按固定 JSON
      规范重算)。首个断点即返回 **409**, 响应给出断点的 seq/event_id/reason
      (reason 取值沿用 /integrity: seq_gap / prev_hash_mismatch /
      hash_mismatch); dry_run 同样校验、同样 409。任一情况下事件、日计数视图
      与链锚点均保持原状(不写库)。断点不在拟归档前缀内时, 既有连续区间仍可
      按边界正常归档。
    - 安全边界: cutoff_seq 不得超过所有已登记 consumer 的最小 cursor_seq;
      无 consumer 时以当前最大 seq 为边界。越界返回 409 并给出 safe_through_seq。
    - 执行时持久化最后一条被删记录的 seq/hash 作为链锚点, /integrity 从锚点继续校验。
    - 同一事务内按被删事件扣减所有日计数视图的对应日期计数(空桶移除)。
    - 只物理删除 events 中的事件内容; received_event_ids 中的 event_id 占用
      **刻意保留**, 故归档后同一 ID 重投仍判 duplicate(不分配新 seq、不重复投递)。
    - dry_run=true 只返回预计删除数与边界, 不写库。
    - cutoff_seq 已被既往压缩覆盖时为幂等空操作, 不再校验安全边界与前缀。
    """
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        anchor_seq, anchor_hash = get_chain_anchor(conn)

        # 幂等: 请求区间已被既往压缩覆盖, 无记录可删, 直接返回
        if req.cutoff_seq <= anchor_seq:
            # 安全边界: 已登记 consumer 的最小游标; 一个都没有时为当前最大 seq
            row = conn.execute("SELECT MIN(cursor_seq) AS m FROM consumers").fetchone()
            if row["m"] is not None:
                safe_through_seq = row["m"]
            else:
                row = conn.execute("SELECT MAX(id) AS m FROM events").fetchone()
                safe_through_seq = row["m"] or 0

            result: dict[str, Any] = {
                "dry_run": req.dry_run,
                "from_seq": anchor_seq + 1,
                "through_seq": anchor_seq,  # from > through 表示空区间
                "safe_through_seq": safe_through_seq,
            }
            if req.dry_run:
                return {"status": "dry_run", **result, "would_delete": 0}
            return {
                "status": "noop",
                **result,
                "deleted": 0,
                "anchor_seq": anchor_seq,
                "anchor_hash": anchor_hash,
            }

        # 拟归档前缀: (锚点, cutoff_seq] 内的现存事件, 按 seq 升序
        rows = conn.execute(
            "SELECT id, event_id, occurred_at, payload, received_at, prev_hash, hash"
            " FROM events WHERE id > ? AND id <= ? ORDER BY id",
            (anchor_seq, req.cutoff_seq),
        ).fetchall()
        # 归档前必须与 /integrity 同口径核对整条拟删前缀; 首个断点即拒绝归档,
        # 否则把被篡改记录的(错误)hash 写入锚点会销毁 /integrity 赖以检出的证据
        _checked, first_break = verify_chain_prefix(rows, anchor_seq, anchor_hash)
        if first_break is not None:
            return JSONResponse(
                status_code=409,
                content={
                    "detail": "chain verification failed within compact prefix",
                    "cutoff_seq": req.cutoff_seq,
                    "dry_run": req.dry_run,
                    "first_break": first_break,
                },
            )

        # 安全边界: 已登记 consumer 的最小游标; 一个都没有时为当前最大 seq
        row = conn.execute("SELECT MIN(cursor_seq) AS m FROM consumers").fetchone()
        if row["m"] is not None:
            safe_through_seq = row["m"]
        else:
            row = conn.execute("SELECT MAX(id) AS m FROM events").fetchone()
            safe_through_seq = row["m"] or 0

        if req.cutoff_seq > safe_through_seq:
            return JSONResponse(
                status_code=409,
                content={
                    "detail": "cutoff_seq exceeds safe retention boundary",
                    "cutoff_seq": req.cutoff_seq,
                    "safe_through_seq": safe_through_seq,
                },
            )

        # 校验通过: 取回的行即从锚点后一条起的连续前缀
        prefix = rows
        from_seq = anchor_seq + 1
        through_seq = prefix[-1]["id"] if prefix else anchor_seq

        if req.dry_run:
            # 只返回预计删除数与边界, 不写库
            return {
                "status": "dry_run",
                "dry_run": True,
                "would_delete": len(prefix),
                "from_seq": from_seq,
                "through_seq": through_seq,
                "safe_through_seq": safe_through_seq,
            }

        if prefix:
            # 同一事务内: 先按被删事件扣减日计数视图(空桶移除), 再物理删除
            for r in prefix:
                remove_event_from_views(
                    conn,
                    occurred_at=r["occurred_at"],
                    payload=json.loads(r["payload"]),
                )
            conn.execute(
                "DELETE FROM events WHERE id BETWEEN ? AND ?",
                (from_seq, through_seq),
            )
            # 刻意不删 received_event_ids: 归档只移除事件内容, event_id 占用持续保留,
            # 同一 ID 重投(单条/批量)仍在写事务内被判为 duplicate
            # 持久化链锚点: 最后一条被删记录的 seq/hash
            anchor_seq, anchor_hash = through_seq, prefix[-1]["hash"]
            conn.execute(
                "INSERT INTO chain_anchor (id, anchor_seq, anchor_hash) VALUES (1, ?, ?)"
                " ON CONFLICT(id) DO UPDATE SET anchor_seq = excluded.anchor_seq,"
                " anchor_hash = excluded.anchor_hash",
                (anchor_seq, anchor_hash),
            )

        return {
            "status": "compacted" if prefix else "noop",
            "dry_run": False,
            "deleted": len(prefix),
            "from_seq": from_seq,
            "through_seq": through_seq,
            "anchor_seq": anchor_seq,
            "anchor_hash": anchor_hash,
            "safe_through_seq": safe_through_seq,
        }


def _chain_break(checked: int, seq: int, event_id: str, reason: str) -> dict[str, Any]:
    return {
        "verified": False,
        "checked": checked,
        "first_break": {"seq": seq, "event_id": event_id, "reason": reason},
    }


def verify_chain_prefix(
    rows: Sequence[sqlite3.Row], anchor_seq: int, anchor_hash: str
) -> tuple[int, Optional[dict[str, Any]]]:
    """从给定锚点出发, 按现有哈希规范逐条校验 rows(须按 seq 升序)。

    校验项与 /integrity 完全一致、按同样顺序:
      1. 序号连续性: 首条须为 anchor_seq + 1, 其后相邻 seq 逐一递增(seq_gap);
      2. 前后哈希衔接: prev_hash 等于上一条的 hash, 首条等于锚点哈希
         (prev_hash_mismatch);
      3. 内容哈希: 按固定 JSON 规范重算并与存储的 hash 比对(hash_mismatch)。
    返回 (断点前已通过校验的记录数, 首个断点); 全部通过时断点为 None。
    /integrity 与 /retention/compact 共用本函数, 保证两处取值与判定口径一致。
    """
    checked = 0
    prev_seq = anchor_seq
    prev_hash = anchor_hash
    for row in rows:
        seq = row["id"]
        if seq != prev_seq + 1:
            return checked, {"seq": seq, "event_id": row["event_id"], "reason": "seq_gap"}
        if row["prev_hash"] is None or row["prev_hash"] != prev_hash:
            return checked, {
                "seq": seq,
                "event_id": row["event_id"],
                "reason": "prev_hash_mismatch",
            }
        if row["hash"] is None or row["hash"] != record_hash(
            seq=seq,
            event_id=row["event_id"],
            occurred_at=row["occurred_at"],
            payload=json.loads(row["payload"]),
            received_at=row["received_at"],
            prev_hash=row["prev_hash"],
        ):
            return checked, {
                "seq": seq,
                "event_id": row["event_id"],
                "reason": "hash_mismatch",
            }
        checked += 1
        prev_seq = seq
        prev_hash = row["hash"]
    return checked, None


@app.get("/integrity")
def integrity():
    """从链锚点开始校验哈希链: 序号连续性、前后哈希衔接、记录内容哈希。

    压缩归档后, 锚点(最后被删记录的 seq/hash)即校验起点; 全部事件已压缩时
    没有待校验记录, 仍返回 verified=true。
    """
    with get_conn() as conn:
        anchor_seq, anchor_hash = get_chain_anchor(conn)
        rows = conn.execute(
            "SELECT id, event_id, occurred_at, payload, received_at, prev_hash, hash"
            " FROM events WHERE id > ? ORDER BY id",
            (anchor_seq,),
        ).fetchall()

    checked, first_break = verify_chain_prefix(rows, anchor_seq, anchor_hash)
    if first_break is not None:
        return _chain_break(
            checked, first_break["seq"], first_break["event_id"], first_break["reason"]
        )

    return {
        "verified": True,
        "checked": checked,
        "first_break": None,
        "anchor_seq": anchor_seq,
    }


# ---------- exports: 持久化快照导出 ----------

@app.post("/exports", status_code=201)
def create_export():
    """创建持久化快照: 同一事务内复制当前全部未压缩事件及链锚点。

    BEGIN IMMEDIATE 与事件写入(/events)、压缩归档(/retention/compact)互斥,
    保证副本与元数据(锚点/末尾序号/总数)来自同一时刻。副本落库后不再变化:
    后续写入、原事件被压缩归档、服务重启都不影响已建快照的分页结果。
    事件表为空时为空快照: end_seq == anchor_seq, total = 0。
    """
    created_at = utc_now()
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        anchor_seq, anchor_hash = get_chain_anchor(conn)
        row = conn.execute("SELECT COUNT(*) AS c, MAX(id) AS m FROM events").fetchone()
        total = row["c"]
        # 空事件表: 末尾序号等于锚点序号
        end_seq = row["m"] if row["m"] is not None else anchor_seq
        cur = conn.execute(
            "INSERT INTO export_snapshots (anchor_seq, anchor_hash, end_seq, total,"
            " created_at) VALUES (?, ?, ?, ?, ?)",
            (anchor_seq, anchor_hash, end_seq, total, created_at),
        )
        export_id = cur.lastrowid
        # 同一事务内整体复制当前全部未压缩事件(含链字段 prev_hash/hash)
        conn.execute(
            "INSERT INTO export_events (snapshot_id, seq, event_id, occurred_at,"
            " payload, received_at, prev_hash, hash)"
            " SELECT ?, id, event_id, occurred_at, payload, received_at, prev_hash,"
            " hash FROM events ORDER BY id",
            (export_id,),
        )
    return {
        "export_id": export_id,
        "anchor_seq": anchor_seq,
        "anchor_hash": anchor_hash,
        "end_seq": end_seq,
        "total": total,
        "created_at": created_at,
    }


@app.get("/exports/{export_id}/events")
def list_export_events(
    export_id: int = Path(...),
    after_seq: Optional[int] = Query(
        None, description="起始序号(不含), 默认为该快照的锚点序号"
    ),
    limit: int = Query(100, ge=1, le=1000),
):
    """按原 seq 升序分页读取快照副本, 返回完整事件(含 prev_hash/hash)。

    - after_seq 缺省为快照锚点序号, 即从副本第一条开始; 必须落在
      [anchor_seq, end_seq] 内, 否则 422(响应附锚点与末尾序号)。
    - next_after_seq 为下一页的起始序号, 末页为 null; 空快照始终返回空页。
    - 只读副本: 与 events 表无关, 后续写入/压缩/重启均不改变结果。
    """
    with get_conn() as conn:
        snap = conn.execute(
            "SELECT id, anchor_seq, end_seq FROM export_snapshots WHERE id = ?",
            (export_id,),
        ).fetchone()
        # 未知或已删除的快照统一 404
        if snap is None:
            return JSONResponse(status_code=404, content={"detail": "export not found"})

        start = snap["anchor_seq"] if after_seq is None else after_seq
        if start < snap["anchor_seq"] or start > snap["end_seq"]:
            return JSONResponse(
                status_code=422,
                content={
                    "detail": "after_seq out of snapshot range",
                    "after_seq": start,
                    "anchor_seq": snap["anchor_seq"],
                    "end_seq": snap["end_seq"],
                },
            )

        # 多取一条判断是否还有下一页
        rows = conn.execute(
            "SELECT seq, event_id, occurred_at, payload, received_at, prev_hash, hash"
            " FROM export_events WHERE snapshot_id = ? AND seq > ?"
            " ORDER BY seq LIMIT ?",
            (export_id, start, limit + 1),
        ).fetchall()

    has_more = len(rows) > limit
    rows = rows[:limit]
    return {
        "export_id": export_id,
        "after_seq": start,
        "limit": limit,
        "count": len(rows),
        "events": [row_to_export_event(r) for r in rows],
        # 末页(含空页)为 null
        "next_after_seq": rows[-1]["seq"] if has_more else None,
    }


@app.get("/exports/{export_id}/field-coverage")
def export_field_coverage(
    export_id: int = Path(...),
    after_path: Optional[str] = Query(
        None,
        description="分页游标(不含), 仅返回路径字典序大于它的项; 缺省从头(含根路径 '')",
    ),
    limit: int = Query(100, ge=1, le=1000, description="每页路径数, 默认 100, 范围 1~1000"),
):
    """读取快照副本的字段覆盖画像: 按 payload 路径统计出现/缺失事件数与各 JSON 类型事件数。

    用于排查事件载荷格式不一致(某字段时有时无、同一路径出现多种类型)。全部统计
    只来自 export_events 快照副本, 不读不写 events 表, 也不返回任何具体字段值。

    路径统计在首次查询时基于副本**一次性聚合并持久化**(含本功能上线前已存在的
    快照); 聚合结果与快照删除共用同一把写锁, 并发首次查询只有一个真正计算,
    其余持锁后复用, 不会产生重复或半成品结果。之后翻页按路径游标只读取该页
    (SQLite 主键范围扫描), 延迟与快照事件数无关; 服务重启后统计仍可复用。

    - 递归包含根、对象、数组与标量: 根路径为空串 ""; 对象键按 JSON Pointer
      风格编码(~、/、* 分别转义为 ~0、~1、~2); 数组下标统一为 "*"。
    - 同一事件内同一路径只计一次出现, 同一(路径, 类型)也只计一次; 混合数组
      可使各类型计数之和超过出现数; null 计为出现; 空数组不产生元素路径。
    - 按路径字典序分页: 只取严格大于 after_path 的项, 缺省从头; next_after_path
      为下一页游标, 末页为 null。空快照首次查询只持久化 total=0 的统计头,
      始终返回空列表。
    - 快照未知或已删除返回 404; limit 越界或 after_path 不是合法路径编码返回 422。
    """
    cursor = "" if after_path is None else after_path

    with get_conn() as conn:
        # 未知或已删除的快照统一 404(先于游标校验, 保持原有边界)
        snap = conn.execute(
            "SELECT id FROM export_snapshots WHERE id = ?",
            (export_id,),
        ).fetchone()
        if snap is None:
            return JSONResponse(status_code=404, content={"detail": "export not found"})

        if not valid_field_path(cursor):
            return JSONResponse(
                status_code=422,
                content={"detail": "after_path is not a valid encoded path", "after_path": cursor},
            )

        # 首次查询在此一次性聚合并原子落库; 已生成则只做一次主键命中查询
        header = ensure_field_coverage(conn, export_id)
        if header is None:
            # 等待写锁期间快照被并发删除
            return JSONResponse(status_code=404, content={"detail": "export not found"})
        total = header["total_events"]

        if after_path is None:
            # 缺省从头: 含根路径 ""
            rows = conn.execute(
                "SELECT path, present_events, t_object, t_array, t_string, t_number,"
                " t_boolean, t_null FROM export_field_coverage_paths"
                " WHERE snapshot_id = ? ORDER BY path LIMIT ?",
                (export_id, limit + 1),
            ).fetchall()
        else:
            # 显式游标: 只取严格大于游标的路径; 显式 after_path="" 与缺省不同,
            # 会跳过根路径
            rows = conn.execute(
                "SELECT path, present_events, t_object, t_array, t_string, t_number,"
                " t_boolean, t_null FROM export_field_coverage_paths"
                " WHERE snapshot_id = ? AND path > ? ORDER BY path LIMIT ?",
                (export_id, cursor, limit + 1),
            ).fetchall()

    # 多取一条判断是否还有下一页
    has_more = len(rows) > limit
    rows = rows[:limit]

    type_columns = FIELD_TYPE_COLUMNS
    items: list[dict[str, Any]] = []
    for row in rows:
        # 只列出计数 > 0 的类型; 顺序固定为 FIELD_TYPES
        types = {
            t: row[col]
            for t, col in zip(FIELD_TYPES, type_columns)
            if row[col] > 0
        }
        present = row["present_events"]
        items.append(
            {
                "path": row["path"],
                "present_events": present,
                "missing_events": total - present,
                "type_events": types,
            }
        )

    return {
        "export_id": export_id,
        "total": total,
        "after_path": cursor,
        "limit": limit,
        "items": items,
        # 末页(含空页、空快照)为 null
        "next_after_path": rows[-1]["path"] if has_more else None,
    }


@app.delete("/exports/{export_id}")
def delete_export(export_id: int = Path(...)):
    """删除快照: 同一事务内原子删除元数据、全部事件副本与字段覆盖统计。

    未知或已删除的快照统一返回 404, 状态不变。
    """
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        snap = conn.execute(
            "SELECT id FROM export_snapshots WHERE id = ?", (export_id,)
        ).fetchone()
        # 未知或已删除统一 404
        if snap is None:
            return JSONResponse(status_code=404, content={"detail": "export not found"})
        # 子表先删: 事件副本、画像路径行与统计头同事务清除, 不留孤儿统计
        conn.execute("DELETE FROM export_events WHERE snapshot_id = ?", (export_id,))
        conn.execute(
            "DELETE FROM export_field_coverage_paths WHERE snapshot_id = ?",
            (export_id,),
        )
        conn.execute(
            "DELETE FROM export_field_coverage WHERE snapshot_id = ?",
            (export_id,),
        )
        conn.execute("DELETE FROM export_snapshots WHERE id = ?", (export_id,))
    return {"export_id": export_id, "deleted": True}


# ---------- views: 事件日计数视图 ----------

@app.post("/views", status_code=201)
def create_view(view: ViewIn):
    """创建事件日计数视图: 按 occurred_at 的 UTC 日期分桶, 只统计当前 events 表内匹配的事件。

    - filters 为 payload 顶层字符串字段的等值条件(全部满足才命中); 空对象表示
      不过滤, 统计全部事件; 非字符串值(数字/布尔/嵌套对象)不匹配字符串条件。
    - 创建在同一写事务(BEGIN IMMEDIATE)内回填现存事件并保存规则; 之后
      /events 与 /events/batch 只对真正新建的事件增量更新(重复 event_id 不计数),
      /retention/compact 删除事件时在同一事务扣减(空桶移除), 迟到事件归入其发生日期。
    - 视图名唯一: 重名返回 409, 不覆盖已有视图。
    """
    filters = view.filters
    # 固定键序存储, 同一规则序列化结果稳定
    filters_json = json.dumps(filters, ensure_ascii=False, sort_keys=True)
    created_at = utc_now()
    counts: dict[str, int] = {}
    try:
        with get_conn() as conn:
            # BEGIN IMMEDIATE 提前获取写锁, 保证"保存规则 -> 回填现存事件"原子完成
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                "INSERT INTO count_views (name, filters, created_at) VALUES (?, ?, ?)",
                (view.name, filters_json, created_at),
            )
            view_id = cur.lastrowid
            # 同一事务内回填: 只统计当前 events 表内匹配的现存事件, 按 UTC 日期分桶
            for row in conn.execute(
                "SELECT occurred_at, payload FROM events"
            ).fetchall():
                if view_matches(filters, json.loads(row["payload"])):
                    day = row["occurred_at"][:10]
                    counts[day] = counts.get(day, 0) + 1
            for day in sorted(counts):
                conn.execute(
                    "INSERT INTO count_view_buckets (view_id, day, count)"
                    " VALUES (?, ?, ?)",
                    (view_id, day, counts[day]),
                )
    except sqlite3.IntegrityError:
        # 视图名已存在 -> 冲突, 不覆盖已有视图
        return JSONResponse(
            status_code=409,
            content={"detail": "view name already exists", "name": view.name},
        )
    return {
        "name": view.name,
        "filters": filters,
        "created_at": created_at,
        "days": [{"day": day, "count": counts[day]} for day in sorted(counts)],
        "total": sum(counts.values()),
    }


@app.get("/views/{name}")
def get_view(name: str = Path(..., min_length=1, max_length=255)):
    """按名称查询日计数视图: 返回规则、各 UTC 日期计数(升序)及总数。未知名称返回 404。"""
    with get_conn() as conn:
        view = conn.execute(
            "SELECT id, name, filters, created_at FROM count_views WHERE name = ?",
            (name,),
        ).fetchone()
        if view is None:
            return JSONResponse(status_code=404, content={"detail": "view not found"})
        rows = conn.execute(
            "SELECT day, count FROM count_view_buckets WHERE view_id = ? ORDER BY day",
            (view["id"],),
        ).fetchall()
    days = [{"day": r["day"], "count": r["count"]} for r in rows]
    return {
        "name": view["name"],
        "filters": json.loads(view["filters"]),
        "created_at": view["created_at"],
        "days": days,
        "total": sum(d["count"] for d in days),
    }


@app.delete("/views/{name}")
def delete_view(name: str = Path(..., min_length=1, max_length=255)):
    """删除日计数视图: 同一事务内原子删除规则与全部日桶。未知名称返回 404, 状态不变。"""
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        view = conn.execute(
            "SELECT id FROM count_views WHERE name = ?", (name,)
        ).fetchone()
        if view is None:
            return JSONResponse(status_code=404, content={"detail": "view not found"})
        conn.execute("DELETE FROM count_view_buckets WHERE view_id = ?", (view["id"],))
        conn.execute("DELETE FROM count_views WHERE id = ?", (view["id"],))
    return {"name": name, "deleted": True}


# ---------- relations: 事件因果(唯一父事件) ----------

def relation_parent_of(conn: sqlite3.Connection, event_id: str) -> Optional[str]:
    """读取事件当前登记的父事件; 未登记返回 None。"""
    row = conn.execute(
        "SELECT parent_id FROM event_relations WHERE event_id = ?",
        (event_id,),
    ).fetchone()
    return None if row is None else row["parent_id"]


def ancestors_of(
    conn: sqlite3.Connection,
    event_id: str,
    *,
    start_depth: int = 1,
) -> list[dict[str, Any]]:
    """沿父链上行收集祖先, 按最近父节点到根节点排序, 深度从 start_depth 起。

    关系独立于 events 保存: 遍历只看 event_relations, 即使中间节点已被安全归档
    (物理删除), 仍可继续上行。visited 防止脏数据自环导致的无限循环。
    """
    out: list[dict[str, Any]] = []
    visited = {event_id}
    current = event_id
    depth = start_depth
    while True:
        row = conn.execute(
            "SELECT parent_id FROM event_relations WHERE event_id = ?",
            (current,),
        ).fetchone()
        if row is None:
            break
        parent_id = row["parent_id"]
        if parent_id in visited:
            break
        out.append({"event_id": parent_id, "depth": depth})
        visited.add(parent_id)
        current = parent_id
        depth += 1
    return out


@app.put("/relations/{event_id}/parent")
def set_parent(
    event_id: str = Path(..., min_length=1, max_length=255),
    req: ParentIn = Body(...),
):
    """为已入库事件设置或更换唯一父事件。

    - 子事件(路径 event_id)与父事件(parent_id)必须是**当前仍存在于 events 表**
      的不同事件: 任一缺失(含已被安全归档删除)返回 404, 父子相同返回 409;
    - 同一写事务(BEGIN IMMEDIATE)内沿现存父链检查: 若变更会形成环
      (从新父事件沿父链上行能到达子事件, 含父子相同)返回 **409**, 原关系不变;
    - 重复设置同一父节点**幂等**: 已登记的 parent_id 与请求一致时原样成功, 不更新;
    - 关系独立保存于 event_relations, 事件之后被归档删除不影响已登记关系的查询,
      但建立/修改关系始终要求两端事件当时仍存在。
    """
    parent_id = req.parent_id
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        child = conn.execute(
            "SELECT 1 FROM events WHERE event_id = ?", (event_id,)
        ).fetchone()
        if child is None:
            # 未知或已归档的子事件: 不允许新建/修改关系(已归档事件的关系仍可经 GET 查询)
            return JSONResponse(
                status_code=404, content={"detail": "event not found", "event_id": event_id}
            )
        parent = conn.execute(
            "SELECT 1 FROM events WHERE event_id = ?", (parent_id,)
        ).fetchone()
        if parent is None:
            return JSONResponse(
                status_code=404,
                content={"detail": "parent event not found", "event_id": parent_id},
            )
        if parent_id == event_id:
            return lease_conflict("event cannot be its own parent")

        current_parent = relation_parent_of(conn, event_id)
        if current_parent == parent_id:
            # 重复设置同一父节点: 幂等成功, 原关系不变
            return {
                "event_id": event_id,
                "parent_id": parent_id,
                "status": "unchanged",
            }

        # 同一写事务内沿父链检查: 从新父事件上行, 若能到达子事件则新边必然成环
        # (覆盖两类: 子事件是新父事件的现存祖先, 如 b->a 已存在又把 a 指向 b;
        # 以及父子相同, 已在上面提前拦截)。仅看子事件自身祖先是不够的。
        for ancestor in ancestors_of(conn, parent_id):
            if ancestor["event_id"] == event_id:
                return JSONResponse(
                    status_code=409,
                    content={
                        "detail": "relation change would create a cycle",
                        "event_id": event_id,
                        "parent_id": parent_id,
                    },
                )

        # 每个事件唯一父事件: 已存在则更换(parent_id 不同), 不存在则新建
        now = utc_now()
        conn.execute(
            "INSERT INTO event_relations (event_id, parent_id, created_at, updated_at)"
            " VALUES (?, ?, ?, ?)"
            " ON CONFLICT(event_id) DO UPDATE SET parent_id = excluded.parent_id,"
            " updated_at = excluded.updated_at",
            (event_id, parent_id, now, now),
        )
    return {"event_id": event_id, "parent_id": parent_id, "status": "set"}


@app.get("/relations/{event_id}/ancestors")
def get_ancestors(event_id: str = Path(..., min_length=1, max_length=255)):
    """按最近父节点到根节点返回祖先链(含 event_id 与深度, 最近父节点 depth=1)。

    - 未知事件(既不在 events 表、也没有任何已登记关系)返回 **404**;
    - 关系独立于 events: 事件被安全归档删除后, 只要曾登记过关系,
      仍可按 event_id 查询其祖先链(遍历不受中间节点归档影响);
    - 没有登记父事件的现存事件返回空链, 根节点(父链顶端)不在列表中。
    """
    with get_conn() as conn:
        # 已知节点: 仍在 events 表, 或在关系图中出现过(登记过父事件, 或被他人登记为父)。
        # 后者保证安全归档删除后, 已登记关系涉及的 event_id 仍可按 ID 查询而非误报 404。
        exists = conn.execute(
            "SELECT 1 FROM events WHERE event_id = ?"
            " UNION ALL SELECT 1 FROM event_relations WHERE event_id = ?"
            " UNION ALL SELECT 1 FROM event_relations WHERE parent_id = ? LIMIT 1",
            (event_id, event_id, event_id),
        ).fetchone()
        if exists is None:
            return JSONResponse(
                status_code=404, content={"detail": "event not found", "event_id": event_id}
            )
        ancestors = ancestors_of(conn, event_id)

    return {
        "event_id": event_id,
        "count": len(ancestors),
        "ancestors": ancestors,
    }
