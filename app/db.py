import json
import os
import sqlite3
from contextlib import contextmanager

from .chain import GENESIS_HASH, record_hash

DB_PATH = os.getenv("DB_PATH", "events.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,  -- 首次接收顺序
    event_id    TEXT NOT NULL UNIQUE,               -- 幂等键
    occurred_at TEXT NOT NULL,                      -- 事件发生时间(UTC ISO8601)
    payload     TEXT NOT NULL,                      -- 原始 JSON
    received_at TEXT NOT NULL,                      -- 首次接收时间(UTC ISO8601)
    prev_hash   TEXT,                               -- 上一条记录的哈希(首条为空串)
    hash        TEXT                                -- 本记录哈希(固定 JSON 规范的 SHA-256)
);
CREATE INDEX IF NOT EXISTS idx_events_occurred_at ON events (occurred_at);

-- 跨归档的 event_id 占用登记: 首次接收即在此占用, /retention/compact 只删
-- events 中的事件内容, **不删本表**。因此即使事件已物理归档, 同一 event_id
-- 重投仍被判为 duplicate, 不会再分配新 seq、不会作为新事件重复投递,
-- 按 ID 保存的因果关系也始终指向首次接收的那条记录。
CREATE TABLE IF NOT EXISTS received_event_ids (
    event_id    TEXT PRIMARY KEY,               -- 幂等键: 首次接收后永久占用
    seq         INTEGER NOT NULL,               -- 首次接收时分配的 seq(events.id), 归档后仍可按 ID 追溯
    received_at TEXT NOT NULL                   -- 首次接收时间(UTC ISO8601)
);

CREATE TABLE IF NOT EXISTS consumers (
    consumer_id      TEXT PRIMARY KEY,
    cursor_seq       INTEGER NOT NULL DEFAULT 0,  -- 已确认到的 seq; 下次从 cursor_seq+1 投递(首次为 1)
    lease_token      TEXT,                        -- 当前租约令牌(无租约时为 NULL)
    lease_from_seq   INTEGER,                     -- 租约批次起始 seq
    lease_to_seq     INTEGER,                     -- 租约批次末尾 seq
    lease_expires_at TEXT                         -- 租约过期时间(UTC ISO8601)
);

CREATE TABLE IF NOT EXISTS chain_anchor (
    id          INTEGER PRIMARY KEY CHECK (id = 1),  -- 恒为 1 的单行表
    anchor_seq  INTEGER NOT NULL,                    -- 已压缩归档到的最大 seq
    anchor_hash TEXT NOT NULL                        -- 最后被压缩记录的哈希, 作为后续校验的 prev_hash 起点
);

CREATE TABLE IF NOT EXISTS quarantined_events (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,  -- 隔离记录顺序
    consumer_id    TEXT NOT NULL,                      -- 执行隔离的 consumer
    event_seq      INTEGER NOT NULL,                   -- 原事件首次接收顺序号(events.id)
    event_id       TEXT NOT NULL,                      -- 以下 5 列复制自原事件的完整内容
    occurred_at    TEXT NOT NULL,
    payload        TEXT NOT NULL,                      -- 原始 JSON 的独立副本
    received_at    TEXT NOT NULL,
    event_hash     TEXT,                               -- 原事件哈希, 原记录归档后仍可核对
    reason         TEXT NOT NULL,                      -- 非空失败原因
    quarantined_at TEXT NOT NULL,                      -- 隔离时间(UTC ISO8601)
    lease_token    TEXT NOT NULL                       -- 隔离时使用的租约令牌(审计留痕)
);
-- 按 consumer 时间线查询隔离记录; 同一 consumer 不会重复隔离同一 seq(游标已越过, 由消费流程保证)
CREATE INDEX IF NOT EXISTS idx_quarantine_consumer
    ON quarantined_events (consumer_id, id);

CREATE TABLE IF NOT EXISTS export_snapshots (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,  -- 快照 ID
    anchor_seq  INTEGER NOT NULL,                   -- 快照时刻的链锚点 seq
    anchor_hash TEXT NOT NULL,                      -- 快照时刻的链锚点哈希
    end_seq     INTEGER NOT NULL,                   -- 快照时刻最大事件 seq(空快照等于 anchor_seq)
    total       INTEGER NOT NULL,                   -- 副本事件总数
    created_at  TEXT NOT NULL                       -- 快照创建时间(UTC ISO8601)
);

CREATE TABLE IF NOT EXISTS export_events (
    snapshot_id INTEGER NOT NULL,                   -- 所属快照(随快照元数据同事务删除)
    seq         INTEGER NOT NULL,                   -- 原 events.id(首次接收顺序)
    event_id    TEXT NOT NULL,                      -- 以下字段复制自原事件的完整内容
    occurred_at TEXT NOT NULL,
    payload     TEXT NOT NULL,                      -- 原始 JSON 的独立副本
    received_at TEXT NOT NULL,
    prev_hash   TEXT,                               -- 复制时的链字段, 原事件归档后仍可核对
    hash        TEXT,
    PRIMARY KEY (snapshot_id, seq)                  -- 按原 seq 升序分页
);

-- 快照字段覆盖画像的持久化结果: 首次查询时基于 export_events 副本一次性计算,
-- 与路径统计同事务原子落库(行存在即代表已完成, 无半成品状态)
CREATE TABLE IF NOT EXISTS export_field_coverage (
    snapshot_id    INTEGER PRIMARY KEY,            -- 所属快照(随快照删除同事务清除)
    total_events   INTEGER NOT NULL,               -- 画像口径的事件总数(即快照副本数)
    path_count     INTEGER NOT NULL,               -- 去重后的路径总数(可观测信息; 翻页边界由 LIMIT+1 判定)
    generated_at   TEXT NOT NULL                   -- 统计生成时间(UTC ISO8601)
);

CREATE TABLE IF NOT EXISTS export_field_coverage_paths (
    snapshot_id    INTEGER NOT NULL,               -- 所属快照(随快照删除同事务清除)
    path           TEXT NOT NULL,                  -- 转义后的 payload 路径(根为空串)
    present_events INTEGER NOT NULL,               -- 该路径出现的事件数(同事件内去重)
    t_object       INTEGER NOT NULL,               -- 以下六列为各 JSON 类型的事件数
    t_array        INTEGER NOT NULL,
    t_string       INTEGER NOT NULL,
    t_number       INTEGER NOT NULL,
    t_boolean      INTEGER NOT NULL,
    t_null         INTEGER NOT NULL,
    PRIMARY KEY (snapshot_id, path)
) WITHOUT ROWID;
-- 字典序游标分页即主键扫描, 无需额外索引

CREATE TABLE IF NOT EXISTS count_views (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,  -- 视图 ID
    name       TEXT NOT NULL UNIQUE,               -- 客户端指定的唯一视图名
    filters    TEXT NOT NULL,                      -- JSON 对象: payload 顶层字符串字段等值条件, 空对象表示不过滤
    created_at TEXT NOT NULL                       -- 创建时间(UTC ISO8601)
);

CREATE TABLE IF NOT EXISTS count_view_buckets (
    view_id INTEGER NOT NULL,                      -- 所属视图(随视图删除同事务清除)
    day     TEXT NOT NULL,                         -- UTC 日期 YYYY-MM-DD(按 occurred_at 分桶)
    count   INTEGER NOT NULL,                      -- 该日匹配的现存事件数(空桶即删, 恒 > 0)
    PRIMARY KEY (view_id, day)
);

CREATE TABLE IF NOT EXISTS event_relations (
    event_id   TEXT PRIMARY KEY,                   -- 子事件: 每个事件至多一行(唯一父事件)
    parent_id  TEXT NOT NULL,                      -- 父事件 event_id
    -- 刻意不建指向 events 的外键: 关系独立于事件保存, 事件被安全归档物理删除后关系仍可查询
    created_at TEXT NOT NULL,                      -- 首次设置父事件时间(UTC ISO8601)
    updated_at TEXT NOT NULL                       -- 最近一次更换父事件时间(UTC ISO8601)
);
-- 父链上行按 event_id 走主键即可; 该索引支撑反向查询(查子节点/归档后的存在性核对)
CREATE INDEX IF NOT EXISTS idx_event_relations_parent
    ON event_relations (parent_id);
"""


def get_chain_anchor(conn: sqlite3.Connection) -> tuple[int, str]:
    """读取链锚点(压缩归档删到的最大 seq 及其哈希); 从未压缩时为 (0, 创世空串)。"""
    row = conn.execute(
        "SELECT anchor_seq, anchor_hash FROM chain_anchor WHERE id = 1"
    ).fetchone()
    if row is None:
        return 0, GENESIS_HASH
    return row["anchor_seq"], row["anchor_hash"]


def init_db() -> None:
    with get_conn() as conn:
        conn.executescript(SCHEMA)
        _migrate_chain_columns(conn)
        _backfill_chain(conn)
        _backfill_received_ids(conn)


def _migrate_chain_columns(conn: sqlite3.Connection) -> None:
    """兼容旧库: 缺失链字段时补列。"""
    cols = {row["name"] for row in conn.execute("PRAGMA table_info(events)")}
    if "prev_hash" not in cols:
        conn.execute("ALTER TABLE events ADD COLUMN prev_hash TEXT")
    if "hash" not in cols:
        conn.execute("ALTER TABLE events ADD COLUMN hash TEXT")


def _backfill_chain(conn: sqlite3.Connection) -> None:
    """为历史数据补齐哈希链(按首次接收顺序重算, 已有哈希的记录原样衔接)。

    从链锚点之后开始: 已压缩归档的记录不在表内, 锚点哈希作为续链起点。
    """
    anchor_seq, prev_hash = get_chain_anchor(conn)
    rows = conn.execute(
        "SELECT id, event_id, occurred_at, payload, received_at, hash"
        " FROM events WHERE id > ? ORDER BY id",
        (anchor_seq,),
    ).fetchall()
    for row in rows:
        if row["hash"]:
            prev_hash = row["hash"]
            continue
        h = record_hash(
            seq=row["id"],
            event_id=row["event_id"],
            occurred_at=row["occurred_at"],
            payload=json.loads(row["payload"]),
            received_at=row["received_at"],
            prev_hash=prev_hash,
        )
        conn.execute(
            "UPDATE events SET prev_hash = ?, hash = ? WHERE id = ?",
            (prev_hash, h, row["id"]),
        )
        prev_hash = h


def _backfill_received_ids(conn: sqlite3.Connection) -> None:
    """升级回填: 为当前所有未归档(events 表内)事件补齐 ID 占用登记。

    升级时覆盖当前未归档事件即可; 旧版已被归档删除且无留存 ID 的历史记录
    无法也不要求追溯。INSERT OR IGNORE 幂等: 已登记的首次占用信息原样保留,
    重复执行(含新库启动)无副作用。
    """
    conn.execute(
        "INSERT OR IGNORE INTO received_event_ids (event_id, seq, received_at)"
        " SELECT event_id, id, received_at FROM events"
    )


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    # 画像首次生成等较长写事务持锁期间, 并发写请求(含并发首次查询)排队等待,
    # 而不是直接抛 SQLITE_BUSY; 仍由 BEGIN IMMEDIATE 串行化, 不会产生重复结果
    conn.execute("PRAGMA busy_timeout=10000")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()
