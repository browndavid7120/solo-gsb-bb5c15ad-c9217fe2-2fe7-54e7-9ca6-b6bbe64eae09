"""事件完整性哈希链: 固定 JSON 规范 + SHA-256。"""
import hashlib
import json
from typing import Any

# 创世记录(第一条)的 prev_hash 约定为空串
GENESIS_HASH = ""


def canonical_json(obj: Any) -> str:
    """固定 JSON 规范: 键排序、紧凑分隔符、非 ASCII 不转义, 保证同内容同哈希。"""
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def record_hash(
    *,
    seq: int,
    event_id: str,
    occurred_at: str,
    payload: Any,
    received_at: str,
    prev_hash: str,
) -> str:
    """按固定规范计算单条记录哈希, 通过 prev_hash 与上一条记录链接。"""
    doc = {
        "seq": seq,
        "event_id": event_id,
        "occurred_at": occurred_at,
        "payload": payload,
        "received_at": received_at,
        "prev_hash": prev_hash,
    }
    return hashlib.sha256(canonical_json(doc).encode("utf-8")).hexdigest()
