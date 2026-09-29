#!/usr/bin/env bash
# 归档前缀校验修复 —— 端到端调用示例。
#
# 演示: 正常接收事件 -> 库外篡改一条待删事件(/integrity 检出 hash_mismatch)->
#       dry_run 与实际归档都返回 409 及首个断点(seq/event_id/reason),
#       事件/视图/锚点保持原状 -> 断点不在拟归档前缀内时干净前缀照常归档、
#       断点仍可检出 -> 已覆盖区间幂等 -> 修复内容后正常归档, /integrity 恢复 verified。
#
# 用法:
#   1) 启动服务:  uvicorn app.main:app --port 8000
#      (或 docker compose up --build)
#   2) 运行示例:  BASE=http://localhost:8000 DB_PATH=events.db \
#                 bash examples/compact_prefix_check.sh
#
# 说明: 仅用 HTTP 接口与只读/直改的 sqlite3 CLI(库外 UPDATE 模拟攻击者直改库文件);
#       事件 ID 带时间戳前缀, 可对已有数据的库重复运行(cutoff_seq 由本批实际 seq 推导)。
set -euo pipefail

BASE="${BASE:-http://localhost:8000}"
DB="${DB_PATH:-events.db}"   # 必须与服务进程的 DB_PATH 一致(默认 events.db)
P="pfx-$(date +%s)"
TS="2026-09-29T12:00"
N=4

post() { curl -sS -w $'\n[%{http_code}]\n' -X POST "$BASE$1" -H 'Content-Type: application/json' -d "$2"; }
get()  { curl -sS -w $'\n[%{http_code}]\n' "$BASE$1"; }

sql() {
  if command -v sqlite3 >/dev/null 2>&1; then
    sqlite3 "$DB" "$1"
  else
    echo "(未找到 sqlite3 CLI, 跳过库内步骤: $1)" >&2
    return 1
  fi
}

echo '== 0. 健康检查 =='
get /health

echo "== 1. 正常接收 $N 条事件($P-1 .. $P-$N) =="
for i in $(seq 1 "$N"); do
  post /events "{\"event_id\":\"$P-$i\",\"occurred_at\":\"${TS}:0${i}Z\",\"payload\":{\"v\":$i}}"
done
# 本批事件在全局哈希链中占据连续 seq 区间 [BASE_SEQ+1, BASE_SEQ+N]
BASE_SEQ=$(sql "SELECT COALESCE(MIN(id),0)-1 FROM events WHERE event_id LIKE '$P-%';")
CUTOFF=$((BASE_SEQ + N))
BREAK=$((BASE_SEQ + 2))   # 本批第 2 条, 稍后篡改它
CLEAN=$((BASE_SEQ + 1))   # 本批第 1 条, 断点之前的干净前缀边界
echo "本批 seq 区间: $((BASE_SEQ + 1))..$CUTOFF"

echo "== 2. consumer 拉取并确认到 $CUTOFF(满足归档安全边界) =="
# 新 consumer 从 seq=1 开始, 循环 拉取->确认 直到游标到达 CUTOFF(兼容库里的历史事件)
while :; do
  BATCH=$(curl -sS -X POST "$BASE/consumers/$P/pull" -H 'Content-Type: application/json' \
    -d '{"limit":1000,"lease_seconds":300}')
  TOKEN=$(printf '%s' "$BATCH" | python3 -c 'import sys,json;print(json.load(sys.stdin)["lease_token"])')
  [ "$TOKEN" = "None" ] && break
  CURSOR=$(curl -sS -X POST "$BASE/consumers/$P/ack" -H 'Content-Type: application/json' \
    -d "{\"lease_token\":\"$TOKEN\"}" | python3 -c 'import sys,json;print(json.load(sys.stdin)["cursor_seq"])')
  [ "$CURSOR" -ge "$CUTOFF" ] && break
done
echo "consumer $P 游标: $CURSOR"

echo '== 3. 完整性基线: verified=true =='
get /integrity

echo "== 4. 库外篡改 seq=$BREAK($P-2) 的 payload(模拟直改库文件) =="
sql "UPDATE events SET payload='{\"v\":666}' WHERE event_id='$P-2';"

echo "== 5. /integrity 在 seq=$BREAK 检出 hash_mismatch =="
get /integrity

echo "== 6. dry_run 归档 cutoff=$CUTOFF: 409 + 断点, 不写库 =="
post /retention/compact "{\"cutoff_seq\":$CUTOFF,\"dry_run\":true}"

echo '== 7. 实际归档: 同样 409, 事件/视图/锚点保持原状 =='
post /retention/compact "{\"cutoff_seq\":$CUTOFF}"
echo "本批事件剩余: $(sql "SELECT COUNT(*) FROM events WHERE event_id LIKE '$P-%';") 条;" \
     "锚点: $(sql "SELECT COALESCE(anchor_seq,0) FROM chain_anchor WHERE id=1;")"

echo "== 8. 断点不在拟归档前缀内: cutoff=$CLEAN 只归档干净前缀 =="
post /retention/compact "{\"cutoff_seq\":$CLEAN}"

echo '== 9. 篡改依旧可检出(归档没有洗白); 已覆盖区间幂等 noop =='
get /integrity
post /retention/compact "{\"cutoff_seq\":$CLEAN}"

echo "== 10. 修复被篡改内容后: 剩余前缀 dry_run -> 执行, /integrity 恢复 verified =="
sql "UPDATE events SET payload='{\"v\":2}' WHERE event_id='$P-2';"
post /retention/compact "{\"cutoff_seq\":$CUTOFF,\"dry_run\":true}"
post /retention/compact "{\"cutoff_seq\":$CUTOFF}"
get /integrity
