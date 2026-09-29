#!/usr/bin/env bash
# 消费流程序号缺口防护修复 —— 端到端调用示例。
#
# 演示: 正常接收事件 -> 库外物理删除中间记录(/integrity 报告 seq_gap)->
#       (A) 新 consumer 首次拉取: 409 + 与 /integrity 一致的 first_break,
#           不建租约/不发令牌/游标不动;
#       (B) 已持有未过期租约的 consumer: 重拉与 ack / ack-prefix 都返回 409,
#           原批次/token/过期时间保持原状(不续租、不发新令牌);
#       (C) 租约过期后续租: 缺口仍在时 409 且不换发新令牌;
#       恢复被删记录(原 seq 原样插回)后:
#       (A) 首次拉取正常成批; (B) 未过期重拉沿用同一 token, ack 成功;
#       (C) 过期续租才换发新 token, 新 token 确认成功。
#       记录恢复后不会再被漏掉(游标从未越过缺口)。
#
# 用法:
#   1) 启动服务:  uvicorn app.main:app --port 8000
#      (或 docker compose up --build)
#   2) 运行示例:  BASE=http://localhost:8000 DB_PATH=events.db \
#                 bash examples/consumer_seq_gap_check.sh
#
# 说明: 仅用 HTTP 接口与 sqlite3 CLI(库外 DELETE/INSERT 模拟中间记录意外缺失
#       与后续恢复; UPDATE 租约过期时间模拟时钟到期)。事件 ID 带时间戳前缀,
#       可对已有数据的库重复运行。
set -euo pipefail

BASE="${BASE:-http://localhost:8000}"
DB="${DB_PATH:-events.db}"   # 必须与服务进程的 DB_PATH 一致(默认 events.db)
P="gap-$(date +%s)"
TS="2026-09-29T13:00"
N=5
GAP=3                        # 稍后物理删除本批 seq 区间内的第 3 条(制造中间缺口)

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
  post /events "{\"event_id\":\"$P-$i\",\"occurred_at\":\"${TS}:0${i}Z\",\"payload\":{\"v\":$i}}" >/dev/null
done
# 本批事件在全局哈希链中占据连续 seq 区间 [BASE_SEQ+1, BASE_SEQ+N]
BASE_SEQ=$(sql "SELECT COALESCE(MIN(id),0)-1 FROM events WHERE event_id LIKE '$P-%';")
FROM=$((BASE_SEQ + 1))
THROUGH=$((BASE_SEQ + N))
MISSING=$((BASE_SEQ + GAP))
echo "本批 seq 区间: $FROM..$THROUGH; 稍后制造缺口: seq=$MISSING($P-$GAP)"

echo '== 2. 完整性基线: verified=true =='
get /integrity

echo "== 3. consumer B / C 在缺口产生前各自租用整批(拿到未过期租约与 token) =="
BATCH=$(curl -sS -X POST "$BASE/consumers/$P-b/pull" -H 'Content-Type: application/json' \
  -d '{"limit":1000,"lease_seconds":300}')
echo "$BATCH" | python3 -m json.tool
B_TOKEN=$(printf '%s' "$BATCH" | python3 -c 'import sys,json;print(json.load(sys.stdin)["lease_token"])')
B_EXPIRES=$(printf '%s' "$BATCH" | python3 -c 'import sys,json;print(json.load(sys.stdin)["lease_expires_at"])')
# C 同样先持有一份未过期租约(第 9 步再把它改过期, 演示过期续租路径)
curl -sS -X POST "$BASE/consumers/$P-c/pull" -H 'Content-Type: application/json' \
  -d '{"limit":1000,"lease_seconds":300}' >/dev/null

echo "== 4. 库外物理删除 seq=$MISSING(先备份整行, 稍后原样恢复) =="
# 备份表在示例库内留存被删行的完整副本(含原 id 与链字段), 恢复时按原 seq 插回
sql "CREATE TABLE IF NOT EXISTS _gap_backup AS SELECT * FROM events WHERE 0;
     DELETE FROM _gap_backup WHERE id=$MISSING;
     INSERT INTO _gap_backup SELECT * FROM events WHERE id=$MISSING;
     DELETE FROM events WHERE id=$MISSING;"

echo "== 5. /integrity 报告 seq_gap, first_break 指向缺口后的第一条现存记录 =="
get /integrity

echo '== 6. (A) 新 consumer 首次拉取: 409 + first_break, 不建租约/不发令牌 =='
post "/consumers/$P-a/pull" '{"limit":1000,"lease_seconds":60}'
echo -n "库内状态(应无租约, cursor=0): "
sql "SELECT 'cursor='||cursor_seq||' token='||COALESCE(lease_token,'NULL')
     FROM consumers WHERE consumer_id='$P-a';"

echo '== 7. (B) 未过期租约重拉: 409, token/过期时间/批次原样不变(不续租) =='
post "/consumers/$P-b/pull" '{"limit":1000,"lease_seconds":1}'
echo -n "库内状态(token 与过期时间应与第 3 步一致): "
sql "SELECT 'token='||lease_token||' expires='||lease_expires_at
     FROM consumers WHERE consumer_id='$P-b';"

echo '== 8. (B) 缺口期间 ack / ack-prefix 一律 409, 游标不推进 =='
post "/consumers/$P-b/ack" "{\"lease_token\":\"$B_TOKEN\"}"
post "/consumers/$P-b/ack-prefix" "{\"lease_token\":\"$B_TOKEN\",\"through_seq\":$FROM}"
echo -n "库内游标(应仍为 0 或缺口前位置, 未越过 seq=$MISSING): "
sql "SELECT 'cursor='||cursor_seq FROM consumers WHERE consumer_id='$P-b';"

echo '== 9. (C) 把 C 的租约改过期; 缺口下过期续租被拒, 不换发新令牌 =='
C_OLD=$(sql "SELECT lease_token FROM consumers WHERE consumer_id='$P-c';")
sql "UPDATE consumers SET lease_expires_at='2000-01-01T00:00:00+00:00'
     WHERE consumer_id='$P-c';"
echo "-- 缺口仍在时过期续租: 409 且不换发新令牌(库内 token 保持 $C_OLD) --"
post "/consumers/$P-c/pull" '{"limit":1000,"lease_seconds":60}'
echo -n "库内 token(应仍是旧 token): "
sql "SELECT lease_token FROM consumers WHERE consumer_id='$P-c';"

echo "== 10. 恢复 seq=$MISSING(按原 seq 完整插回) =="
sql "INSERT INTO events SELECT * FROM _gap_backup WHERE id=$MISSING;
     DELETE FROM _gap_backup WHERE id=$MISSING;"
get /integrity

echo '== 11. (A) 恢复后新 consumer 首次拉取: 完整批次 + 令牌, 可正常确认 =='
post "/consumers/$P-a/pull" '{"limit":1000,"lease_seconds":60}'

echo '== 12. (B) 未过期重拉沿用原 token/过期时间, 同一 token 直接 ack 成功 =='
REPULL=$(curl -sS -X POST "$BASE/consumers/$P-b/pull" -H 'Content-Type: application/json' \
  -d '{"limit":1000,"lease_seconds":300}')
echo "$REPULL" | python3 -c '
import sys,json
b=json.load(sys.stdin)
print("events:", [e["seq"] for e in b["events"]])
print("token 与第 3 步相同:", b["lease_token"] == "'"$B_TOKEN"'")
print("过期时间与第 3 步相同:", b["lease_expires_at"] == "'"$B_EXPIRES"'")'
post "/consumers/$P-b/ack" "{\"lease_token\":\"$B_TOKEN\"}"

echo '== 13. (C) 恢复后过期续租: 同一区间换发新 token, 旧 token 失效 =='
RENEW=$(curl -sS -X POST "$BASE/consumers/$P-c/pull" -H 'Content-Type: application/json' \
  -d '{"limit":1000,"lease_seconds":60}')
C_NEW=$(printf '%s' "$RENEW" | python3 -c 'import sys,json;print(json.load(sys.stdin)["lease_token"])')
echo "$RENEW" | python3 -c '
import sys,json
b=json.load(sys.stdin)
print("events:", [e["seq"] for e in b["events"]])
print("换发新 token:", bool(b["lease_token"]) and b["lease_token"] != "'"$C_OLD"'")'
echo -n '旧 token ack(应 409): '
curl -sS -o /dev/null -w '%{http_code}\n' -X POST "$BASE/consumers/$P-c/ack" \
  -H 'Content-Type: application/json' -d "{\"lease_token\":\"$C_OLD\"}"
echo -n '新 token ack(应 200, cursor 越过整批): '
curl -sS -X POST "$BASE/consumers/$P-c/ack" -H 'Content-Type: application/json' \
  -d "{\"lease_token\":\"$C_NEW\"}"
echo

echo '完成: 三条消费路径在缺口期间均拒绝越过缺口(first_break 与 /integrity 一致),'
echo '      游标与租约保持原状、未发放新令牌; 记录恢复后按原约定正常投递与确认。'
