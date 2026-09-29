#!/usr/bin/env bash
# 消费流程序号缺口防护 —— 端到端调用示例。
#
# 演示事件表中间记录意外缺失(/integrity 已报 seq_gap)时:
#   1) 首次拉取遇缺口 -> 409, first_break 与 /integrity 一致, 不建租约、不发令牌;
#      记录以原 seq 恢复后 -> 同请求正常建租、返回完整连续批次;
#   2) 未过期租约重拉遇缺口 -> 409, 同一 token/过期时间/区间与游标不变;
#      ack 与 ack-prefix(through_seq 落在缺口之前)一律拒绝推进;
#      恢复后重拉沿用原 token, ack 正常推进;
#   3) 过期续租遇缺口 -> 409, 不换发新令牌(旧 token 仍按 lease expired 拒绝);
#      恢复后续租才换发新 token;
#   4) 无缺口时: 期内重拉原样返回、过期换令牌、前缀确认等原有约定不变。
#
# 用法:
#   1) 启动服务:  uvicorn app.main:app --port 8000
#      (或 docker compose up --build)
#   2) 运行示例:  BASE=http://localhost:8000 DB_PATH=events.db \
#                 bash examples/consume_seq_gap_check.sh
#
# 说明: 仅用 HTTP 接口与 sqlite3 CLI(库外 DELETE 模拟记录意外缺失、
#       UPDATE 模拟租约过期); 缺失记录先备份到临时表, 恢复时按原 seq 原样插回。
set -euo pipefail

BASE="${BASE:-http://localhost:8000}"
DB="${DB_PATH:-events.db}"   # 必须与服务进程的 DB_PATH 一致(默认 events.db)
P="gap-$(date +%s)"
TS="2026-09-29T13:00"
N=5
CA="c-$P-a"   # 首次缺口 + 期内重拉缺口
CB="c-$P-b"   # 过期续租缺口

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

# 拉取并输出 (token|expires|seqs 空格分隔), 便于断言
pull_parts() {
  curl -sS -X POST "$BASE/consumers/$1/pull" -H 'Content-Type: application/json' \
    -d '{"limit":100,"lease_seconds":300}' \
  | python3 -c 'import sys,json
b=json.load(sys.stdin)
print((b.get("lease_token") or "")+"|"+(b.get("lease_expires_at") or "")+"|"+
      " ".join(str(e["seq"]) for e in b.get("events", [])))'
}

echo '== 0. 健康检查 =='
get /health

echo "== 1. 正常接收 $N 条事件($P-1 .. $P-$N) =="
for i in $(seq 1 "$N"); do
  post /events "{\"event_id\":\"$P-$i\",\"occurred_at\":\"${TS}:0${i}Z\",\"payload\":{\"v\":$i}}" >/dev/null
done
BASE_SEQ=$(sql "SELECT COALESCE(MIN(id),0)-1 FROM events WHERE event_id LIKE '$P-%';")
S1=$((BASE_SEQ + 1)); S2=$((BASE_SEQ + 2)); S3=$((BASE_SEQ + 3))
S4=$((BASE_SEQ + 4)); S5=$((BASE_SEQ + N))
echo "本批 seq 区间: $S1..$S5"

sql "CREATE TABLE IF NOT EXISTS _gap_backup AS SELECT * FROM events WHERE 0;"
hide() { sql "DELETE FROM _gap_backup WHERE id=$1; INSERT INTO _gap_backup SELECT * FROM events WHERE id=$1; DELETE FROM events WHERE id=$1;"; }
restore() { sql "INSERT INTO events SELECT * FROM _gap_backup WHERE id=$1;"; }

# ---------- 场景一: 首次拉取遇缺口 ----------
echo "== 2. [首次拉取] 库外删除 seq=$S2($P-2) =="
hide "$S2"

echo '== 3. /integrity 在缺口后第一条(seq='"$S3"') 报 seq_gap =='
get /integrity

echo '== 4. 首次 pull: 期望 409 + first_break(与 /integrity 一致), 不建租约 =='
post "/consumers/$CA/pull" '{"limit":100,"lease_seconds":300}'
echo "库内租约状态(期望 lease_token 为空):"
sql "SELECT 'cursor='||cursor_seq||' token='||COALESCE(lease_token,'NULL') FROM consumers WHERE consumer_id='$CA';"

echo "== 5. 恢复 seq=$S2 后重新 pull: 期望 200, 完整批次 $S1..$S5 =="
restore "$S2"
BATCH_A=$(pull_parts "$CA")
echo "$BATCH_A"
T1="${BATCH_A%%|*}"
echo "token=$T1"

# ---------- 场景二: 未过期租约重拉遇缺口 ----------
echo "== 6. [期内重拉] 租约生效期间库外删除 seq=$S4($P-4) =="
hide "$S4"
get /integrity

echo '== 7. 期内重复 pull: 期望 409, 游标/租约不变(不续租、不换 token) =='
post "/consumers/$CA/pull" '{"limit":100,"lease_seconds":300}'

echo '== 8. ack 与 ack-prefix(through_seq='"$S1"' 在缺口之前): 均期望 409, 拒绝推进 =='
post "/consumers/$CA/ack" "{\"lease_token\":\"$T1\"}"
post "/consumers/$CA/ack-prefix" "{\"lease_token\":\"$T1\",\"through_seq\":$S1}"
echo "库内游标(期望仍为 0)与 token(期望与租约建立时一致):"
sql "SELECT 'cursor='||cursor_seq||' token='||lease_token FROM consumers WHERE consumer_id='$CA';"

echo "== 9. 恢复 seq=$S4: 期内重拉沿用同一 token/过期时间, ack 推进到 $S5 =="
restore "$S4"
BATCH_A2=$(pull_parts "$CA")
echo "$BATCH_A2"
post "/consumers/$CA/ack" "{\"lease_token\":\"$T1\"}"

# ---------- 场景三: 过期续租遇缺口 ----------
echo '== 10. [过期续租] 第二个 consumer 先正常租用一批 =='
BATCH_B=$(pull_parts "$CB")
T2="${BATCH_B%%|*}"
echo "原 token=$T2"

echo "== 11. 库外删除 seq=$S3, 并把租约过期时间改到过去 =="
hide "$S3"
sql "UPDATE consumers SET lease_expires_at='2000-01-01T00:00:00+00:00' WHERE consumer_id='$CB';"

echo '== 12. 过期后 pull: 期望 409, 不换发新令牌; 旧 token ack 仍是 lease expired =='
post "/consumers/$CB/pull" '{"limit":100,"lease_seconds":300}'
echo "库内 token(期望仍是续租前的旧 token):"
sql "SELECT lease_token FROM consumers WHERE consumer_id='$CB';"
post "/consumers/$CB/ack" "{\"lease_token\":\"$T2\"}"

echo "== 13. 恢复 seq=$S3 后 pull: 期望 200 且换发新 token, 新 token ack 成功 =="
restore "$S3"
BATCH_B2=$(pull_parts "$CB")
echo "$BATCH_B2"
T3="${BATCH_B2%%|*}"
post "/consumers/$CB/ack" "{\"lease_token\":\"$T3\"}"
echo "旧 token 此时已作废:"
post "/consumers/$CB/ack" "{\"lease_token\":\"$T2\"}"

echo '== 14. 收尾: 完整性恢复, 清理临时备份表 =='
get /integrity
sql "DROP TABLE _gap_backup;"
