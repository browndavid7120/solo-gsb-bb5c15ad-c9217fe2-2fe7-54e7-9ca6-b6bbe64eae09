#!/usr/bin/env bash
# 跨归档 event_id 幂等判重 —— 端到端调用示例。
#
# 演示: 首次接收占用 ID -> consumer 确认 -> 安全归档删除事件内容 ->
#       同一 ID 重投仍判 duplicate(单条 200、批量按序 duplicate/seq null),
#       只有新 ID 才续接哈希链并推进日计数视图。
#
# 用法:
#   1) 启动服务:  uvicorn app.main:app --port 8000
#      (或 docker compose up --build)
#   2) 运行示例:  BASE=http://localhost:8000 bash examples/cross_archive_dedup.sh
set -euo pipefail

BASE="${BASE:-http://localhost:8000}"
CONSUMER="${CONSUMER:-demo-worker}"
TS="2026-09-29T09:00"

post() { curl -sS -w $'\n[%{http_code}]\n' -X POST "$BASE$1" -H 'Content-Type: application/json' -d "$2"; }
put()  { curl -sS -w $'\n[%{http_code}]\n' -X PUT  "$BASE$1" -H 'Content-Type: application/json' -d "$2"; }
get()  { curl -sS -w $'\n[%{http_code}]\n' "$BASE$1"; }

echo '== 0. 健康检查 =='
get /health

echo '== 1. 首次接收 evt-1 / evt-2 (201 created) =='
post /events "{\"event_id\":\"evt-1\",\"occurred_at\":\"${TS}:00Z\",\"payload\":{\"v\":1}}"
post /events "{\"event_id\":\"evt-2\",\"occurred_at\":\"${TS}:01Z\",\"payload\":{\"v\":2}}"

echo '== 2. 归档前重投 evt-1: 200 duplicate(即使 payload 不同) =='
post /events "{\"event_id\":\"evt-1\",\"occurred_at\":\"${TS}:02Z\",\"payload\":{\"v\":999}}"

echo '== 3. 创建全量日计数视图(回填现存事件, total=2) =='
post /views '{"name":"demo-all","filters":{}}'

echo '== 4. consumer 拉取并确认整批(把游标推进到 2, 满足归档安全边界) =='
BATCH=$(curl -sS -X POST "$BASE/consumers/$CONSUMER/pull" \
  -H 'Content-Type: application/json' -d '{"lease_seconds":60}')
TOKEN=$(printf '%s' "$BATCH" | python3 -c 'import sys,json;print(json.load(sys.stdin)["lease_token"])')
post "/consumers/$CONSUMER/ack" "{\"lease_token\":\"$TOKEN\"}"

echo '== 5. 安全归档 seq<=2: 事件内容被物理删除, ID 占用保留 =='
post /retention/compact '{"cutoff_seq":2,"dry_run":true}'
post /retention/compact '{"cutoff_seq":2}'

echo '== 6. 归档后单条重投 evt-1: 仍 200 duplicate, 不分配新 seq =='
post /events "{\"event_id\":\"evt-1\",\"occurred_at\":\"${TS}:10Z\",\"payload\":{\"v\":1000}}"

echo '== 7. 归档后批量重投(dup,new,dup): 按输入顺序判重, 含新事件 201 =='
post /events/batch "[
  {\"event_id\":\"evt-1\",\"occurred_at\":\"${TS}:11Z\",\"payload\":{}},
  {\"event_id\":\"evt-3\",\"occurred_at\":\"${TS}:12Z\",\"payload\":{\"v\":3}},
  {\"event_id\":\"evt-2\",\"occurred_at\":\"${TS}:13Z\",\"payload\":{}}
]"

echo '== 8. 全重复批量: 200, results 中 seq 全为 null =='
post /events/batch "[
  {\"event_id\":\"evt-1\",\"occurred_at\":\"${TS}:14Z\",\"payload\":{}},
  {\"event_id\":\"evt-2\",\"occurred_at\":\"${TS}:15Z\",\"payload\":{}}
]"

echo '== 9. 哈希链从锚点续接, 校验通过; 视图只计新事件(total=1) =='
get /integrity
get /views/demo-all

echo '== 10. 当前列表只剩未归档的 evt-3; 归档 ID 无法重放/建关系 =='
get '/events'
post /replay '{"event_ids":["evt-1","evt-3"]}'
put /relations/evt-3/parent '{"parent_id":"evt-1"}'
