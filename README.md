# Webhook 事件接收与重放 API

> **修复记录(消费流程序号缺口防护)**:事件表中间记录意外缺失时,完整性校验虽已报告 `seq_gap`,旧消费流程的 `pull` 却用 `LIMIT` 直接取行,会把缺口两侧事件合为一批;`ack` 后游标越过缺口,记录即使恢复也不再投递。现修复为:**首次拉取、未过期租约重拉、过期续租**三条会产出/复用批次的路径,返回前都核对拟返回批次从起点到末尾的序号连续性;遇缺口一律返回 **409** 及与 `/integrity` 一致的 `first_break`(seq/event_id/`reason="seq_gap"`),游标与租约保持原状(不续租、不发放新令牌);`ack` 与 `ack-prefix` 推进前也核对租约区间,缺口仍在时拒绝推进;缺口修复后沿用**原租约时效与令牌**(未过期同 token 直接确认、过期才按原区间换发新 token)。无缺口时批次内容、过期换令牌、前缀确认等请求/响应约定完全不变;归档锚点仍是可投递起点(压缩后新 consumer 从 `anchor_seq+1` 起拉)。可运行示例见 `examples/consumer_seq_gap_check.sh`。
>
> **修复记录(归档前缀校验)**:旧实现推进链锚点前只检查 `seq` 连续,不核对前后哈希衔接与内容哈希;待删事件若已被篡改(原本会被 `/integrity` 检出),归档删除并把其存储哈希写入锚点后,证据随记录消失,`/integrity` 反而返回 `verified=true`。现修复为归档(含 `dry_run`)前从当前锚点按现有哈希规范逐条校验拟删除前缀(序号连续性 → 前后哈希衔接 → 内容哈希):遇首个断点实际归档与 `dry_run` 均返回 **409** 及断点的 `seq`/`event_id`/`reason`(取值沿用 `/integrity`),事件、日计数视图与锚点保持原状;断点不在拟归档前缀内时仍按既有边界归档,已覆盖区间幂等;无断点时响应与 `/integrity` 约定不变。校验逻辑抽为 `/integrity` 与归档共用的 `verify_chain_prefix`,两处口径天然一致。

基于 FastAPI + SQLite 的 Webhook 事件接收服务:按 `event_id` 幂等接收(支持单条与 1~1000 条批量、同一写事务续接哈希链)、记录首次接收顺序,支持时间范围分页查询与批量重放,并可创建按 UTC 日期分桶的事件计数视图。

## 启动

```bash
docker compose up --build
```

一条命令完成构建、数据库初始化与启动;服务监听 **8000** 端口,接口入口为 **http://localhost:8000**(交互式文档在 http://localhost:8000/docs,健康检查 http://localhost:8000/health)。SQLite 数据持久化在 named volume `webhook-data`,数据库表(含隔离记录表、快照导出表、日计数视图表、事件因果关系表、跨归档 `event_id` 占用表)在启动时自动创建/升级(含把当前未归档事件的 ID 回填进占用表),无需手工初始化。

不用 compose 也可以:

```bash
docker build -t webhook-replay . && docker run -p 8000:8000 webhook-replay
```

本地开发:`pip install -r requirements.txt && uvicorn app.main:app --port 8000`

**配置**:仅一个环境变量 `DB_PATH`——SQLite 数据库文件路径,默认 `events.db`;compose 镜像内置为 `/data/events.db`(挂载在 `webhook-data` 卷)。

## 示例 curl

### 1. 接收事件(幂等)

```bash
curl -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"evt-1","occurred_at":"2026-09-18T09:00:00Z","payload":{"a":1}}'
# 首次: 201 {"status":"created","event_id":"evt-1"}
# 重复: 200 {"status":"duplicate","event_id":"evt-1"}  —— 不产生重复记录
```

### 1b. 批量接收事件(整批一个写事务)

`POST /events/batch` 一次接收 **1~1000 条**与单条接口同格式的事件,请求体为 JSON 数组(不是包一层对象)。整批在**同一个写事务**(`BEGIN IMMEDIATE`)内处理,按请求顺序逐条判定 `event_id`:

- **库中已有**、或**本批前面已出现过**的 ID 一律标记 `duplicate`,不写入、也不消耗 `seq`;
- 仅首次出现的 ID 入库,并从当前链头按请求顺序**逐条续接哈希链**,`seq` 连续无空洞;
- 响应 `results` 严格按**输入顺序**返回每条的 `event_id`/`status`/`seq`(`duplicate` 的 `seq` 为 `null`),并汇总 `created`/`duplicate`;
- 只要新建了至少一条就返回 **201**,全部重复为 **200**;
- 任一条格式非法(字段缺失、`event_id` 为空、时间非法等)在事务开始前即被整体拒绝(**422**);写入过程中失败则事务回滚(**409**)——两种情况都**不会留下部分新事件**;
- 与单条接口共享同一套幂等语义和哈希链:批量里的重复投递与单条重发效果完全一致。

```bash
curl -X POST http://localhost:8000/events/batch \
  -H 'Content-Type: application/json' \
  -d '[
    {"event_id":"evt-1","occurred_at":"2026-09-18T09:00:00Z","payload":{"a":1}},
    {"event_id":"evt-2","occurred_at":"2026-09-18T09:01:00Z","payload":{"a":2}},
    {"event_id":"evt-1","occurred_at":"2026-09-18T09:02:00Z","payload":{"a":3}}
  ]'
```

`evt-1` 已在上面的单条示例中创建,因此响应为(注意输入顺序被完整保留,批内第二次出现的 `evt-1` 同样判重):

```json
{
  "created": 1,
  "duplicate": 2,
  "results": [
    {"event_id": "evt-1", "status": "duplicate", "seq": null},
    {"event_id": "evt-2", "status": "created", "seq": 2},
    {"event_id": "evt-1", "status": "duplicate", "seq": null}
  ]
}
```

空数组(`[]`)或超过 1000 条同样返回 **422**;批量提交与单条提交可以任意交错,哈希链始终连续,随时可用 `/integrity` 校验。

### 1c. 跨归档幂等:首次接收的 ID 永久占用

幂等判定**不依赖事件内容是否还在**。每个 `event_id` 在首次接收时,除了写入 `events`,还会在同一写事务内登记到独立的 **ID 占用表 `received_event_ids`**(记录首次 `seq` 与 `received_at`)。`/retention/compact` 归档只物理删除 `events` 里的事件内容,**刻意保留占用登记**:

- 归档后同一 `event_id` 再走 `POST /events` 重投,依旧返回 **200 `duplicate`**——不会重新写入、不会分配新 `seq`、不会推进哈希链;
- 走 `POST /events/batch` 重投时,这些 ID 按原输入顺序标为 `duplicate`、`seq` 为 `null`;整批全是已占用 ID 返回 **200**,含任意新 ID 返回 **201**;
- 只有**从未接收过的新 ID** 才会续接哈希链并增量更新日计数视图;重复投递(无论是否已归档)不回增计数;
- 按 `event_id` 保存的因果关系(第 13 节)因此始终指向首次接收的那条记录,归档后的 ID 重投不会冒出"同 ID 新记录"。

占用登记与事件写入、归档删除在各自的写事务内原子完成,要么一起生效要么一起回滚;服务重启后占用仍在。

```bash
# 1) 接收 evt-1 / evt-2, consumer ack 后整体归档(events 内容被物理删除)
curl -s -X POST http://localhost:8000/retention/compact \
  -H 'Content-Type: application/json' -d '{"cutoff_seq":2}'
# => {"status":"compacted","deleted":2,"anchor_seq":2,...}

# 2) 归档后重投同一 ID: 单条仍是 200 duplicate(内容不同也一样)
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"evt-1","occurred_at":"2026-09-19T00:00:00Z","payload":{"changed":true}}'
# => 200

# 3) 批量重投归档 ID + 夹一个新 ID: 按输入顺序判重, 含新事件返回 201
curl -X POST http://localhost:8000/events/batch \
  -H 'Content-Type: application/json' \
  -d '[
    {"event_id":"evt-1","occurred_at":"2026-09-19T00:01:00Z","payload":{}},
    {"event_id":"evt-3","occurred_at":"2026-09-19T00:02:00Z","payload":{}},
    {"event_id":"evt-2","occurred_at":"2026-09-19T00:03:00Z","payload":{}}
  ]'
```

```json
{
  "created": 1,
  "duplicate": 2,
  "results": [
    {"event_id": "evt-1", "status": "duplicate", "seq": null},
    {"event_id": "evt-3", "status": "created", "seq": 3},
    {"event_id": "evt-2", "status": "duplicate", "seq": null}
  ]
}
```

新事件 `evt-3` 从归档锚点哈希继续接链(`seq` 紧接锚点之后),`/integrity` 仍为 `verified=true`;而 `evt-1`/`evt-2` 不会以新 `seq` 被重新投递。

> 升级说明:新版本启动时自动建表,并把**当前未归档事件**的 `event_id` 一次性回填进占用表(幂等,可重复执行)。旧版本里已经归档删除、且无任何留存信息的历史 ID 无法追溯,这类 ID 再次提交会被当作新事件——属预期,不做历史回填。

### 2. 按时间范围分页查询

```bash
curl 'http://localhost:8000/events?from=2026-09-18T09:30:00Z&to=2026-09-18T11:30:00Z&page=1&page_size=50'
```

返回 `total / page / page_size / items`,每条含 `seq`(首次接收顺序号)、`event_id`、`occurred_at`、`payload`、`received_at`。`from`/`to` 均可省略。

### 3. 批量重放

```bash
curl -X POST http://localhost:8000/replay \
  -H 'Content-Type: application/json' \
  -d '{"event_ids":["evt-3","evt-1","evt-404"]}'
```

```json
{
  "replayed": 2,
  "skipped": 1,
  "results": [
    {"event_id": "evt-1", "status": "replayed", "event": {"seq": 1, "...": "..."}},
    {"event_id": "evt-3", "status": "replayed", "event": {"seq": 2, "...": "..."}},
    {"event_id": "evt-404", "status": "skipped", "reason": "not_found"}
  ]
}
```

命中的事件**按首次接收顺序(`seq`)输出**,与请求顺序无关;未命中的标记 `skipped`;请求中重复的 `event_id` 只输出一次。

### 4. 完整性校验

```bash
curl http://localhost:8000/integrity
```

```json
{"verified": true, "checked": 3, "first_break": null, "anchor_seq": 0}
```

从**链锚点**开始逐条校验哈希链:**序号连续性**(相邻 `seq` 逐一递增,首条须为 `anchor_seq + 1`)、**前后哈希衔接**(每条 `prev_hash` 等于上一条的 `hash`,首条等于锚点哈希——未压缩过时为创世空串)、**事件字段**(按固定 JSON 规范重算哈希比对)。空库返回 `verified=true`、`checked=0`;全部事件被压缩归档后同样返回 `verified=true`、`checked=0`(锚点本身即完整性证明)。

发现篡改时返回首个断点的 `seq`、`event_id` 和原因(`seq_gap` / `prev_hash_mismatch` / `hash_mismatch`),`checked` 为断点前已通过校验的记录数:

```json
{"verified": false, "checked": 1, "first_break": {"seq": 2, "event_id": "evt-2", "reason": "hash_mismatch"}}
```

### 5. 消费游标:拉取并租用事件

每个 consumer 独立维护确认游标,按首次接收顺序(`seq` 从 1 开始)拉取尚未确认的连续事件。`limit`(默认 100,1~1000)与 `lease_seconds`(默认 30,1~86400)均可省略,甚至可以不带 body:

```bash
curl -X POST http://localhost:8000/consumers/worker-1/pull \
  -H 'Content-Type: application/json' \
  -d '{"limit":100,"lease_seconds":30}'
```

```json
{
  "consumer_id": "worker-1",
  "events": [
    {"seq": 1, "event_id": "evt-1", "occurred_at": "...", "payload": {"a": 1}, "received_at": "..."},
    {"seq": 2, "event_id": "evt-2", "occurred_at": "...", "payload": {"a": 2}, "received_at": "..."}
  ],
  "lease_token": "NcueAr-DjIE5Cjwdn0X5OiSxe9CH_XyCkya2G0vp3sk",
  "lease_expires_at": "2026-09-18T19:38:12.919089+00:00"
}
```

- **租约期内重复拉取**:原样返回**同一批次、同一 token、同一过期时间**(不续租),参数里的 `limit`/`lease_seconds` 不影响结果。
- **租约过期后**:同一批次可被重新投递(沿用原区间,换发**新 token**);旧 token 立即作废。
- **空队列**:返回 `{"events":[],"lease_token":null,"lease_expires_at":null}`,**不创建租约**;之后新事件到达可继续拉取。
- 首次出现的 `consumer_id` 自动建档,游标为 0,即首批从 `seq=1` 开始;压缩归档之后才建档的 consumer 游标仍为 0,已归档区间不可投递,首批从 **`anchor_seq + 1`** 开始(归档锚点是可投递起点)。

#### 5b. 拉取前的序号连续性核对(防游标越过缺口)

事件表中间记录意外缺失(如库外删除)时,若直接用 `LIMIT` 取行,缺口两侧的事件会被合为一批,确认后游标就越过了缺口——记录即使恢复也不会再投递。为此三条会产出/复用批次的路径在返回前都先核对**拟返回批次从起点到末尾的序号连续性**:

1. **首次拉取**(无租约):从可投递起点 `max(cursor_seq, anchor_seq) + 1` 取到本批末尾,逐个 `seq` 必须连续;
2. **未过期租约重拉**:核对当前租约区间 `[lease_from_seq, lease_to_seq]`;
3. **过期续租**:换发新令牌前同样核对原租约区间。

任一处发现缺口都返回 **409**,响应给出与 `/integrity` **完全一致的 `first_break`**(缺口后第一条现存记录的 `seq`/`event_id`,`reason` 固定为 `seq_gap`)及当前区间边界;**游标与租约保持原状,不续租、不发放任何新令牌**(响应里没有 `lease_token`):

```json
{"detail": "seq gap detected while leasing next batch",
 "first_break": {"seq": 3, "event_id": "evt-3", "reason": "seq_gap"},
 "lease_from_seq": 1, "lease_to_seq": 5}
```

- 缺口不在本批 `LIMIT` 范围内时,缺口之前的干净批次仍正常投递;消费停在缺口前一条,**任何路径都不会把游标推进过缺口**。
- 缺口修复(被删记录按原 `seq` 恢复)后:首次拉取正常成批发新令牌;未过期租约重拉仍返回**原批次、原 token、原过期时间**(不续租);过期租约才按原区间换发新 token。恢复的记录随批次正常投递,不会漏掉。
- 若缺失的是批次/租约的**末尾记录**,`/integrity` 因缺口之后没有现存记录而不可见(仍 `verified=true`),消费侧仍以首个缺失 `seq`(按 ID 占用表还原 `event_id`)返回同样的 `seq_gap` 409,拒绝越过。
- 无缺口时响应字段与上述批次/令牌约定完全不变。可运行的端到端调用示例见 `examples/consumer_seq_gap_check.sh`。

### 6. 消费游标:确认批次

只能确认**当前未过期租约**的整批事件,确认后游标推进到本批次末尾,下次 `pull` 从下一个 `seq` 开始:

```bash
curl -X POST http://localhost:8000/consumers/worker-1/ack \
  -H 'Content-Type: application/json' \
  -d '{"lease_token":"NcueAr-DjIE5Cjwdn0X5OiSxe9CH_XyCkya2G0vp3sk"}'
```

```json
{"consumer_id": "worker-1", "acknowledged": true, "cursor_seq": 2}
```

以下情况一律返回 **409**(游标保持不变,等待租约过期后重新投递):

- token 与当前租约不一致(**伪造**或**旧 token**——旧批次过期后已被新租约取代);
- 租约**已过期**(过期批次可重投,旧 token 不得再确认);
- 当前没有租约(从未拉取、已确认完毕、或上次拉取是空批次),包括未知 consumer;
- **租约区间存在序号缺口**(如租用期间中间记录意外缺失):返回与 `/integrity` 一致的 `first_break`(`reason="seq_gap"`)与区间边界,游标、租约与 token 全部保持原状——禁止确认后游标越过缺口。缺口修复后用**同一个 token** 重新确认即可(原租约时效不变)。

游标只能经由确认前移(整批 `ack`,或前缀 `ack-prefix`),因此**禁止越序确认**:批次内的任何一条都无法被单独跳过或提前确认。需要处理到一半先确认一部分时,见下一节的前缀确认。

一个典型的拉取-确认循环:

```bash
# 1) 租用
BATCH=$(curl -s -X POST http://localhost:8000/consumers/worker-1/pull \
  -H 'Content-Type: application/json' -d '{"limit":100,"lease_seconds":30}')
TOKEN=$(printf '%s' "$BATCH" | python3 -c 'import sys,json;print(json.load(sys.stdin)["lease_token"])')
# 2) 处理 $BATCH ...
# 3) 确认
curl -X POST http://localhost:8000/consumers/worker-1/ack \
  -H 'Content-Type: application/json' -d "{\"lease_token\":\"$TOKEN\"}"
```

### 7. 消费游标:确认连续前缀

处理到批次中途、希望先确认已完成的部分时,可在**当前未过期租约**内确认从批次起点 `lease_from_seq` 到 `through_seq` 的连续前缀:

```bash
curl -X POST http://localhost:8000/consumers/worker-1/ack-prefix \
  -H 'Content-Type: application/json' \
  -d '{"lease_token":"NcueAr-DjIE5Cjwdn0X5OiSxe9CH_XyCkya2G0vp3sk","through_seq":2}'
```

```json
{"consumer_id": "worker-1", "acknowledged": true, "through_seq": 2, "cursor_seq": 2}
```

确认在**同一个写事务**内完成:校验通过后游标推进到 `through_seq` 并**清除原租约**,旧 token 立即作废。下一次 `pull` 会从 `through_seq + 1` 起从未确认的首条**重新租用**(换发新 token);前缀确认的 `[lease_from_seq, through_seq]` 不会再次投递。

- `through_seq` 必须落在当前租约区间 `[lease_from_seq, lease_to_seq]` 内;因为前缀必须从批次起点连续,**无法跳过中间事件**单独确认后面的某一条。
- `through_seq` 等于批次末尾 `lease_to_seq` 时**等同整批确认**,效果与 `ack` 完全一致。
- 确认后再用同一 token 调 `ack` 或 `ack-prefix` 都会得到 **409**(租约已清除);需要继续确认剩余事件,请先重新 `pull`。

以下情况一律返回 **409 且状态不变**(游标与租约均不改动):

- **未知 consumer**,或当前没有租约(从未拉取、已确认完毕、或上次拉取是空批次);
- token 与当前租约不一致(**伪造**或**旧 token**);
- 租约**已过期**(过期批次可重投,旧 token 不得再确认);
- `through_seq` **越界**:早于批次起点(响应 `through_seq before batch start`)或晚于批次末尾(`through_seq beyond batch end`),响应中同时给出当前租约的 `lease_from_seq`/`lease_to_seq`;
- **租约区间存在序号缺口**:与整批 `ack` 同一套核对——即使缺口位于 `through_seq` 之后的批次其余部分,也返回与 `/integrity` 一致的 `first_break`(`reason="seq_gap"`)拒绝推进。缺口修复后沿用同一 token 与原租约时效确认前缀即可。

```json
{"detail": "through_seq beyond batch end", "through_seq": 9,
 "lease_from_seq": 1, "lease_to_seq": 2}
```

典型用法:租一大批、边处理边确认前缀,租约快到期时已确认的部分不会在重投中重复出现:

```bash
# 1) 租用一批, 处理完前 50 条
# 2) 确认连续前缀
curl -X POST http://localhost:8000/consumers/worker-1/ack-prefix \
  -H 'Content-Type: application/json' \
  -d "{\"lease_token\":\"$TOKEN\",\"through_seq\":50}"
# 3) 再次 pull: 从 seq=51 开始租用未确认事件
curl -X POST http://localhost:8000/consumers/worker-1/pull \
  -H 'Content-Type: application/json' -d '{"limit":100,"lease_seconds":30}'
```

### 8. 消费游标:回退重投

处理逻辑出错或需要重放历史时,可把**已登记** consumer 的游标回退到 `target_seq`,下一次 `pull` 从 `target_seq + 1` 重新投递:

```bash
curl -X POST http://localhost:8000/consumers/worker-1/rewind \
  -H 'Content-Type: application/json' \
  -d '{"target_seq":42}'
```

```json
{"consumer_id": "worker-1", "previous_cursor_seq": 100, "cursor_seq": 42, "next_seq": 43}
```

- **回退区间**:`target_seq` 必须落在 `[anchor_seq, cursor_seq]`(链锚点 ~ 当前游标)。低于锚点(对应事件已压缩归档、无法重投)或大于当前游标(**向前推进只能经由 `ack` 整批确认或 `ack-prefix` 前缀确认**)一律返回 **409** 且状态不变;`target_seq` 等于当前游标时为幂等空操作。
- **租约**:存在**未过期租约**时返回 **409**(回退会与在途批次冲突);**已过期租约**在同一事务内清除后再回退,旧 token 随之作废,后续 `pull` 不会重投旧批次区间。
- **未知 consumer** 返回 **409**,不会像 `pull` 那样自动建档。
- **并发安全**:回退与 `pull`/`ack`/`ack-prefix`/`compact` 在同一写锁下串行执行,并发下只有单一结果;回退后游标不低于链锚点,`compact` 的安全边界也随之收紧,绝不会产生指向已压缩事件的租约。

```json
{"detail": "target_seq below chain anchor", "target_seq": 3, "anchor_seq": 4}
```

### 9. 消费游标:隔离毒丸事件

批次首条事件反复处理失败(毒丸)时,可只隔离这一条、越过它继续消费剩余事件,而不必回退或卡住整个批次:

```bash
curl -X POST http://localhost:8000/consumers/worker-1/quarantine \
  -H 'Content-Type: application/json' \
  -d '{"lease_token":"NcueAr-DjIE5Cjwdn0X5OiSxe9CH_XyCkya2G0vp3sk","reason":"JSON 解析失败: 缺少必填字段 order_id"}'
```

```json
{
  "consumer_id": "worker-1",
  "quarantined": true,
  "quarantine_id": 1,
  "seq": 1,
  "event_id": "evt-1",
  "cursor_seq": 1,
  "quarantined_at": "2026-09-26T12:00:00.000000+00:00"
}
```

全部操作在**单个写事务**(`BEGIN IMMEDIATE`)内完成:

1. **核对令牌**:必须存在与 `lease_token` 一致且**未过期**的租约;无租约、租约过期、伪造或旧令牌一律 **409 且状态不变**(与 `ack` 同一套校验)。
2. **核对租约起点与游标相邻**:`lease_from_seq` 必须等于下一条可投递事件(正常情况下为 `cursor_seq + 1`;压缩归档后才建档的 consumer 游标仍为 0,已归档区间不可投递,起点为 `anchor_seq + 1`)。不相邻返回 409 `lease start not adjacent to cursor`(响应附 `cursor_seq`/`anchor_seq`/`lease_from_seq`/`lease_to_seq`),状态不变。
3. **复制隔离记录**:把首条事件的**完整内容**(`seq`/`event_id`/`occurred_at`/`payload`/`received_at` 及原事件 `hash`)连同非空 `reason`、隔离时间、所用令牌写入**独立隔离表** `quarantined_events`(不修改、不删除原事件)。
4. **游标仅越过这一条**:`cursor_seq` 推进到 `lease_from_seq`,同时**清除整批租约**。下一次 `pull` 从该 seq 之后**重新租用剩余事件**(换发新 token);原批次的旧令牌立即作废,再用它调 `ack`/`ack-prefix`/`quarantine` 都是 409。

- **只能隔离首条**:不允许跳过批次中间的事件(与"禁止越序确认"一致);失败原因为空或纯空白由请求校验拦截(**422**)。
- **单事件批次**:隔离后租约清除,下次 `pull` 返回空批次,直到新事件到达。

查询某 consumer 的隔离记录(按隔离时间分页):

```bash
curl 'http://localhost:8000/consumers/worker-1/quarantine?page=1&page_size=50'
```

```json
{
  "consumer_id": "worker-1",
  "total": 1,
  "page": 1,
  "page_size": 50,
  "items": [
    {
      "id": 1,
      "consumer_id": "worker-1",
      "event": {
        "seq": 1,
        "event_id": "evt-1",
        "occurred_at": "2026-09-26T09:00:01+00:00",
        "payload": {"a": 1},
        "received_at": "2026-09-26T11:59:30.000000+00:00",
        "hash": "2b2f3ae9…(64 位十六进制)"
      },
      "reason": "JSON 解析失败: 缺少必填字段 order_id",
      "quarantined_at": "2026-09-26T12:00:00.000000+00:00"
    }
  ]
}
```

隔离记录携带的是原事件内容的**独立副本**,因此即使原事件之后被安全归档压缩(`/retention/compact` 物理删除),隔离副本仍可随时查看,并可用 `event.hash` 与归档锚点核对;不同 consumer 的隔离记录互相独立,未知 consumer 返回空列表。

### 10. 安全归档压缩

已确认消费的历史事件可压缩归档:物理删除 `(当前锚点, cutoff_seq]` 的连续事件前缀,并把最后一条被删记录的 `seq`/`hash` 持久化为**链锚点**;删除前会按与 `/integrity` 完全相同的规范校验拟归档前缀(见 10b),`/integrity` 之后从锚点继续校验,历史不可篡改性不受影响。

```bash
# 先演练: 只返回预计删除数与边界, 不写库
curl -X POST http://localhost:8000/retention/compact \
  -H 'Content-Type: application/json' \
  -d '{"cutoff_seq":1000,"dry_run":true}'
```

```json
{"status": "dry_run", "dry_run": true, "would_delete": 1000,
 "from_seq": 1, "through_seq": 1000, "safe_through_seq": 1000}
```

```bash
# 确认无误后真正执行
curl -X POST http://localhost:8000/retention/compact \
  -H 'Content-Type: application/json' \
  -d '{"cutoff_seq":1000}'
```

```json
{"status": "compacted", "dry_run": false, "deleted": 1000,
 "from_seq": 1, "through_seq": 1000,
 "anchor_seq": 1000, "anchor_hash": "9f2c…(64 位十六进制)",
 "safe_through_seq": 1000}
```

**安全边界**(前缀校验通过后才检查):`cutoff_seq` 不得超过所有已登记 consumer 的**最小 `cursor_seq`**(未确认的事件绝不会被删);一个 consumer 都没有时以**当前最大 seq** 为边界。越界返回 **409** 并给出当前安全上界:

```json
{"detail": "cutoff_seq exceeds safe retention boundary",
 "cutoff_seq": 1200, "safe_through_seq": 1000}
```

若拟归档前缀本身存在哈希链断点,会**优先**返回链断点 409(详见 10b 节),两类 409 均不写库。

**幂等**:`cutoff_seq` 落在既往压缩已覆盖的区间(≤ 当前 `anchor_seq`)时返回 `{"status":"noop","deleted":0,…}`,不重复删除、不改变锚点(也不再校验安全边界与前缀);`dry_run` 任何时候都不写库。全部事件被压缩后,新写入的事件会从锚点哈希继续接链,`/integrity` 仍返回 `verified=true`。

**只删内容、不删 ID 占用**:归档删除的是 `events` 中的事件记录;首次接收时写入的 `event_id` 占用登记(`received_event_ids`)在同一事务中**刻意保留**。因此归档后同一 ID 重投不会"复活"为新事件——单条仍返回 200 `duplicate`,批量按序标 `duplicate`(`seq` 为 null),只有真正的新 ID 才续链接收(详见 1c 节)。日计数视图随被删事件扣减,重复投递不会把计数加回来。

一个典型的演练-执行循环:

```bash
# 1) 演练, 确认预计删除数与边界
curl -s -X POST http://localhost:8000/retention/compact \
  -H 'Content-Type: application/json' -d '{"cutoff_seq":1000,"dry_run":true}'
# 2) 执行
curl -s -X POST http://localhost:8000/retention/compact \
  -H 'Content-Type: application/json' -d '{"cutoff_seq":1000}'
# 3) 校验链(从锚点继续)
curl -s http://localhost:8000/integrity
```

### 10b. 归档前缀校验(防篡改洗白)

归档是**先校验、后删除**:从当前链锚点出发,对拟删除的 `(anchor_seq, cutoff_seq]` 区间按现有哈希规范逐条核对——**序号连续性**(首条须为 `anchor_seq + 1`,相邻 `seq` 逐一递增)、**前后哈希衔接**(`prev_hash` 等于上一条的 `hash`,首条等于锚点哈希)、**事件内容哈希**(按固定 JSON 规范重算)。这与 `/integrity` 共用同一套校验代码与判定顺序,因此 `/integrity` 能检出的篡改,归档**绝不**会把它连同事件一起删掉、再把(被篡改记录的)错误哈希写进锚点,导致归档后反而 `verified=true`。

- **遇到首个断点**:`dry_run` 与实际归档**都返回 409**,响应体给出断点的 `seq`、`event_id`、`reason`(取值与 `/integrity` 一致:`seq_gap` / `prev_hash_mismatch` / `hash_mismatch`)以及本次的 `cutoff_seq`、`dry_run`;事件、日计数视图与链锚点**全部保持原状**(不写库)。
- **断点不在拟归档前缀内**(断点 `seq > cutoff_seq`):前缀本身仍然干净,可按既有安全边界正常归档;已归档区间与后续断点之间的链证据保留,`/integrity` 仍能从新锚点检出该断点。
- **已覆盖区间幂等**:`cutoff_seq <= anchor_seq` 时直接返回 `noop`(dry_run 返回 `would_delete=0` 的 `dry_run`),不重复删除、不改锚点,也不因锚点之后可能存在的断点而失败。
- **无断点时**:沿用原有 `dry_run` / `compacted` / `noop` 响应字段与 `/integrity` 约定,行为不变。
- 校验在安全边界检查**之前**进行:前缀已被篡改时,即使 `cutoff_seq` 同时越过安全边界,也优先返回链断点 409(状态不变);前缀干净而越界时仍是原有的 `cutoff_seq exceeds safe retention boundary` 409。

```bash
# 假设 seq=2 的事件内容被库外篡改, /integrity 已检出 hash_mismatch
curl -s http://localhost:8000/integrity
# {"verified":false,"checked":1,
#  "first_break":{"seq":2,"event_id":"evt-2","reason":"hash_mismatch"}}

# dry_run: 409, 断点与 /integrity 完全一致, 不写库
curl -s -w '\nHTTP %{http_code}\n' -X POST http://localhost:8000/retention/compact \
  -H 'Content-Type: application/json' -d '{"cutoff_seq":4,"dry_run":true}'
# {"detail":"chain verification failed within compact prefix",
#  "cutoff_seq":4,"dry_run":true,
#  "first_break":{"seq":2,"event_id":"evt-2","reason":"hash_mismatch"}}
# HTTP 409

# 实际归档: 同样 409, 事件/视图/锚点保持原状
curl -s -w '\nHTTP %{http_code}\n' -X POST http://localhost:8000/retention/compact \
  -H 'Content-Type: application/json' -d '{"cutoff_seq":4}'
# HTTP 409

# 断点不在拟归档前缀内: 只归档干净的 seq<=1 前缀
curl -s -X POST http://localhost:8000/retention/compact \
  -H 'Content-Type: application/json' -d '{"cutoff_seq":1}'
# {"status":"compacted","deleted":1,"anchor_seq":1,...}

# 篡改仍可被 /integrity 检出(未被归档洗白); 修复内容后可继续正常归档
```

可运行的端到端调用示例见 `examples/compact_prefix_check.sh`(含篡改、409、断点外归档、修复后归档全流程)。

### 11. 持久化快照导出

需要把某一时刻的事件流整体导出(对账、迁移、审计)时,可创建**持久化快照**:在**同一个数据库事务**内把当前全部未压缩事件(含链字段 `prev_hash`/`hash`)连同链锚点复制到独立的快照表,并返回快照 ID、锚点序号与哈希、末尾序号和总数:

```bash
curl -X POST http://localhost:8000/exports
```

```json
{
  "export_id": 1,
  "anchor_seq": 1000,
  "anchor_hash": "9f2c…(64 位十六进制)",
  "end_seq": 1200,
  "total": 200,
  "created_at": "2026-09-27T08:00:00.000000+00:00"
}
```

快照与事件接收(`/events`)、压缩归档(`/retention/compact`)在同一写锁下**串行**执行,因此副本和元数据严格来自同一时刻;事件表为空时创建的是**空快照**(`end_seq == anchor_seq`、`total = 0`)。

按原 `seq` 升序分页读取快照副本:

```bash
# 第一页: after_seq 缺省为该快照的锚点序号
curl 'http://localhost:8000/exports/1/events?limit=100'
# 后续页: 用上一页返回的 next_after_seq
curl 'http://localhost:8000/exports/1/events?after_seq=1100&limit=100'
```

```json
{
  "export_id": 1,
  "after_seq": 1000,
  "limit": 100,
  "count": 100,
  "events": [
    {"seq": 1001, "event_id": "evt-1", "occurred_at": "...", "payload": {"a": 1},
     "received_at": "...", "prev_hash": "9f2c…", "hash": "ab01…"}
  ],
  "next_after_seq": 1100
}
```

- `after_seq`(不含,默认锚点序号)必须落在该快照的 **`[anchor_seq, end_seq]`** 内,否则返回 **422**(响应附 `anchor_seq`/`end_seq`);`limit` 范围 1~1000(默认 100),越界同样 422。
- `next_after_seq` 是下一页的起始序号,**末页为 `null`**;`after_seq == end_seq` 或空快照时始终返回空页(`events: []`、`next_after_seq: null`)。
- 分页只读**副本**:快照之后的继续写入、原事件被压缩归档、服务重启都**不改变**已建快照的分页结果。

删除快照(元数据与全部副本在**同一事务**内原子删除):

```bash
curl -X DELETE http://localhost:8000/exports/1
# => {"export_id": 1, "deleted": true}
```

未知或已删除的快照,`GET /exports/{id}/events` 与 `DELETE /exports/{id}` 统一返回 **404**;删除不影响其他快照。

#### 11b. 快照字段覆盖画像

排查事件载荷格式不一致(某字段时有时无、同一路径在不同事件里类型不同)时,可对**已有快照副本**读取字段覆盖画像:按 payload 路径递归统计出现事件数、缺失事件数以及各 JSON 类型的事件数。画像**只读快照副本**,不返回任何具体字段值,也不改原事件和快照;后续写入、原事件被压缩归档、服务重启都不影响结果。

**统计结果持久化、惰性一次性生成**:画像在该快照的**首次查询**时基于 `export_events` 副本**全量扫描一次**并聚合(本功能上线前已经创建好的历史快照同样适用),随后把每个路径的统计**原子保存**到独立的画像统计表;之后所有翻页请求都按路径游标做**主键范围读取**,只取本页所需的路径行,延迟与快照中的事件总数无关(首次查询付一次成本)。服务重启后持久化的统计继续复用;`POST /exports` **不**预先生成统计(空快照首次查询只落一个 `total=0` 的统计头)。统计**绝不从当前 `events` 表补算**,即使原事件已被压缩删除,结果仍来自建快照时的副本。

- **并发首次查询**:首次聚合在单个写事务(`BEGIN IMMEDIATE`)内完成,与快照删除互斥;并发的多个首次查询在写锁上**串行**,第一个完成聚合并落库,其余查询持锁后复查并直接复用,因此**不会重复计算,也不会读到半成品**(统计行只在事务提交后可见)。
- **删除快照即清理统计**:`DELETE /exports/{id}` 在**同一事务**内删除事件副本、画像路径行、画像统计头与快照元数据,不残留孤儿统计。

```bash
# 第一页: after_path 缺省从头(含根路径 ""), limit 默认 100(1~1000)
curl 'http://localhost:8000/exports/1/field-coverage'
# 后续页: 用上一页返回的 next_after_path
curl --get 'http://localhost:8000/exports/1/field-coverage' --data-urlencode 'after_path=/user/roles' --data 'limit=100'
# 用 jq 自动翻完整个画像
curl -s 'http://localhost:8000/exports/1/field-coverage?limit=2' | jq .
```

假设快照中两条事件 payload 分别为 `{"user":{"id":"u1","roles":["admin"]},"score":5}` 与 `{"user":{"id":7},"score":null}`:

```json
{
  "export_id": 1,
  "total": 2,
  "after_path": "",
  "limit": 100,
  "items": [
    {"path": "", "present_events": 2, "missing_events": 0,
     "type_events": {"object": 2}},
    {"path": "/score", "present_events": 2, "missing_events": 0,
     "type_events": {"number": 1, "null": 1}},
    {"path": "/user", "present_events": 2, "missing_events": 0,
     "type_events": {"object": 2}},
    {"path": "/user/id", "present_events": 2, "missing_events": 0,
     "type_events": {"string": 1, "number": 1}},
    {"path": "/user/roles", "present_events": 1, "missing_events": 1,
     "type_events": {"array": 1}},
    {"path": "/user/roles/*", "present_events": 1, "missing_events": 1,
     "type_events": {"string": 1}}
  ],
  "next_after_path": null
}
```

**路径编码与统计口径**:

- **递归包含根、对象、数组与标量**;根路径固定为空串 `""`;对象键按 JSON Pointer 风格编码——`~`、`/`、`*` 分别转义为 `~0`、`~1`、`~2`(先转义 `~`);**数组下标统一为 `*`**(不区分第几个元素),如 `user.roles[0]` 与 `user.roles[3]` 都记为 `/user/roles/*`。
- **同一事件内同一路径只计一次出现,同一(路径, 类型)也只计一次**:因此**混合数组**(同一数组路径下既有字符串又有数字)会让各类型计数之和**大于出现数**;`null` **计为出现**(类型为 `null`);**空数组不产生元素路径**(但数组自身所在路径仍计一次 `array`)。
- `present_events`(出现事件数)+ `missing_events`(缺失事件数)恒等于 `total`;`type_events` 只列出计数大于 0 的 JSON 类型,取值为 `object`/`array`/`string`/`number`/`boolean`/`null`(整数与浮点统一为 `number`,布尔单列 `boolean`)。

**分页与错误**:

- 按路径**字典序**分页,只取**严格大于** `after_path` 的项;缺省从头(包含根路径 `""`),显式传 `after_path=`(空串)则从根之后开始;`next_after_path` 为下一页游标(本页最后一条路径),**末页为 `null`**;游标落在末尾之后时返回空页与 `null`,游标无需是真实存在的路径。首次查询之后,翻页只按 `(snapshot_id, path)` 主键做范围扫描读取本页路径,不再扫描快照副本。
- **空快照**返回 `total: 0`、空 `items` 与 `next_after_path: null`(首次查询同样会持久化空统计头)。
- 快照**未知或已删除返回 404**;`limit` 越界(不在 1~1000)或 `after_path` 不是合法路径编码(如非根路径不以 `/` 开头、出现裸 `~3`、键段内裸 `*`)返回 **422**(响应回显 `after_path`)。
- 删除快照会在同一事务内同步清除持久化画像;既有 `GET /exports/{id}/events` 导出接口的请求与响应格式保持不变。

### 12. 事件日计数视图

按 `occurred_at` 的 **UTC 日期**分桶统计事件数的物化视图:创建时指定**唯一名称**和可选的 **payload 顶层字符串字段等值条件**,创建在**同一写事务**内回填现存事件并保存规则;之后单条/批量接收只对**真正新建**的事件增量更新(重复 `event_id` 不重复计数),归档压缩删除事件时在**同一事务**内扣减对应日期计数(空桶移除),迟到事件归入其发生日期。视图始终只反映**当前 events 表**内的匹配事件。

```bash
# 创建视图(不过滤可省略 filters 或传 {})
curl -X POST http://localhost:8000/views \
  -H 'Content-Type: application/json' \
  -d '{"name":"signup-ios","filters":{"type":"signup","platform":"ios"}}'
```

```json
{
  "name": "signup-ios",
  "filters": {"type": "signup", "platform": "ios"},
  "created_at": "2026-09-27T08:00:00.000000+00:00",
  "days": [
    {"day": "2026-09-18", "count": 2},
    {"day": "2026-09-19", "count": 1}
  ],
  "total": 3
}
```

- **匹配规则**:`filters` 中每个键值对都要求事件 `payload` **顶层同名字段为字符串且相等**;非字符串值(数字、布尔、嵌套对象)不匹配字符串条件。`filters` 为空对象时统计全部事件;条件最多 50 个,字段名不可为空白。
- **重名创建**返回 **409** `view name already exists`,不覆盖已有视图;名称空白、过滤值非字符串等由请求校验拦截(**422**)。
- **分桶**:按归一化后的 UTC `occurred_at` 取 `YYYY-MM-DD`,与接收时间无关——迟到事件按其发生日期归桶;带时区偏移的时间(如 `+08:00`)先换算成 UTC 再分桶。

按名称查询视图(各日计数按日期升序,附总数):

```bash
curl http://localhost:8000/views/signup-ios
```

删除视图(规则与全部日桶在**同一事务**内原子删除):

```bash
curl -X DELETE http://localhost:8000/views/signup-ios
# => {"name": "signup-ios", "deleted": true}
```

未知名称的查询与删除统一返回 **404**;删除不影响事件本身与其他视图。

### 13. 事件因果关系(唯一父事件)

为已入库事件登记**唯一父事件**,形成一棵事件因果树(森林):`PUT /relations/{event_id}/parent` 设置或更换父节点,`GET /relations/{event_id}/ancestors` 查询祖先链。关系**独立于 events 表保存**(刻意不建外键)——原事件被安全归档压缩物理删除后,已登记的关系仍可按 `event_id` 查询;但**建立或修改关系时,父子两端事件必须当前仍存在**。

先准备 3 个事件(复用上面 `/events` 接口):

```bash
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"event_id":"evt-a","occurred_at":"2026-09-28T09:00:00Z","payload":{"k":"a"}}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"event_id":"evt-b","occurred_at":"2026-09-28T09:01:00Z","payload":{"k":"b"}}'
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"event_id":"evt-c","occurred_at":"2026-09-28T09:02:00Z","payload":{"k":"c"}}'
```

设置父事件(每个事件最多一个,重复 PUT 会**更换**):

```bash
# evt-c -> evt-b
curl -X PUT http://localhost:8000/relations/evt-c/parent \
  -H 'Content-Type: application/json' -d '{"parent_id":"evt-b"}'
# => {"event_id":"evt-c","parent_id":"evt-b","status":"set"}

# evt-b -> evt-a
curl -X PUT http://localhost:8000/relations/evt-b/parent \
  -H 'Content-Type: application/json' -d '{"parent_id":"evt-a"}'
# => {"event_id":"evt-b","parent_id":"evt-a","status":"set"}
```

- **幂等**:对同一事件重复设置**同一个**父节点原样成功,返回 `"status":"unchanged"`,不更新时间戳、不改变任何关系。
- **必须是不同的现存事件**:子事件或父事件未知、或已被安全归档删除,返回 **404**(`event not found` / `parent event not found`);`parent_id` 等于路径里的 `event_id`(自环)返回 **409** `event cannot be its own parent`。
- **环检测在同一写事务内沿父链完成**:若更换父事件会使父链成环,返回 **409** `relation change would create a cycle` 且**原关系不变**。例如已有 `evt-c -> evt-b -> evt-a` 时,再把根 `evt-a` 的父设为 `evt-c`:

```bash
curl -X PUT http://localhost:8000/relations/evt-a/parent \
  -H 'Content-Type: application/json' -d '{"parent_id":"evt-c"}'
# 409 {"detail":"relation change would create a cycle",
#      "event_id":"evt-a","parent_id":"evt-c"}
# evt-a 仍是根, 既有关系链不变
```

查询祖先链——按**最近父节点到根节点**排序,最近父节点深度为 1:

```bash
curl http://localhost:8000/relations/evt-c/ancestors
```

```json
{
  "event_id": "evt-c",
  "count": 2,
  "ancestors": [
    {"event_id": "evt-b", "depth": 1},
    {"event_id": "evt-a", "depth": 2}
  ]
}
```

未登记父事件的事件(根节点或游离事件)返回空链 `{"count":0,"ancestors":[]}`;**未知事件**(从未入库也没有任何已登记关系)返回 **404**。

**归档后仍可查询**:关系表不随 `/retention/compact` 删除事件而清理。例如把 `evt-a`、`evt-b` 归档(物理删除)后:

```bash
# 关系仍在, 沿链穿越已归档节点继续返回
curl http://localhost:8000/relations/evt-c/ancestors
# => {"event_id":"evt-c","count":2,"ancestors":[
#      {"event_id":"evt-b","depth":1},{"event_id":"evt-a","depth":2}]}
# 已归档事件自身的关系也能按 ID 查到
curl http://localhost:8000/relations/evt-b/ancestors
# => {"event_id":"evt-b","count":1,"ancestors":[{"event_id":"evt-a","depth":1}]}

# 但不能对已归档事件建立或修改关系(两端都必须当前存在)
curl -X PUT http://localhost:8000/relations/evt-c/parent \
  -H 'Content-Type: application/json' -d '{"parent_id":"evt-a"}'
# => 404 {"detail":"parent event not found","event_id":"evt-a"}
# 子事件已归档时同样 404 event not found, 其原有关系保持不变
```

## 哈希链规范

每条记录写入时在同一事务内计算并存储两个字段:

- `prev_hash`:上一条记录的 `hash`(首条为空串 `""`)
- `hash`:对以下文档做**固定 JSON 规范化**(键排序、紧凑分隔符 `,`/`:`、非 ASCII 不转义)后的 SHA-256 十六进制:

```json
{"seq":1,"event_id":"evt-1","occurred_at":"...","payload":{...},"received_at":"...","prev_hash":""}
```

重复 `event_id` 的投递走幂等返回,不会写入记录,链保持不变;`POST /events/batch` 中批量重复(已占用或批内前面已出现)的条目同样跳过,只有新建条目参与续链。幂等判定基于独立的 ID 占用表 `received_event_ids` 而非 `events` 本身,因此**事件被压缩归档(内容物理删除)后,同一 ID 重投依旧判重**,不会分配新 seq(见 1c 节)。任何对历史记录的字段篡改、删除或重排都会被 `/integrity` 在首个断点处检出。

**链锚点**:压缩归档删除的历史前缀由单行表 `chain_anchor` 记录——`anchor_seq`(已压缩到的最大 seq)与 `anchor_hash`(最后被删记录的哈希)。锚点之后的第一条现存记录必须 `seq = anchor_seq + 1` 且 `prev_hash = anchor_hash`;事件表为空时新写入的记录也从锚点哈希续链,因此压缩不会削弱后续记录的防篡改能力。

**旧库迁移**:启动时自动为已有 SQLite 数据补加 `prev_hash`/`hash` 列并按首次接收顺序回填整条链;同时新建 `received_event_ids` 占用表并把**当前未归档事件**的 `event_id`(附首次 seq/接收时间)一次性回填,保证升级后跨归档判重立即生效。迁移幂等可重复执行;旧版已归档删除且无留存的历史 ID 不追溯。

## 设计要点

- **幂等(跨归档)**:`event_id` 首次接收时在**同一写事务**内写入 `events` 并登记到独立占用表 `received_event_ids`(附首次 `seq`/`received_at`),后者主键唯一。重投先查占用表:命中(无论事件内容是否仍在)即返回 200 `duplicate`,`events.event_id UNIQUE` 约束作并发兜底。`/retention/compact` 只删 `events` 内容、**不删占用**,所以归档前后判重口径一致:同一 ID 永远只占首次那条记录、不会二次分配 seq,按 ID 保存的因果关系也不会指向别的记录。
- **批量接收**:`POST /events/batch` 接受 1~1000 条同格式事件的 JSON 数组,在单个 `BEGIN IMMEDIATE` 事务内"一次查齐已占用的 `event_id`(含仅余占用的已归档 ID) + 用批内 `seen` 集合追踪 → 按输入顺序逐条判重、续链接入并同事务占 ID"。已占用或批内重复的条目标记 `duplicate`(`seq` 为 null)且不消耗 seq,新建条目 `seq` 连续;响应 `results` 按输入顺序返回并汇总 `created`/`duplicate`,有新建即 201、全重复 200。格式非法在事务前整体 422,写入冲突回滚整批(409),任何失败路径都不留部分新事件;与单条接口共用同一判重逻辑与哈希链,单条/批量可任意交错提交。
- **接收顺序**:自增主键 `id` 即首次接收顺序,重放按它排序。
- **完整性哈希链**:每条记录引用上一条哈希,写入与链字段更新在同一事务完成(`BEGIN IMMEDIATE` 保证并发下链头一致);`GET /integrity` 全链校验。
- **时间比较**:`occurred_at` 归一化为 UTC ISO8601 字符串存储,范围查询走字典序比较,并建有索引。
- **消费游标与租约**:每个 consumer 一行(`consumers` 表),保存已确认游标与当前租约(token、批次起止 seq、过期时间)。`pull` 在 `BEGIN IMMEDIATE` 事务内完成"判租约 → 取批次 → 写租约",与事件写入互斥,并发重复拉取只会得到同一个租约;`ack` 仅推进整批游标并清空租约,token 不符、过期或无租约一律 409。**序号缺口防护**:`first_seq_gap` 只核对序号(不涉及哈希),在首次拉取(`max(cursor_seq, anchor_seq)+1` 起的本批)、未过期租约重拉、过期续租三条路径返回前逐 seq 核对连续性,`ack`/`ack-prefix`(整个租约区间,含前缀之后)推进前同样核对;发现缺口返回 409 及与 `/integrity` 同形的 `first_break`(缺口后第一条现存记录;缺口延伸到区间末尾、`/integrity` 不可见时用首个缺失 seq 并按 ID 占用表还原 event_id),首次拉取不建租约、续租路径不换令牌,游标与租约全部保持原状;缺口修复后未过期租约沿用原 token/过期时间直接确认,过期租约才按原区间换发新 token——游标在任何情况下都不会越过中间缺口,记录恢复后必然再次投递。归档锚点始终是可投递起点,无缺口时批次内容与令牌约定不变。
- **前缀确认**:`POST /consumers/{consumer_id}/ack-prefix` 与 `ack` 共用同一套租约校验(含整个租约区间的序号缺口核对),额外要求 `through_seq` 落在 `[lease_from_seq, lease_to_seq]`;在单个 `BEGIN IMMEDIATE` 事务内把游标推进到 `through_seq` 并清除租约,旧 token 立即作废,下次 `pull` 从 `through_seq+1` 重新租用。`through_seq` 等于批次末尾即整批确认;未知 consumer、过期/伪造/旧 token、序号越界或租约区间内仍有 seq_gap 均 409 且状态不变。
- **游标回退**:`POST /consumers/{consumer_id}/rewind` 在单个 `BEGIN IMMEDIATE` 事务内完成"校验 consumer/租约/回退区间 `[anchor_seq, cursor_seq]` → 清除已过期租约 → 回退游标",与 `pull`/`ack`/`ack-prefix`/`compact` 串行,并发下只有单一结果。未过期租约、低于链锚点、向前推进、未知 consumer 均 409 且状态不变;回退后游标不低于链锚点,不会产生指向已压缩事件的租约。
- **毒丸隔离**:`POST /consumers/{consumer_id}/quarantine` 在单个 `BEGIN IMMEDIATE` 事务内完成"核对令牌(与 `ack` 同套:无租约/过期/旧令牌 409)→ 核对租约起点与游标相邻(考虑链锚点)→ 复制首条事件完整内容、原哈希与非空原因到独立 `quarantined_events` 表 → 游标仅越过这一条并清除整批租约"。下次 `pull` 重新租用剩余事件并换发新 token,旧令牌立即作废;失败原因空白返回 422。`GET /consumers/{consumer_id}/quarantine` 分页查看隔离记录;副本独立于原事件存储,原事件被压缩归档(物理删除)后仍可查看。隔离会把游标推进过该事件,因此被隔离事件视为"已处理",可正常纳入安全归档,内容由隔离副本留存。
- **安全归档压缩**:`POST /retention/compact` 在单个 `BEGIN IMMEDIATE` 事务内完成"读锚点 → 幂等速判已覆盖区间 → 与 `/integrity` 共用 `verify_chain_prefix` 从锚点逐条校验拟删前缀(序号连续性 → 前后哈希衔接 → 内容哈希)→ 算安全边界(所有 consumer 的最小 `cursor_seq`,无 consumer 时为当前最大 seq)→ 校验 `cutoff_seq` → 删连续前缀 → 写链锚点"。拟删前缀出现首个断点时,**实际归档与 `dry_run` 都返回 409**,响应附与 `/integrity` 同形的 `first_break`(seq/event_id/reason,reason 取值 `seq_gap`/`prev_hash_mismatch`/`hash_mismatch`),事件、视图、锚点保持原状——篡改不会因删除并把错误哈希写入锚点而在归档后"洗白"成 `verified=true`;断点不在拟归档前缀内时干净前缀仍可按边界归档,断点在归档后依旧可检;越界 409 并返回 `safe_through_seq`;`dry_run` 只算不写;已覆盖区间幂等返回 `noop`。删除只作用于 `events` 内容与视图计数,**不触碰 `received_event_ids` 占用**,归档后重投同一 ID 仍是 duplicate(单条 200、批量按序 duplicate/seq null),占用信息随写入与归档在同一事务内保持一致。
- **持久化快照导出**:`POST /exports` 在单个 `BEGIN IMMEDIATE` 事务内完成"读链锚点 → 写快照元数据(锚点 seq/hash、末尾 seq、总数)→ `INSERT ... SELECT` 整体复制当前全部未压缩事件到 `export_events`",与事件写入、压缩归档串行,副本与元数据来自同一时刻。副本落库后不可变:`GET /exports/{id}/events` 只查快照表,按 `(snapshot_id, seq)` 主键升序分页(多取一条判断 `next_after_seq`,末页为 `null`),后续写入/压缩/重启均不影响;`after_seq` 越出 `[anchor_seq, end_seq]` 返回 422。`DELETE /exports/{id}` 在同一事务内删除画像统计(`export_field_coverage` 及其路径表)、事件副本与元数据,未知或已删除统一 404。
- **快照字段覆盖画像**:`GET /exports/{id}/field-coverage` 只读 `export_events` 副本(不读不写 events、不返回字段值),对每条 payload 用显式栈迭代递归(避免深层 JSON 触发递归上限),产出 (路径, JSON 类型) 集合:根路径 `""`、对象键把 `~`/`/`/`*` 转义为 `~0`/`~1`/`~2`、数组下标统一 `*`、空数组不产生元素路径。聚合时**出现按路径、类型按 (路径, 类型) 双重去重**——同一事件同路径只计一次、每种类型也只计一次,因此混合数组的类型数之和可超过出现数,`null` 计为出现,`missing = total - present`。**路径统计惰性持久化**:首次查询(含历史快照)在单个 `BEGIN IMMEDIATE` 事务内把副本全量扫描一次、聚合并把统计头(total/路径数)与每个路径的六类计数写入 `export_field_coverage` / `export_field_coverage_paths`(`WITHOUT ROWID`,`(snapshot_id, path)` 复合主键),同事务原子提交,无半成品;并发首次查询经写锁 + 持锁复查串行化,只计算一次、不重复写入,与删除快照互斥(等待期间快照被删则返回 404)。之后翻页仅按主键做 `path > cursor` 范围读取本页(查询计划为主键 SEARCH),延迟与事件数无关,重启后复用,且**绝不从当前 events 补算**;`POST /exports` 不预生成,空快照只落 `total=0` 统计头。分页只取严格大于 `after_path` 的项(缺省从根 `""` 起含根,显式空串游标则跳过根;末页 `next_after_path` 为 null)。空快照返回 `total=0` 空列表;未知/已删除 404,`limit` 越界或游标不是合法转义(非根不以 `/` 开头、裸 `~n`、键段内裸 `*`)422。`DELETE /exports/{id}` 同事务清理统计与副本。不改动既有 `/exports/{id}/events` 导出接口。
- **事件日计数视图**:规则存 `count_views`(名称 `UNIQUE`,重名 409),日桶计数存 `count_view_buckets`(`(view_id, day)` 主键)。`POST /views` 在单个 `BEGIN IMMEDIATE` 事务内"保存规则 → 全量扫描现存事件按 UTC 日期回填";`/events` 与 `/events/batch` 只对真正新建的条目逐视图匹配增量 +1(重复 `event_id` 走幂等分支,不动视图);`/retention/compact` 在同一事务内对被删前缀逐条扣减并移除空桶——因此视图始终等于"当前 events 表内匹配事件按发生日期的计数",迟到事件按 `occurred_at` 归其发生日期。`GET /views/{name}` 按名称返回各日计数与总数,`DELETE /views/{name}` 单事务删除规则与日桶,未知名称均 404。
- **事件因果关系**:关系存独立表 `event_relations`(`event_id` 主键保证唯一父事件,另建 `parent_id` 索引),**不建指向 events 的外键**,故 `/retention/compact` 物理删除事件后已登记关系仍保留可查。`PUT /relations/{event_id}/parent` 在单个 `BEGIN IMMEDIATE` 事务内完成"校验子事件现存 → 校验父事件现存(均 404)→ 拒绝自环(409)→ 同一父节点幂等返回 `unchanged` → **从新父事件沿父链上行, 命中子事件即判定成环**(409, 原关系不变)→ UPSERT 写入/更换"。环检测从新父侧上行是关键:边的终点能沿现存边到达边的起点才成环(如 `c→b→a` 已存在,再把 `a` 指向 `c`),仅检查子事件祖先会漏判。`GET /relations/{event_id}/ancestors` 沿关系表上行(穿越已归档节点),按最近父节点到根返回 `event_id`/`depth`(最近父节点 depth=1);已知节点的判定覆盖 events 表与关系图两侧(登记过父或被他人登记为父),因此归档后仍按 ID 可查,只有完全未知的 ID 返回 404。
- **配置**:数据库路径由环境变量 `DB_PATH` 控制(容器内默认 `/data/events.db`)。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/events` | 接收单个事件(幂等):首次接收占用 `event_id`,即使事件之后被归档删除,重投仍返回 200 `duplicate`(不分配新 seq、不动链与视图) |
| POST | `/events/batch` | 批量接收 1~1000 条事件(JSON 数组,单写事务):已占用(含已归档仅余占用)/批内重复标 `duplicate` 不写,新建按序续链并占 ID;响应按输入顺序给出状态与 `seq`(duplicate 为 null),全重复 200、含新建 201,整批失败无部分写入 |
| GET | `/events` | 时间范围分页查询(`from`/`to`/`page`/`page_size`) |
| POST | `/replay` | 批量重放,按首次接收顺序输出 |
| GET | `/integrity` | 从链锚点校验完整性哈希链,返回 `verified`/`checked`/`first_break`/`anchor_seq` |
| POST | `/consumers/{consumer_id}/pull` | 按首次接收顺序拉取并原子租用未确认事件(`limit`/`lease_seconds`);首次拉取/未过期重拉/过期续租返回前都核对拟返回批次序号连续性,遇 seq_gap 返回 409 及与 `/integrity` 一致的 `first_break`,游标与租约不变、不发新令牌;归档锚点是可投递起点 |
| POST | `/consumers/{consumer_id}/ack` | 确认当前租约批次并推进游标;过期/伪造/旧 token 返回 409;租约区间仍有 seq_gap 时返回 409 + `first_break` 且游标/租约/令牌不变,修复后沿用原 token 确认 |
| POST | `/consumers/{consumer_id}/ack-prefix` | 确认租约内 `[批次起点, through_seq]` 连续前缀并清除租约,下次 `pull` 从 `through_seq+1` 重租;`through_seq` 到批次末尾即整批确认;未知 consumer/过期/旧 token/越界序号/租约区间内 seq_gap(含前缀之后)返回 409,修复后沿用原 token 与时效 |
| POST | `/consumers/{consumer_id}/rewind` | 游标回退到 `target_seq`(限 `[anchor_seq, cursor_seq]`),下次 `pull` 从 `target_seq+1` 重投;未过期租约/越界/未知 consumer 返回 409 |
| POST | `/consumers/{consumer_id}/quarantine` | 隔离当前未过期租约的首条事件(带非空 `reason`):单事务核对令牌与租约相邻性、复制完整事件到独立隔离记录、游标越过一条并清除整批租约;无租约/过期/旧令牌/不相邻返回 409,原因为空返回 422 |
| GET | `/consumers/{consumer_id}/quarantine` | 分页查询该 consumer 的隔离记录;携带原事件完整内容副本,原事件归档后仍可查看 |
| POST | `/retention/compact` | 安全归档压缩连续事件前缀(`cutoff_seq`/`dry_run`):删除前从锚点按 `/integrity` 同规范校验拟删前缀,首个断点时实际归档与 `dry_run` 均 409 并回 `first_break`(seq/event_id/reason),事件/视图/锚点不变;断点在拟归档前缀之外仍可按边界归档;同事务删内容/扣视图/写链锚点但**保留 event_id 占用**,归档后重投仍判重;已覆盖区间幂等;越界 409 并返回 `safe_through_seq` |
| POST | `/exports` | 创建持久化快照:单事务复制全部未压缩事件与链锚点,返回快照 ID、锚点 seq/hash、末尾 seq 与总数 |
| GET | `/exports/{id}/events` | 按原 `seq` 升序分页读取快照副本(`after_seq` 默认锚点序号,限 `[anchor_seq, end_seq]` 否则 422;`limit` 1~1000;`next_after_seq` 末页为 null) |
| GET | `/exports/{id}/field-coverage` | 读取快照副本的字段覆盖画像(首次查询基于副本一次性聚合并**原子持久化**,之后按路径游标主键范围读页、重启复用,绝不从 events 补算;并发首次查询不重复/不半成品):按路径(根 `''`、键转义 `~0/~1/~2`、数组下标 `*`)统计出现/缺失事件数与各 JSON 类型事件数;`after_path` 字典序游标(严格大于,缺省含根)、`limit` 1~1000,`next_after_path` 末页为 null;只读副本不返回字段值;未知快照 404,非法游标/`limit` 422 |
| DELETE | `/exports/{id}` | 单事务原子删除快照元数据、全部副本与字段覆盖统计;未知或已删除统一 404 |
| POST | `/views` | 创建事件日计数视图(唯一名称 + 可选 payload 顶层字符串字段等值条件):单事务回填现存事件,按 UTC 日期分桶;重名 409 |
| GET | `/views/{name}` | 按名称查询视图:各 UTC 日计数(升序)及总数;未知名称 404 |
| DELETE | `/views/{name}` | 单事务删除视图规则与全部日桶;未知名称 404 |
| PUT | `/relations/{event_id}/parent` | 为现存事件设置/更换唯一父事件:同事务沿父链检测环(成环/自环 409 且原关系不变),同一父节点幂等;父子须为不同现存事件,缺失 404 |
| GET | `/relations/{event_id}/ancestors` | 按最近父节点到根返回祖先 `event_id` 与 `depth`;关系独立存储,事件归档后仍可查;未知事件 404 |
| GET | `/health` | 健康检查 |

交互式文档:启动后访问 `http://localhost:8000/docs`。
