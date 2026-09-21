# 断点分片上传协调 API（Chunked Upload Coordinator）

只协调**元数据**的上传调度服务：创建会话、分片租约、完成上报、提交汇总与状态查询。
**不接收、不保存任何文件内容**，文件字节流由客户端自行点对点传输或写入对象存储。

技术栈：Python 3.12 · FastAPI · SQLite（WAL）

## 快速启动（单条命令）

```bash
docker compose up --build
```

启动后访问：

| 地址 | 说明 |
|---|---|
| `http://localhost:8000` | 服务信息 |
| `http://localhost:8000/docs` | Swagger 交互式 API 文档 |
| `http://localhost:8000/openapi.json` | OpenAPI 描述 |
| `http://localhost:8000/healthz` | 健康检查 |

依赖安装已纳入镜像构建，SQLite 建库建表在应用启动时自动完成，无需额外初始化步骤。
数据通过命名卷 `coordinator-data` 持久化，删除卷即清空全部状态。

## 数据库版本化迁移（启动时自动执行）

数据库结构由代码内一组**固定顺序、不可变**的版本化迁移脚本定义（`app/db.py`
中的 `MIGRATIONS`，当前最新版本 16）。每次启动时迁移器在**一笔
`BEGIN IMMEDIATE` 事务**内完成收敛，三种库最终得到完全一致的最新结构：

| 启动时的库 | 行为 |
|---|---|
| 空库（首次启动） | 按版本 1 → 16 顺序执行全部脚本，逐版本写入 `schema_migrations` |
| 旧库（按列探测时代创建，无 `schema_migrations` 表） | 逐版本探测已有表/列/索引：已存在的结构只登记版本不执行，缺失部分补齐；**既有数据、索引与外键原样保留** |
| 已纳管库（再次启动） | 校验已记录版本后只执行新增版本；无新增时纯校验后启动 |

每个版本执行成功后在 `schema_migrations(version, name, checksum, applied_at)`
中登记版本号、用途与该版本规范脚本的 **SHA-256**（64 位小写十六进制）。

**启动安全校验（不通过即拒绝服务，进程退出、不监听端口）：**

- **校验和变化**：某已记录版本的脚本 SHA-256 与库中记录不一致，说明已发布脚本
  被改动——报错并拒绝启动。
- **版本缺口**：已记录版本不连续（如记录了 1、2、4 却缺 3）——报错并拒绝启动，
  不会跳过缺口继续向前迁移。
- **数据库版本高于代码**：库中存在本代码不认识的更高版本（误回滚旧版本代码
  打开了新库）——报错并拒绝启动。

**并发启动（多进程/多副本同时启动）：** `BEGIN IMMEDIATE` 保证同一时刻只有一个
进程取得写锁执行迁移；其它进程按固定间隔重试等待，总等待时间以
`MIGRATION_LOCK_TIMEOUT_SECONDS`（默认 30 秒）为**硬上限**——抢锁期间连接的
`busy_timeout` 被临时绕过，配置值小于 `busy_timeout` 时也能按配置及时失败。
拿到锁后**重新读取迁移表复核结果**：已登记的版本只做结构探测、绝不重复执行，
因此不会出现重复 `ALTER TABLE`/重复建表。超时抛错放弃本次启动，数据库保持
原状态，由编排层重试。

**失败恢复：** 整轮迁移（含 `schema_migrations` 建表）在单个事务内提交，任何
步骤失败都会**整体 ROLLBACK**，数据库保持启动前的一致状态，不会留下“迁移了
一半”的库。按日志中的明确错误提示处理后再次启动即可原样重试：

- 脚本执行失败（磁盘满、I/O 错误等）：释放空间/修复文件权限后直接重启；
- 校验和/版本不匹配：核对部署的代码版本，或从备份恢复数据库后重启；
- 取得写锁超时：确认其它实例已经完成迁移并退出后重启（Docker Compose 配置了
  `restart: unless-stopped`，会自动重试）。

查看当前库的迁移状态：

```bash
docker compose exec coordinator python -c "\
import sqlite3, os; \
c = sqlite3.connect(os.environ['UPLOAD_DB_PATH']); \
print(c.execute('SELECT version, name, checksum, applied_at FROM schema_migrations ORDER BY version').fetchall())"
```

> 维护约定：新增结构只能在 `MIGRATIONS` **末尾追加新版本**，严禁修改或删除已
> 发布版本的语句（会改变校验和导致所有现存库拒绝启动）。

## 事务保证（写接口的统一事务边界）

所有状态变更（单片/批量领取、跨会话调度、完成、失败、回退 reopen、提交 finalize、
取消、创建/分支 fork、租户配额、交付凭证签发/兑换/撤销、验收领取/决定）都在统一的
事务边界内执行，保证：

- **校验失败即回滚**：请求在返回任何 4xx（404 / 409 / 422 业务冲突）时，不会遗留
  本次请求的任何写入。校验全部是只读并先于写入完成；确实需要先写再判定的路径，
  其 4xx 响应由事务回滚后渲染，**绝不会**把开着的 SQLite 事务留给后续请求意外提交。
- **执行异常即回滚**：写入中途发生数据库异常（如约束冲突、I/O 错误）时整笔事务
  ROLLBACK，连接立即恢复可用状态——随后的请求不会看到“只发了一半的批量租约”、
  “凭证已消费但缺幂等记录”、“游标推进了但租约没发出”这类半成品，账务
  （`used_bytes`/`reserved_bytes`）、分片状态、调度游标均保持请求前的一致状态。
- **预留到期的惰性取消独立持久化**：active 会话超过预留 TTL 后，首次对其的写访问
  会把它原子标记为 `cancelled`。该取消是一笔**独立、完整**的事务，在请求自身的
  校验/写入事务**之前**立即提交——即使该请求随后返回 409（或调度扫描最终
  `granted=null`），取消结果也已落库：会话进入 `cancelled`，在途**上传租约与验收
  租约立即失效并清除**，待验收分片按 reject 同口径回到 `pending`，且不会被同一请求
  或后续请求的回滚带走。
- **连接模型**：共享 SQLite 连接以 autocommit 模式（`isolation_level=None`）打开，
  Python sqlite3 不会隐式开事务；每个写路径显式 `BEGIN IMMEDIATE … COMMIT/ROLLBACK`
  （经进程内 `DB_LOCK` 串行化）。只读端点不开启事务。因此请求之间不存在“上一个
  失败请求的事务被下一个请求的 commit 意外带上”的可能。

成功响应、状态码与全部幂等语义（完成上报、reopen 修订号、fork/兑换/验收决定的
request_id/令牌重放等）与之前完全一致。

## 配置（环境变量）

均可在 `docker-compose.yml` 的 `environment` 中调整：

| 变量 | 默认值 | 说明 |
|---|---|---|
| `UPLOAD_DB_PATH` | `/data/coordinator.db` | SQLite 数据库文件路径 |
| `MIGRATION_LOCK_TIMEOUT_SECONDS` | `30` | 并发启动时等待其它进程释放迁移写锁的最长秒数（总等待硬上限，不受连接 busy_timeout 影响；支持小数），超时放弃本次启动 |
| `DEFAULT_LEASE_TTL_SECONDS` | `300` | 租约默认有效期（秒） |
| `MAX_LEASE_TTL_SECONDS` | `3600` | 租约有效期上限（秒），超出会被截断 |
| `DEFAULT_VERIFICATION_TTL_SECONDS` | `300` | 验收令牌默认有效期（秒），缺省跟随 `DEFAULT_LEASE_TTL_SECONDS` |
| `MAX_VERIFICATION_TTL_SECONDS` | `3600` | 验收令牌有效期上限（秒），缺省跟随 `MAX_LEASE_TTL_SECONDS` |
| `DEFAULT_BASE_RETRY_SECONDS` | `5` | 创建会话未显式指定时的失败退避基数（秒） |
| `DEFAULT_MAX_RETRY_SECONDS` | `300` | 创建会话未显式指定时的失败退避冷却上限（秒） |
| `DEFAULT_RESERVATION_TTL_SECONDS` | `86400`（24 小时） | 创建会话未显式指定 `reservation_ttl_seconds` 时的**容量预留有效期**（秒）；fork 新会话固定取该默认值 |
| `MAX_RESERVATION_TTL_SECONDS` | `604800`（7 天） | 容量预留有效期上限（秒），创建会话指定更长 TTL 会被截断 |

端口映射默认为宿主机 `8000` → 容器 `8000`（TCP/HTTP，唯一对外端口），可在
`docker-compose.yml` 的 `ports` 中修改（例如 `"18000:8000"` 改为宿主机 18000）。
服务不监听其它端口；SQLite 数据经命名卷 `coordinator-data` 持久化在容器内 `/data`，
不对外暴露。

## 核心语义

- **会话**：创建时必须提供 `tenant_id`（租户标识）与 `declared_bytes`（本会话声明占用的字节数，允许 0），并登记 `total_chunks`（总分片数）与 `file_digest`（期望的完整文件摘要），分片序号从 `0` 开始。`tenant_id` 必须事先通过 `PUT /tenants/{tenant_id}/quota` 设置过配额，否则返回 `404`。可选 `reservation_ttl_seconds` 覆盖容量预留有效期（缺省取服务端默认 24h、上限 7 天，超出截断）。可选 `digest_algorithm`（默认 `sha256`，仅作记录；取 `sha256-merkle-v1` 时启用提交时的 Merkle 完整性校验）。可选失败退避参数 `base_retry_seconds`（默认 `5`，必须为正）与 `max_retry_seconds`（默认 `300`，必须为正且不小于 base）。
- **依赖（可选）**：创建会话时可提交 `dependencies`（键与值均为分片序号），声明某分片领取租约前必须完成的前置分片。越界、自依赖、有环的依赖图会被拒绝（`422`）；不提供时保持无依赖的原有行为。
- **租约**：同一分片同一时刻仅允许一个有效租约；租约过期后可被重新领取（令牌轮换）。前置分片未全部完成时领取返回 `409` 及按序缺失的 `missing_dependencies`；前置全部完成后无需额外操作即可领取。
- **失败上报与退避重试（可选自动生效）**：`POST /sessions/{id}/chunks/{index}/fail` 必须携带该分片**当前有效租约的令牌**；校验通过后在同一事务内**原子清除租约、`failure_count` 加 1**，并将分片置于退避冷却中，第 n 次失败冷却 `min(max_retry_seconds, base_retry_seconds × 2^(n-1))` 秒（默认序列 5、10、20、40、80、160、300、300…）。冷却中的分片**单片与批量领取均跳过/拒绝**，`/ready` 单独列出 `cooling_chunks`；冷却结束后无需干预即可重新领取。令牌不匹配、租约已过期、分片已完成或会话已提交均返回 `409`。
- **完成上报**：必须携带该分片**当前有效租约的令牌**与分片摘要；以相同令牌 + 相同摘要重复上报为幂等成功（返回 `idempotent_replay: true`），摘要不同则返回 `409`。完成后**清除退避冷却但保留 `failure_count`**。普通会话完成即 `completed`；**启用分片验收队列**的会话先进入 `awaiting_verification`（见下文「分片验收队列」）。
- **完成纠错回退（reopen）**：`POST /sessions/{id}/chunks/{index}/reopen` 凭目标分片**完成时保留的租约令牌**与 `expected_digest`（完成时上报的摘要）发起，仅允许**未提交会话中已完成且凭据匹配**的分片。校验通过后服务端沿依赖图找出**全部传递下游**，在**同一事务**内将目标及下游重置为 `pending`：清除摘要、完成时间、有效租约与退避截止时间，**保留累计失败次数**；同时会话 `revision` 递增 1。响应中的 `affected_chunks` 按**（拓扑层级, 分片序号）**升序稳定排列（目标层级为 0，层级取受影响子图中的最长路径）。任一校验失败（会话已提交 / 分片未完成 / 凭据不匹配）返回 `409` 且**不产生任何部分回退**；整个回退与并发领取互斥，领取方不会看到中间状态。
- **进度分支（fork）**：`POST /sessions/{id}/fork` 从一个 **`active`** 会话分叉出全新的上传会话，请求体包含 `expected_revision`（调用方认为源会话所处的修订号，不匹配返回 `409` 及 `current_revision`）、幂等键 `request_id` 与 `reuse_chunks`（要复用进度的分片序号列表，可为空）。仅当 `reuse_chunks` 中**每一项都已在源会话完成**（越界序号按未完成处理，响应以 `not_completed_chunks` 返回），且集合**包含这些分片的全部传递前置分片**（缺失项以 `missing_prerequisites` 升序返回）时才会创建；任一校验失败返回 `409` 且**不落库**。新会话复制源会话的**全部配置**（总分片数、文件摘要、摘要算法、并发上限、退避参数）与**完整依赖图**：复用分片的**摘要与完成时间**复制为 `completed`，其余分片初始化为 `pending`；**不复制**租约（令牌/过期时间）、退避冷却与累计失败次数。新会话 `revision` 从 0 开始、状态为 `active`，与源会话相互独立。**同一源会话 + 同一 `request_id` 的重放**（含仅顺序/重复不同的 `reuse_chunks`）返回**首次创建的新会话**（`200`、`idempotent_replay: true`）；相同键但 `expected_revision` 或 `reuse_chunks` 变化返回 `409`。已提交（`finalized`）的会话不可再分支。
- **会话级并发上限（可选）**：创建会话时可设置 `max_active_leases`，限制该会话**当前有效（未过期）租约**的总数。单片领取在槽位占满时返回 `409`；批量领取则原子地领取至多 `min(count, 剩余槽位, 可领取分片数)` 个租约。不设置（`null`）时保持原有不限并发的行为。
- **批量领取**：`POST /sessions/{id}/leases/batch` 在**同一临界区**内完成资格计算与写入：仅从“未完成、无有效租约（过期租约视为空闲）、**不在退避冷却中**、前置已全部完成”的分片中，按分片序号升序领取，返回各分片独立令牌；并发请求之间不会重复分配、也不会突破并发上限。无可领取项时返回空列表及 `active` / `blocked` / `next_expiry` 汇总，其中 **`next_expiry` 取有效租约最早过期时间与各分片退避最早结束时间中的更早者**。
- **跨会话公平调度**：`POST /dispatch/lease` 面向多个上传会话组成的调度队列，每次调用从候选会话中**轮转发一片分片租约**。请求携带 `queue_key`（队列标识）、`session_ids`（候选会话列表）与可选 `lease_ttl_seconds`。服务端将候选**去重后按（创建时间, id）稳定排序**，并按 `queue_key` 持久化**最近一次成功发放的会话**作为游标；每次请求在**同一写锁事务**内从游标之后的会话开始**循环扫描**，命中首个“存在可领取分片且仍有并发槽位”的会话后，发放其**序号最低**的可领取分片。资格判定完全复用既有规则：未完成、无有效租约（过期视为空闲）、不在退避冷却中、前置全部完成、会话未提交且未达 `max_active_leases`，因此并发请求**不会重复发放**。**成功后才把游标推进到被发放会话**，连续请求会在可用会话之间轮转；**无可发放项时不推进游标**，响应中 `granted` 为 `null`、`unavailable` 按扫描顺序列出各会话原因（`session_not_found` / `session_finalized` / `session_full` / `chunks_blocked_by_dependencies` / `chunks_in_backoff` / `chunks_leased` / `no_unfinished_chunks`）与各会话的 `next_retry_at`，顶层 `next_retry_at` 取其中最早者。游标所指会话不在本次候选中时，从排序首位重新开始扫描。
- **提交**：仅当全部分片完成才允许提交，返回按分片序号升序汇总的清单；否则返回 `409` 及 `missing_chunks` 缺失分片列表。重复提交幂等返回同一清单。会话使用 `sha256-merkle-v1` 时，提交前先按分片序号构建 Merkle 树并与 `file_digest` 常量时间比较，不匹配返回 `409` 及 `computed_digest`，会话保持 `active`（见下文「完整性校验」）。提交在同一写事务内把会话状态置为 `finalized`：其 `declared_bytes` 由此**从租户预留量（reserved）原子转为已用量（used）**。
- **取消**：`POST /sessions/{id}/cancel` **仅允许 `active` 会话**。取消在同一写事务内**原子释放该会话的容量预留**（`declared_bytes` 从租户 reserved 中移除）并使会话的**全部在途分片租约与验收令牌立即失效**（`leased` 分片回 `pending`；`awaiting_verification` 分片按验收 reject 同口径清除摘要回 `pending`；`completed` 分片摘要保留作只读审计）。`finalized` 会话不可取消（`409`，已用量不能通过取消释放）。**重复取消幂等**：再次调用返回 `200`、`idempotent_replay: true`、`released_bytes: 0` 与首次 `cancelled_at`。预留已到期的会话也可取消（幂等同口径，不重复释放）。
- **交付凭证（delivery ticket，一次性清单领取）**：已 `finalized` 的会话可通过 `POST /sessions/{id}/delivery-tickets`（携带 `expires_in_seconds`，1～365 天）签发**一次性清单领取凭证**。服务端生成高强度随机令牌（`secrets.token_urlsafe(32)`），**明文令牌仅在签发响应中返回一次**；数据库内**只保存令牌的 SHA-256、所属会话、到期时间与状态**（`active` / `consumed` / `revoked`），明文遗失不可找回。同一会话可签发多张相互独立的凭证；`active`（未提交）会话不能签发（`409`）。
  - **兑换**：`POST /delivery-tickets/redeem` 携带 `token` 与调用方生成的幂等键 `request_id`，在**同一写事务**内依次校验**未消费、未撤销、未过期**，通过后原子地把凭证置为 `consumed` 并返回该会话**按分片序号升序的 manifest**（与 finalize 清单一致）及 `redeemed_at`。**并发的不同 `request_id` 只有一个成功**（其余 `409 already consumed`）；**同一 `request_id` 的重放（含并发重试）返回与首次完全相同的结果**——首次响应被原样持久化，即使此后凭证已过期或被撤销。已撤销（`409 revoked`）、已过期（`409 expired`）或已被其它 `request_id` 消费（`409 already consumed`）的凭证均不可领取；未知令牌返回 `404`。
  - **撤销**：`POST /delivery-tickets/revoke` 携带 `token`，将未消费凭证置为 `revoked`（**已消费凭证不可撤销**，返回 `409`；已过期但未消费的凭证仍可撤销）；对已撤销凭证重复撤销幂等成功（`already_revoked: true`）。撤销后任何兑换请求都返回 `409`。
- **已提交清单差异（manifest diff）**：`POST /manifests/diff` 接收 `base_session_id` 与 `target_session_id`，仅比较两个 **finalized** 会话按分片序号排列的 `chunk_digest` 序列，返回**插入+删除次数之和最小**的编辑脚本（多解时**固定优先删除再插入**），连续同类操作合并为携带 base/target **半开区间**与对应摘要的区段——单处分片插入只产生一个 insert 区段，其后的相同分片仍归入 equal 区段，不会全部误报。会话不存在 `404`、未提交 `409`、任一清单超过 **2000** 项 `422`；两份清单完全一致时返回覆盖整个清单的**单个 equal 区段**（见下文「已提交清单差异」）。
- 已完成的分片不可再领取租约；已提交的会话进入只读状态。

## 分片验收队列（verification_required，可选）

创建会话时设置 `"verification_required": true` 即启用**分片验收队列**；缺省（`false`）时
完成上报直接转 `completed`，全部既有行为保持不变。启用后上传与验收解耦：上报方负责传分片，
验收方按队列独立审核。

启用后的状态机：

```
pending --(领取上传租约)--> leased --(complete：令牌+摘要匹配)--> awaiting_verification
                                                                   |
                              +------------------------------------+-----------------------------------+
                              |                                                                                                |
                  POST /verifications/lease 领取并签发有 TTL 的验收令牌（FIFO，全局队列）                                       |
                              |                                                                                                |
                  decide approve                                              decide reject
                              |                                                                                                |
                              v                                                                                                v
                        completed（沿用上报摘要/完成时间）                              pending（原子清除摘要/完成时间与上传租约，
                                                                                         保留 failure_count，可立即重新领取重传）
```

规则要点：

- **完成上报先待验收**：启用会话的 complete 成功后分片为 `awaiting_verification`（幂等重放
  亦返回该状态），**不再直接 `completed`**。待验收分片：
  - 不能再次领取上传租约、不能 `fail`、不能 `reopen`（均 `409`，驳回请走 reject）；
  - **不计入** `/ready` 的 `ready_chunks` / `blocked_chunks` / `cooling_chunks`，而单独列入
    `awaiting_verification_chunks`；会话状态接口同时给出会话级 `awaiting_verification_chunks`
    列表与逐分片的 `verification_expires_at`（当前验收令牌到期时间，未领取/已过期为 `null`）；
  - 对依赖图而言仍是「未完成」：下游分片的前置若处于待验收，下游依旧不可领取；
  - 跨会话调度跳过这种会话，原因报告为 `chunks_awaiting_verification`。
- **未验收不得提交**：只要存在非 `completed` 分片（含待验收），`finalize` 返回 `409` 且
  `missing_chunks` 包含待验收分片；全部 approve 后才可正常提交（含 Merkle 校验）。
- **领取验收任务**：`POST /verifications/lease`（请求体可省略，或携带 `lease_ttl_seconds`）
  在**同一写锁事务**内跨全部启用会话，按**完成时间从早到晚**（`completed_at`，同刻以
  `session_id`、`chunk_index` 破序）领取**一个**当前可领取的待验收分片，返回
  `session_id` / `chunk_index` / `chunk_digest`（供验收方核对）/ `completed_at` 与一枚
  **一次性验收令牌** `verification_token`（明文仅此一次返回，库内只存其 SHA-256）及
  `issued_at` / `expires_at`。
  - 未持有有效验收令牌（从未领取或令牌已过期）的分片才可领取；**未过期令牌独占**，
    重复或并发请求**不会重复领取**——其余分片被领完后返回 `409 no awaiting verification
    chunk available`，不做任何写入。
  - **过期可重领**：令牌到期后分片仍是 `awaiting_verification`，任意请求可重新领取，
    旧令牌随即失效（同一分片的租约行被原子换发，库内每分片至多一行）。
- **作出决定**：`POST /verifications/{session_id}/{chunk_index}/decide` 携带
  `verification_token` 与 `decision`（`approve` / `reject`）：
  - `approve`：分片转 **`completed`**，摘要与完成时间沿用完成上报值；
  - `reject`：**在同一事务内原子清除摘要与完成时间**、分片回到 **`pending`**（同时清除
    上传租约与退避冷却，**保留累计失败次数**），可立即重新领取上传租约重传；
  - 令牌必须是该分片**当前未过期**的验收令牌：未知/不匹配/已过期/已被换发均 `409`；
    分片已不在待验收状态返回 `409`；会话或分片不存在返回 `404`。
  - **同一令牌同一决定重放幂等（返回首次响应快照）**：首次 approve/reject 成功时，
    服务在**与分片状态变更同一笔事务**内把首次响应（`status`、`chunk_digest`、
    `decided_at`）持久化为决定快照。之后同一令牌同一决定的重放**只读快照、不再读
    分片现态**——即使此后分片被 `reopen` 重置、会话被取消或容量预留到期，重放始终
    返回 **HTTP 200** 与首次的 `status` / `chunk_digest` / `decided_at`
    （`idempotent_replay: true`），并发重放同样安全。校验顺序为：会话/分片存在性
    （404）→ 决定记录（命中即按快照应答，先于会话状态、预留到期与分片现态校验）→
    首次决定路径的其余校验。
  - **同一令牌携带冲突决定返回 `409`**（响应体带首次的 `original_decision`），同样
    不依赖分片当前状态；分片维持首次决定结果。
- **进度分支**：fork 复制会话的 `verification_required` 开关；待验收分片不属于
  `completed`，不能作为 `reuse_chunks` 复用（返回 `409 not_completed_chunks`）。
- **未启用会话完全不受影响**：complete 直接 `completed`，`/verifications/lease` 在全局
  没有待验收分片时返回 `409`。

调用示例（curl）：

```bash
# 1. 创建启用验收队列的会话并逐分片领取/上报完成（complete 返回 awaiting_verification）
#    创建会话前需先 PUT /tenants/tenant-a/quota 设置配额
SID=$(curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' \
  -d '{"total_chunks": 2, "file_digest": "<文件摘要>", "verification_required": true,
       "tenant_id": "tenant-a", "declared_bytes": 1048576}' \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["id"])')
TOKEN=$(curl -s -X POST http://localhost:8000/sessions/$SID/chunks/0/lease \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["lease_token"])')
curl -s -X POST http://localhost:8000/sessions/$SID/chunks/0/complete \
  -H 'Content-Type: application/json' \
  -d "{\"lease_token\": \"$TOKEN\", \"chunk_digest\": \"<分片0摘要>\"}"
# => {"status": "awaiting_verification", ...}

# 2. 此时不能提交：missing_chunks 包含待验收分片
curl -s -X POST http://localhost:8000/sessions/$SID/finalize
# => 409 {"detail": "upload incomplete", "missing_chunks": [0, 1]}

# 3. 验收方按完成时间 FIFO 领取一个待验收分片（TTL 600 秒）
VT=$(curl -s -X POST http://localhost:8000/verifications/lease \
  -H 'Content-Type: application/json' -d '{"lease_ttl_seconds": 600}')
echo "$VT"
# => 201 {"session_id": "...", "chunk_index": 0, "chunk_digest": "<分片0摘要>",
#         "verification_token": "Rk9...（仅此一次）", "issued_at": "...", "expires_at": "..."}
VTOKEN=$(echo "$VT" | python3 -c 'import sys, json; print(json.load(sys.stdin)["verification_token"])')

# 4a. 验收通过 -> completed；重放同决定幂等；冲突决定 409
curl -s -X POST http://localhost:8000/verifications/$SID/0/decide \
  -H 'Content-Type: application/json' \
  -d "{\"verification_token\": \"$VTOKEN\", \"decision\": \"approve\"}"
# => {"decision": "approve", "status": "completed", "chunk_digest": "<分片0摘要>",
#     "decided_at": "<首次决定时间>", "idempotent_replay": false, ...}

# 4a-续. 即使此后分片被 reopen 回 pending、会话被取消或预留到期，
# 同一令牌同一决定重放仍返回首次响应快照（200，idempotent_replay=true），
# status/chunk_digest/decided_at 与首次完全一致；冲突决定仍为 409
curl -s -X POST http://localhost:8000/verifications/$SID/0/decide \
  -H 'Content-Type: application/json' \
  -d "{\"verification_token\": \"$VTOKEN\", \"decision\": \"approve\"}"
# => {"decision": "approve", "status": "completed", "chunk_digest": "<分片0摘要>",
#     "decided_at": "<首次决定时间>", "idempotent_replay": true, ...}
curl -s -X POST http://localhost:8000/verifications/$SID/0/decide \
  -H 'Content-Type: application/json' \
  -d "{\"verification_token\": \"$VTOKEN\", \"decision\": \"reject\"}"
# => 409 {"detail": "verification token already used with a conflicting decision",
#         "original_decision": "approve"}

# 4b. 或验收驳回 -> pending（摘要被清除，可重新领取上传租约重传）
curl -s -X POST http://localhost:8000/verifications/$SID/0/decide \
  -H 'Content-Type: application/json' \
  -d "{\"verification_token\": \"$VTOKEN\", \"decision\": \"reject\"}"
# => {"decision": "reject", "status": "pending", "chunk_digest": null, ...}

# 5. 状态与就绪查询区分待验收分片
curl -s http://localhost:8000/sessions/$SID
# => {"verification_required": true, "awaiting_verification_chunks": [0],
#     "chunks": [{"chunk_index": 0, "status": "awaiting_verification",
#                 "verification_expires_at": "2026-...（未过期令牌）", ...}, ...], ...}
curl -s http://localhost:8000/sessions/$SID/ready
# => {"ready_chunks": [], "blocked_chunks": [], "cooling_chunks": [],
#     "awaiting_verification_chunks": [0]}

# 6. 验收令牌过期后可被重新领取（换发新令牌，旧令牌失效）；全部分片 approve 后即可提交
curl -s -X POST http://localhost:8000/sessions/$SID/finalize
```

## 租户容量预留与硬配额（tenant quota）

每个上传会话从属于一个租户，并在创建时**原子预留**声明的字节容量。容量口径：

| 指标 | 含义 | 数据来源 |
|---|---|---|
| `quota_bytes`（limit） | 租户硬配额（字节），由配额接口 UPSERT 设置 | `tenants.quota_bytes` |
| `used_bytes`（used） | 已用量：全部 `finalized` 会话 `declared_bytes` 之和 | 按 `sessions` 实时聚合 |
| `reserved_bytes`（reserved） | 预留量：全部**未到期 `active`** 会话 `declared_bytes` 之和 | 按 `sessions` 实时聚合 |
| `available_bytes`（available） | 当前可预留容量：`quota_bytes − used_bytes − reserved_bytes` | 计算值，恒非负 |

规则要点：

- **创建即预留**：`POST /sessions` 必填 `tenant_id` 与 `declared_bytes`（允许 `0`）。
  服务端在**同一个 SQLite 写事务（进程内写锁）**内完成「读取租户配额 → 聚合 used/reserved
  → 校验 `used + reserved + declared_bytes ≤ quota_bytes` → 写入会话（预留）」。
  并发创建请求经同一写锁串行化，每个请求都基于最新聚合值判定，**并发创建绝不超额**；
  超额返回 `409`，响应体带 `quota_bytes` / `used_bytes` / `reserved_bytes` /
  `requested_bytes` / `available_bytes`，且不落库。未知租户（从未设置配额）返回 `404`。
- **预留 TTL（可配置）**：创建会话可用 `reservation_ttl_seconds` 指定预留有效期，
  缺省取 `DEFAULT_RESERVATION_TTL_SECONDS`（24 小时），超过
  `MAX_RESERVATION_TTL_SECONDS`（7 天）截断，均可用环境变量配置。创建响应与状态查询
  返回 `reservation_expires_at`。
- **到期释放（惰性）**：未提交会话超过预留 TTL 后，其预留**立即不再计入 reserved**
  （容量可被同租户新会话使用）；会话行保留作审计，首次对该会话的写操作（领取/上报/
  失败/回退/提交/fork/取消/调度命中）会在同一事务内把它**原子标记为 `cancelled`**
  并清除全部在途租约，此后与显式取消一致拒绝一切写入（`409`）。状态查询中
  `reservation_expired: true` 表示「仍为 active 但预留已到期」。到期动作不需要后台
  清扫线程，账务在读取时按 `reservation_expires_at > now` 口径实时聚合。
- **提交原子转量**：`finalize` 成功时会话 `active → finalized`，其 `declared_bytes`
  在同一事务内**从 reserved 移到 used**（两者都是按状态实时 SUM，无独立计数器，
  不存在中间态）。提交后容量不释放——它代表已确认占用的用量。
- **取消释放**：`active` 会话可 `POST /sessions/{id}/cancel` 取消（见上文「取消」），
  预留原子释放、租约全部失效；重复取消幂等。
- **配额接口**：
  - `PUT /tenants/{tenant_id}/quota`，请求体 `{"quota_bytes": <非负整数>}`，
    UPSERT 设置硬配额；**降低配额时不得小于当前 `used_bytes + reserved_bytes`**，
    否则 `409`（响应体带当前 used/reserved 与 `minimum_quota_bytes`）且不修改。
    上调与首次创建始终允许。
  - `GET /tenants/{tenant_id}/quota` 返回 `tenant_id` / `quota_bytes` / `used_bytes`
    / `reserved_bytes` / `available_bytes`，以及观测用 `active_session_count`
    （未到期 active 会话数）与 `created_at` / `updated_at`。未知租户 `404`。
- **fork 与配额**：fork 出的新会话归属**源租户**、复制源会话的 `declared_bytes`，
  按一笔全新预留重新校验硬配额（TTL 取服务端默认）；配额不足返回 `409` 且不落库。
  同一 `request_id` 的幂等重放不重复预留。

容量状态机：

```
                     创建（同事务校验硬配额）
        (无) ───────────────────────────────► reserved（active，计入 reserved）
                                              │
                  finalize（全部分片完成）      │  TTL 到期（惰性，首次写访问时落终态）
                  原子转量，同事务             │  或 POST /cancel（仅 active）
                                              ▼
                                          used（finalized，计入 used，不可取消）
                                              │
                                              └─► released（cancelled，不占任何容量；
                                                           在途租约/验收令牌全部失效）
```

调用示例（curl）：

```bash
# 1. 为租户设置硬配额 10 GiB（首次调用即创建租户）
curl -s -X PUT http://localhost:8000/tenants/tenant-a/quota \
  -H 'Content-Type: application/json' -d '{"quota_bytes": 10737418240}'
# => 200 {"tenant_id":"tenant-a","quota_bytes":10737418240,
#         "used_bytes":0,"reserved_bytes":0,"available_bytes":10737418240,
#         "active_session_count":0,"created_at":"...","updated_at":"..."}

# 2. 创建会话：必填 tenant_id / declared_bytes，预留 1 GiB、预留 TTL 1 小时
SID=$(curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' \
  -d '{"total_chunks": 4, "file_digest": "<文件摘要>",
       "tenant_id": "tenant-a", "declared_bytes": 1073741824,
       "reservation_ttl_seconds": 3600}' \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["id"])')

# 3. 查询配额：reserved=1GiB、available 相应减少
curl -s http://localhost:8000/tenants/tenant-a/quota
# => {"quota_bytes":10737418240,"used_bytes":0,
#     "reserved_bytes":1073741824,"available_bytes":9663676416,...}

# 4a. 正常上传并提交：预留原子转为已用量
curl -s -X POST http://localhost:8000/sessions/$SID/finalize   # 分片全部完成后
# => used_bytes=1073741824, reserved_bytes=0

# 4b. 或在提交前取消：原子释放预留并使所有在途租约失效
curl -s -X POST http://localhost:8000/sessions/$SID/cancel
# => 200 {"session_id":"...","status":"cancelled","released_bytes":1073741824,
#         "cancelled_at":"...","idempotent_replay":false}
curl -s -X POST http://localhost:8000/sessions/$SID/cancel
# => 200 {"status":"cancelled","released_bytes":0,"idempotent_replay":true,...}

# 5. 降低配额不得小于 used+reserved：返回 409 且不修改
curl -s -X PUT http://localhost:8000/tenants/tenant-a/quota \
  -H 'Content-Type: application/json' -d '{"quota_bytes": 1}'
# => 409 {"detail":"quota_bytes cannot be less than used_bytes + reserved_bytes",
#         "used_bytes":...,"reserved_bytes":...,"minimum_quota_bytes":...}

# 6. 预留到期（超过 reservation_ttl_seconds 未提交）：容量自动可用；
#    对该会话的任何写操作返回 409，会话被标记为 cancelled
```

## 完整性校验（sha256-merkle-v1，可选）

创建会话时将 `digest_algorithm` 设为 `sha256-merkle-v1` 即启用端到端完整性校验；
其余取值（含默认的 `sha256`）仅作记录，行为与之前完全一致。

启用后的约束与规则：

- **摘要格式**：创建会话的 `file_digest` 与每次 complete 的 `chunk_digest` 必须是
  **64 位小写十六进制**（SHA-256 的 32 字节十六进制编码），否则返回 `422`。
  `chunk_digest` 应取该分片原始内容的 SHA-256。
- **提交时校验**：finalize 按分片序号升序读取各 `chunk_digest`，构建 Merkle 树：
  - 叶节点：`leaf = SHA-256(0x00 || chunk_digest 的原始 32 字节)`
  - 父节点：`parent = SHA-256(0x01 || 左节点 32 字节 || 右节点 32 字节)`
  - 每层节点数为奇数时，**末节点复制自身**后配对
  - 逐层归约直至单一根（单分片会话的根即唯一叶节点）
- **比较与结果**：根的小写十六进制与 `file_digest` 做**常量时间比较**。
  不匹配返回 `409` 及 `computed_digest`（服务端算出的根），**不做任何写入、会话保持
  `active`**，可通过 reopen 纠错后重试；匹配才沿用现有的原子提交与幂等响应。

参考计算（Python）：

```python
import hashlib

def merkle_root(chunk_digests_hex):
    level = [hashlib.sha256(b"\x00" + bytes.fromhex(d)).digest()
             for d in chunk_digests_hex]
    while len(level) > 1:
        if len(level) % 2 == 1:
            level.append(level[-1])            # 奇数末节点复制自身
        level = [hashlib.sha256(b"\x01" + level[i] + level[i + 1]).digest()
                 for i in range(0, len(level), 2)]
    return level[0].hex()
```

调用示例（curl）：

```bash
# 1. 客户端先算出每个分片的 SHA-256（64 位小写十六进制），再算出 Merkle 根
ROOT=$(python3 -c '...')   # 上述 merkle_root([d0, d1, d2, ...])

# 2. 创建启用完整性校验的会话，file_digest 登记 Merkle 根
#    （需先 PUT /tenants/tenant-a/quota 设置配额）
SID=$(curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' \
  -d "{\"total_chunks\": 3, \"file_digest\": \"$ROOT\",
       \"digest_algorithm\": \"sha256-merkle-v1\",
       \"tenant_id\": \"tenant-a\", \"declared_bytes\": 1048576}" \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["id"])')

# 3. 逐分片领取租约并上报完成；chunk_digest 必须是 64 位小写十六进制
curl -s -X POST http://localhost:8000/sessions/$SID/chunks/0/complete \
  -H 'Content-Type: application/json' \
  -d "{\"lease_token\": \"$TOKEN\", \"chunk_digest\": \"$D0\"}"

# 4. 提交：根与 file_digest 匹配则正常提交；不匹配返回 409 与 computed_digest，
#    会话保持 active，可 reopen 出错分片、重新上报后再次提交
curl -s -X POST http://localhost:8000/sessions/$SID/finalize
# => 409 {"detail": "file digest mismatch", "computed_digest": "<算出的根>"}
```

状态查询与创建/提交响应均暴露 `digest_algorithm` 字段。旧 SQLite 数据无需重写：
既有会话的 `digest_algorithm` 不是 `sha256-merkle-v1`，行为完全不变。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| `PUT` | `/tenants/{tenant_id}/quota` | 设置（UPSERT）租户硬配额 `quota_bytes`；降低不得小于 used+reserved（否则 409） |
| `GET` | `/tenants/{tenant_id}/quota` | 查询租户配额：`quota_bytes`/`used_bytes`/`reserved_bytes`/`available_bytes` 与 active 会话数；未知租户 404 |
| `POST` | `/sessions` | 创建上传会话（**必填 `tenant_id`/`declared_bytes`**，原子校验硬配额并预留容量；可选 `reservation_ttl_seconds`、`dependencies`、`max_active_leases`、退避参数、`verification_required`） |
| `GET` | `/sessions/{session_id}` | 查询会话与逐分片状态（含租户/预留字段、各分片 `dependencies`、`failure_count`、`retry_after`、退避参数、当前 `active_leases`、会话 `revision` 与 `source_session_id`） |
| `POST` | `/sessions/{session_id}/cancel` | 取消 active 会话：原子释放容量预留并使全部在途租约/验收令牌失效（finalized 409；重复取消幂等） |
| `POST` | `/sessions/{session_id}/fork` | 进度分支：从 active 会话创建新会话（同租户、复制 declared_bytes 并重新预留；校验修订号/完成状态/传递前置闭包/配额，`request_id` 幂等） |
| `GET` | `/sessions/{session_id}/ready` | 当前可领取的分片、被依赖阻塞的分片，以及退避冷却中的分片（`cooling_chunks`） |
| `POST` | `/sessions/{session_id}/chunks/{index}/lease` | 领取分片租约（同样受 `max_active_leases` 与退避冷却约束） |
| `POST` | `/sessions/{session_id}/leases/batch` | 原子批量领取租约（按分片序号，数量不超过 `count` 与剩余并发槽位；自动跳过冷却分片） |
| `POST` | `/dispatch/lease` | 跨会话公平调度：在候选会话间轮转发一片分片租约（按 `queue_key` 持久化游标，自动跳过不可用/已取消/预留到期会话） |
| `POST` | `/sessions/{session_id}/chunks/{index}/complete` | 上报分片完成（幂等；清除退避、保留失败次数；启用验收队列时先转 awaiting_verification） |
| `POST` | `/sessions/{session_id}/chunks/{index}/reopen` | 完成纠错回退（凭完成时的令牌与摘要，原子重置目标及全部传递下游，`revision` 递增 1） |
| `POST` | `/sessions/{session_id}/chunks/{index}/fail` | 上报分片失败（校验有效租约，原子清除租约并进入指数退避） |
| `POST` | `/sessions/{session_id}/finalize` | 提交会话，返回按序分片清单（同事务把预留原子转为已用量；`sha256-merkle-v1` 会话先校验 Merkle 根，不匹配返回 409 及 `computed_digest`） |
| `POST` | `/sessions/{session_id}/delivery-tickets` | 为已 finalized 会话签发一次性清单领取凭证（明文 token 仅此一次返回，库内只存 SHA-256/会话/到期/状态） |
| `POST` | `/delivery-tickets/redeem` | 凭 token + 幂等 `request_id` 兑换按序 manifest（一次性；并发不同 request_id 仅一个成功，同 request_id 重放返回首次结果） |
| `POST` | `/delivery-tickets/revoke` | 撤销未消费凭证（已消费不可撤销；重复撤销幂等，撤销后不可兑换） |
| `POST` | `/manifests/diff` | 比较两个已提交会话的按序分片清单：返回插入+删除次数之和最小的编辑脚本（多解时固定优先删除再插入），连续同类操作合并为携带半开区间与摘要的区段；`404`/`409`/`422` |
| `POST` | `/verifications/lease` | 按完成时间 FIFO 领取一个待验收分片并签发有 TTL 的验收令牌（未过期独占、过期可重领、并发不重复） |
| `POST` | `/verifications/{session_id}/{chunk_index}/decide` | 凭当前验收令牌 approve（转 completed）或 reject（清除摘要回 pending）；首次响应快照与状态变更同事务持久化，同令牌同决定在 reopen/取消/预留到期后重放仍返回首次快照（200），冲突决定 409 |
| `GET` | `/healthz` | 健康检查 |

## 批量领取与并发上限（curl）

```bash
# 1. 创建会话：8 个分片，同一时刻最多 3 个有效租约（需先设置租户配额）
SID=$(curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' \
  -d '{"total_chunks": 8, "file_digest": "<文件SHA256>", "max_active_leases": 3,
       "tenant_id": "tenant-a", "declared_bytes": 1073741824}' \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["id"])')

# 2. 一次申请 5 个、TTL 120 秒：实际只发 3 个（受剩余并发槽位限制），按分片序号升序返回
curl -s -X POST http://localhost:8000/sessions/$SID/leases/batch \
  -H 'Content-Type: application/json' -d '{"count": 5, "lease_ttl_seconds": 120}'
# => {
#      "requested": 5,
#      "granted": [
#        {"chunk_index": 0, "lease_token": "...", "expires_at": "..."},
#        {"chunk_index": 1, "lease_token": "...", "expires_at": "..."},
#        {"chunk_index": 2, "lease_token": "...", "expires_at": "..."}
#      ],
#      "active": 3, "blocked": 0, "next_expiry": "2026-...Z",
#      "max_active_leases": 3, "remaining_slots": 0
#    }

# 3. 槽位占满时再申请：返回空列表与汇总，可按 next_expiry 等待重试
curl -s -X POST http://localhost:8000/sessions/$SID/leases/batch \
  -H 'Content-Type: application/json' -d '{"count": 5}'
# => {"requested": 5, "granted": [], "active": 3, "blocked": 0,
#     "next_expiry": "2026-...Z", "max_active_leases": 3, "remaining_slots": 0}

# 4. 用令牌上报完成后，槽位与后续分片同时释放，再批量领取即可拿到新的分片
#    （单片接口 POST .../chunks/{index}/lease 同样占用槽位，占满时返回 409）

# 5. 状态查询暴露并发上限与当前有效租约数（过期租约不计入）
curl -s http://localhost:8000/sessions/$SID
# => {"max_active_leases": 3, "active_leases": 3, ...}
```

依赖图与批量领取可以组合使用：被前置依赖阻塞的分片不会进入 `granted`，
其数量汇总在响应的 `blocked` 中；前置分片陆续完成后会自动具备领取资格。
不设置 `max_active_leases` 的会话中，`max_active_leases` 与 `remaining_slots` 均为 `null`，
批量领取只受 `count` 与可领取分片数约束。

## 跨会话公平调度（curl）

一个调度队列（`queue_key`）对应一组相互轮转的上传会话：调用方不必记住上次派给了谁，
服务端持久化游标并在**同一写锁事务**内循环扫描候选，保证并发请求不重复发放、
连续请求在可用会话之间轮转。

```bash
# 1. 三个会话（可以由不同客户端创建），各有若干分片
A=<会话A的id>; B=<会话B的id>; C=<会话C的id>

# 2. 首次调度：候选去重后按（创建时间, id）排序，从最早的会话开始发放最低序号分片
curl -s -X POST http://localhost:8000/dispatch/lease \
  -H 'Content-Type: application/json' \
  -d "{\"queue_key\": \"workers-zone-1\",
       \"session_ids\": [\"$A\", \"$B\", \"$C\"],
       \"lease_ttl_seconds\": 120}"
# => 200 {
#      "queue_key": "workers-zone-1",
#      "granted": {"session_id": "<A>", "chunk_index": 0,
#                  "lease_token": "...", "expires_at": "2026-..."},
#      "cursor_session_id": "<A>", "unavailable": [], "next_retry_at": null
#    }

# 3. 连续请求自动轮转：第二次从游标（A）之后的会话开始 -> B，再 -> C，再回到 A
curl -s -X POST http://localhost:8000/dispatch/lease \
  -H 'Content-Type: application/json' \
  -d "{\"queue_key\": \"workers-zone-1\", \"session_ids\": [\"$A\", \"$B\", \"$C\"]}"
# => granted.session_id == B，cursor_session_id == B

# 4. 轮到的会话暂不可用（分片全在租约/冷却中、槽位占满、已提交、已完成、不存在等）
#    时，自动跳过并继续向后扫描；全部候选都不可发放时 granted=null，游标不推进，
#    响应给出每个会话的原因与最早可重试时刻：
curl -s -X POST http://localhost:8000/dispatch/lease \
  -H 'Content-Type: application/json' \
  -d "{\"queue_key\": \"workers-zone-1\", \"session_ids\": [\"$A\", \"$B\", \"$C\"]}"
# => 200 {
#      "granted": null,
#      "cursor_session_id": "<上次成功的会话，未变>",
#      "unavailable": [
#        {"session_id": "<C>", "reason": "chunks_leased",
#         "detail": "all eligible chunks hold an active lease",
#         "next_retry_at": "2026-...", "blocked": null,
#         "active_leases": null, "max_active_leases": null},
#        {"session_id": "<A>", "reason": "session_full",
#         "next_retry_at": "2026-...",
#         "active_leases": 2, "max_active_leases": 2},
#        {"session_id": "<B>", "reason": "chunks_in_backoff",
#         "next_retry_at": "2026-..."}
#      ],
#      "next_retry_at": "<全部会话中最早的可重试时刻>"
#    }

# 5. 不可用原因一览：
#    session_not_found                 会话不存在（不返回 4xx，按不可用项报告，排扫描序末尾）
#    session_finalized                 会话已提交（只读）
#    session_full                      有可领取分片但有效租约已达 max_active_leases
#    chunks_blocked_by_dependencies    未完成分片全部被未完成的前置阻塞
#    chunks_in_backoff                 前置已完成的未完成分片全部处于失败退避冷却
#    chunks_leased                     前置已完成的未完成分片全部持有有效租约
#    no_unfinished_chunks              全部分片已完成
#    （租约/冷却同时存在时，以更早到期的事件类型作为原因；
#      next_retry_at = min(最早租约过期, 最早退避结束)，无等待事件时为 null）

# 6. 不同 queue_key 的游标相互独立；游标所指会话不在本次候选中时从排序首位重新扫描
```

发放到的租约与单片/批量接口发放的完全一致：用 `lease_token` 调
`/sessions/{session_id}/chunks/{chunk_index}/complete` 或 `/fail` 上报即可，
退避、依赖与并发上限规则对三种领取方式同样生效。

## 调用示例（curl）

```bash
# 0. 为租户设置硬配额（首次调用即创建租户；字节）
curl -s -X PUT http://localhost:8000/tenants/tenant-a/quota \
  -H 'Content-Type: application/json' -d '{"quota_bytes": 10737418240}'

# 1. 创建会话：4 个分片，登记期望的文件 SHA-256，声明占用 1 GiB 并预留容量
SID=$(curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' \
  -d '{"total_chunks": 4, "file_digest": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
       "digest_algorithm": "sha256",
       "tenant_id": "tenant-a", "declared_bytes": 1073741824}' \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["id"])')

# 2. 领取分片 0 的租约（有效期 120 秒）
LEASE=$(curl -s -X POST http://localhost:8000/sessions/$SID/chunks/0/lease \
  -H 'Content-Type: application/json' -d '{"lease_ttl_seconds": 120}')
TOKEN=$(echo "$LEASE" | python3 -c 'import sys, json; print(json.load(sys.stdin)["lease_token"])')

# 3. 客户端自行传输该分片后，上报完成（携带租约令牌与分片摘要）
curl -s -X POST http://localhost:8000/sessions/$SID/chunks/0/complete \
  -H 'Content-Type: application/json' \
  -d "{\"lease_token\": \"$TOKEN\", \"chunk_digest\": \"<分片0的SHA256>\"}"

# 4. 对其余分片重复 2-3 步，然后提交会话（缺分片时返回 409 + missing_chunks）
curl -s -X POST http://localhost:8000/sessions/$SID/finalize

# 5. 随时查询状态
curl -s http://localhost:8000/sessions/$SID
```

## 依赖调度示例（curl）

```bash
# 1. 创建带依赖的会话：分片 2 依赖 0 和 1，分片 3 依赖 2（键/值均为分片序号）
#    （需先 PUT /tenants/tenant-a/quota 设置配额）
SID=$(curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' \
  -d '{"total_chunks": 4, "file_digest": "<文件SHA256>", "digest_algorithm": "sha256",
       "tenant_id": "tenant-a", "declared_bytes": 1048576,
       "dependencies": {"2": [0, 1], "3": [2]}}' \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["id"])')

# 2. 查看当前可领取与被阻塞的分片
#    => ready_chunks: [0, 1]
#    => blocked_chunks: [{chunk_index: 2, missing_dependencies: [0, 1]}, {chunk_index: 3, ...}]
curl -s http://localhost:8000/sessions/$SID/ready

# 3. 前置未完成时领取分片 2：返回 409 及按序缺失的前置分片
#    => {"detail": "chunk dependencies not completed", "missing_dependencies": [0, 1]}
curl -s -X POST http://localhost:8000/sessions/$SID/chunks/2/lease

# 4. 完成分片 0、1 后，分片 2 自动变为可领取，无需额外操作
TOKEN=$(curl -s -X POST http://localhost:8000/sessions/$SID/chunks/0/lease \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["lease_token"])')
curl -s -X POST http://localhost:8000/sessions/$SID/chunks/0/complete \
  -H 'Content-Type: application/json' \
  -d "{\"lease_token\": \"$TOKEN\", \"chunk_digest\": \"<分片0的SHA256>\"}"
# ... 同样完成分片 1，之后即可正常领取分片 2

# 5. 状态查询中每个分片都带有 dependencies 字段
curl -s http://localhost:8000/sessions/$SID
```

## 失败上报与退避重试（curl）

```bash
# 1. 创建会话时指定退避参数（缺省 base=5、max=300，须为正且 max≥base；需先设置租户配额）
SID=$(curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' \
  -d '{"total_chunks": 4, "file_digest": "<文件SHA256>",
       "base_retry_seconds": 5, "max_retry_seconds": 300,
       "tenant_id": "tenant-a", "declared_bytes": 1048576}' \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["id"])')

# 2. 领取租约后传输失败，凭“当前有效租约令牌”上报失败
TOKEN=$(curl -s -X POST http://localhost:8000/sessions/$SID/chunks/0/lease \
  -H 'Content-Type: application/json' -d '{}' \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["lease_token"])')
curl -s -X POST http://localhost:8000/sessions/$SID/chunks/0/fail \
  -H 'Content-Type: application/json' -d "{\"lease_token\": \"$TOKEN\"}"
# => {"status": "pending", "failure_count": 1,
#     "retry_after_seconds": 5, "retry_after": "2026-...Z"}
# 第 n 次失败冷却 min(300, 5×2^(n-1))：5、10、20、40、80、160、300、300…

# 3. 冷却结束前，单片领取返回 409，批量领取跳过该分片
curl -s -X POST http://localhost:8000/sessions/$SID/chunks/0/lease \
  -H 'Content-Type: application/json' -d '{}'
# => 409 {"detail": "chunk in failure backoff",
#         "failure_count": 1, "retry_after": "2026-...Z"}
curl -s http://localhost:8000/sessions/$SID/ready
# => {"ready_chunks": [1,2,3], "blocked_chunks": [],
#     "cooling_chunks": [{"chunk_index": 0, "failure_count": 1, "retry_after": "..."}]}

# 4. 批量接口在无可领取项时，next_expiry = min(最早租约过期, 最早退避结束)
curl -s -X POST http://localhost:8000/sessions/$SID/leases/batch \
  -H 'Content-Type: application/json' -d '{"count": 8}'
# => {"granted": [...], "next_expiry": "2026-...Z", ...}

# 5. retry_after 过后重新领取并完成：退避状态清除，failure_count 保留
curl -s -X POST http://localhost:8000/sessions/$SID/chunks/0/complete \
  -H 'Content-Type: application/json' \
  -d "{\"lease_token\": \"<新租约令牌>\", \"chunk_digest\": \"<分片0的SHA256>\"}"
# 状态查询中：status=completed, failure_count=1, retry_after=null
```

## 完成纠错回退（curl）

已完成分片发现内容错误时，凭**完成时保留的租约令牌与摘要**调用 reopen，
将目标分片连同依赖图中的全部传递下游原子重置为 `pending` 后重新上传：

```bash
# 1. 创建带依赖的会话：2 依赖 0、1；3 依赖 2
SID=$(curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' \
  -d '{"total_chunks": 4, "file_digest": "<文件SHA256>",
       "dependencies": {"2": [0, 1], "3": [2]}}' \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["id"])')

# 2. 依次完成 0、1、2（略：领取租约 -> 上报完成），此时发现分片 1 的内容有误

# 3. 凭分片 1 完成时保留的 lease_token 与摘要发起回退
curl -s -X POST http://localhost:8000/sessions/$SID/chunks/1/reopen \
  -H 'Content-Type: application/json' \
  -d '{"lease_token": "<分片1完成时的租约令牌>", "expected_digest": "<分片1完成时上报的摘要>"}'
# => {
#      "session_id": "...", "chunk_index": 1, "revision": 1,
#      "affected_chunks": [
#        {"chunk_index": 1, "level": 0},
#        {"chunk_index": 2, "level": 1},
#        {"chunk_index": 3, "level": 2}
#      ]
#    }
# 目标 1 与全部传递下游（2、3）在同一事务内被重置为 pending：
# 摘要、完成时间、有效租约、退避截止时间全部清除，failure_count 保留；
# 分片 0 不受影响。会话已提交、分片未完成或凭据不匹配时返回 409 且不做任何修改。

# 4. 状态查询中的 revision 已递增（每次成功回退 +1），可据此判断发生过纠错
curl -s http://localhost:8000/sessions/$SID
# => {"revision": 1, "completed_chunks": 1, "missing_chunks": [1, 2, 3], ...}

# 5. 按依赖顺序重新领取、重新完成受影响分片后即可正常提交
curl -s -X POST http://localhost:8000/sessions/$SID/finalize
```

## 进度分支（fork，curl）

从一个进行中的会话**分叉**出新会话：已完成且依赖闭包完整的分片直接复用为
`completed`（只复制摘要与完成时间），其余分片回到 `pending` 重新上传；
源会话与新会话互不影响。新会话可用于「换个客户端/换条链路续传」或
「保留当前进度的同时重试剩余分片」。

```bash
# 1. 源会话：4 个分片，依赖图 2 -> [0, 1]、3 -> [2]；先完成 0、1、2
SID=...   # 同前，POST /sessions 创建并逐分片领取/完成

# 2. 基于源会话当前修订号（GET /sessions/$SID 的 revision 字段）创建分支，
#    复用已完成的 0、1、2（集合包含全部传递前置：2 的前置 0、1 也在列表中）
curl -s -X POST http://localhost:8000/sessions/$SID/fork \
  -H 'Content-Type: application/json' \
  -d '{"expected_revision": 0, "request_id": "resume-2026-09-19-001",
       "reuse_chunks": [0, 1, 2]}'
# => 201 {
#      "id": "<新会话id>", "source_session_id": "<源会话id>",
#      "request_id": "resume-2026-09-19-001",
#      "total_chunks": 4, "status": "active", "revision": 0,
#      "reused_chunks": [0, 1, 2], "pending_chunks": [3],
#      "file_digest": "...", "digest_algorithm": "sha256",
#      "max_active_leases": null, "base_retry_seconds": 5,
#      "max_retry_seconds": 300, "idempotent_replay": false,
#      "created_at": "2026-..."
#    }
# 新会话中 0、1、2 为 completed（摘要/完成时间已复制），3 为 pending；
# 租约、退避冷却、failure_count 均不复制，3 可立即领取后完成并提交。

# 3. 前置闭包不完整：只复用 2 而不带 0、1 -> 409 且不落库
curl -s -X POST http://localhost:8000/sessions/$SID/fork \
  -H 'Content-Type: application/json' \
  -d '{"expected_revision": 0, "request_id": "bad-closure",
       "reuse_chunks": [2]}'
# => 409 {"detail": "reuse_chunks must include all transitive prerequisites",
#         "missing_prerequisites": [0, 1]}
# 复用了未完成（或越界）分片：
# => 409 {"detail": "reuse_chunks must reference completed chunks only",
#         "not_completed_chunks": [3]}
# 修订号不匹配（源会话期间发生过 reopen）：
# => 409 {"detail": "revision mismatch",
#         "expected_revision": 0, "current_revision": 1}

# 4. 网络重试安全：同一 request_id + 相同参数重放，返回首次创建的会话
#    （200，idempotent_replay=true；reuse_chunks 顺序不同或含重复项视为同参数）
curl -s -X POST http://localhost:8000/sessions/$SID/fork \
  -H 'Content-Type: application/json' \
  -d '{"expected_revision": 0, "request_id": "resume-2026-09-19-001",
       "reuse_chunks": [2, 1, 0]}'
# => 200 {"id": "<与首次相同的新会话id>", "idempotent_replay": true, ...}
# 相同 request_id 但参数变化 -> 409
#    {"detail": "request_id already used with different parameters"}

# 5. 状态查询暴露来源会话（原生会话为 null）
curl -s http://localhost:8000/sessions/<新会话id>
# => {"source_session_id": "<源会话id>", "revision": 0, ...}
```

## 交付凭证（delivery ticket，curl）

会话提交后，可向**已提交会话**签发一次性的清单领取凭证。凭证明文**只返回一次**，
数据库内仅保存其 SHA-256、所属会话、到期时间与状态，因此适合把凭证交给只应读取
一次交付清单的一方（如下游交付系统）；配合 `request_id` 可安全重试。

```bash
# 前置：会话已 finalize（全部分片完成并通过可选的 Merkle 校验）
SID=...   # 同前，POST /sessions 创建、逐分片领取/完成后 POST /sessions/$SID/finalize

# 1. 签发凭证：有效期 600 秒；ticket_token 仅此一次返回，请立即保存
curl -s -X POST http://localhost:8000/sessions/$SID/delivery-tickets \
  -H 'Content-Type: application/json' -d '{"expires_in_seconds": 600}'
# => 201 {
#      "ticket_token": "Rk9...（随机令牌，只展示这一次）",
#      "session_id": "...", "status": "active",
#      "expires_at": "2026-...Z", "created_at": "2026-...Z"
#    }
TK='<上一步返回的 ticket_token>'

# 2. 兑换：携带调用方生成的幂等键 request_id，返回按分片序号升序的交付清单
curl -s -X POST http://localhost:8000/delivery-tickets/redeem \
  -H 'Content-Type: application/json' \
  -d "{\"token\": \"$TK\", \"request_id\": \"delivery-2026-09-19-001\"}"
# => 200 {
#      "session_id": "...", "status": "finalized",
#      "file_digest": "...", "digest_algorithm": "sha256",
#      "total_chunks": 4,
#      "manifest": [
#        {"chunk_index": 0, "chunk_digest": "...", "completed_at": "2026-..."},
#        {"chunk_index": 1, "chunk_digest": "...", "completed_at": "2026-..."},
#        ...
#      ],
#      "finalized_at": "2026-...Z",
#      "redeemed_at": "2026-...Z",
#      "request_id": "delivery-2026-09-19-001"
#    }

# 3. 网络重试安全：同一 request_id 重放返回与首次完全相同的结果
#    （redeemed_at / manifest 均为首次值；即使凭证之后过期或被撤销也一样）
curl -s -X POST http://localhost:8000/delivery-tickets/redeem \
  -H 'Content-Type: application/json' \
  -d "{\"token\": \"$TK\", \"request_id\": \"delivery-2026-09-19-001\"}"
# => 200（与首次逐字段一致）

# 4. 凭证一次性：不同 request_id 的兑换（含并发请求）只有一个成功
#    => 409 {"detail": "delivery ticket already consumed"}
#    过期 => 409 {"detail": "delivery ticket expired"}
#    撤销 => 409 {"detail": "delivery ticket revoked"}
#    未知令牌 => 404 {"detail": "delivery ticket not found"}

# 5. 撤销尚未消费的凭证（已消费凭证返回 409；重复撤销幂等）
curl -s -X POST http://localhost:8000/delivery-tickets/revoke \
  -H 'Content-Type: application/json' -d "{\"token\": \"$TK2\"}"
# => 200 {"session_id": "...", "status": "revoked",
#         "revoked_at": "2026-...Z", "already_revoked": false}
```

同一会话的多张凭证相互独立：撤销一张不影响其它凭证，各自只能成功兑换一次。
注意签发与撤销都需要凭证明文；库内无法从 SHA-256 反推出令牌，因此明文丢失后
只能重新签发（必要时等旧凭证自然过期）。

## 已提交清单差异（manifest diff，curl）

`POST /manifests/diff` 比较两个**已提交（finalized）**会话按分片序号排列的
`chunk_digest` 序列，回答「两份交付清单之间差了哪些分片」。服务默认监听
`http://localhost:8000`（端口映射见「快速启动」，以下示例均用该地址）。

请求体：

```json
{"base_session_id": "<基准会话id>", "target_session_id": "<目标会话id>"}
```

规则要点：

- **最小编辑脚本**：仅使用插入/删除两种操作，服务端保证**插入次数 + 删除次数之和
  最小**（= m + n − 2×LCS）；`edit_distance` 即该最小值，`deletions` / `insertions`
  为分项计数。
- **多解时固定优先删除再插入**：同一替换位置既有删除又有插入时，`delete` 区段固定
  排在 `insert` 区段之前，输出完全确定。
- **区段合并**：连续同类操作合并为一个区段，携带 base/target 两侧的**半开区间**
  `[base_start, base_end)` / `[target_start, target_end)` 与对应摘要
  （`base_digests` / `target_digests`；insert 区段无 base 摘要、delete 区段无
  target 摘要）。单处分片插入只产生一个 insert 区段，其后的相同分片仍归入 equal
  区段，不会全部误报。
- **错误码**：任一会话不存在 `404`；任一会话未 finalized `409`；任一清单超过
  **2000** 项 `422`；两份清单完全一致时返回覆盖整个清单的**单个 equal 区段**
  （`identical: true`、`edit_distance: 0`）。

```bash
# 前置：两个会话均已 finalize。设 base 清单为 [A,B,C]，target 清单为 [A,X,B,C]
# （在分片 1 处插入 X），摘要均为 64 位十六进制
BASE=...; TARGET=...

curl -s -X POST http://localhost:8000/manifests/diff \
  -H 'Content-Type: application/json' \
  -d "{\"base_session_id\": \"$BASE\", \"target_session_id\": \"$TARGET\"}"
# => 200 {
#      "base_session_id": "...", "target_session_id": "...",
#      "base_total": 3, "target_total": 4,
#      "identical": false, "edit_distance": 1,
#      "deletions": 0, "insertions": 1,
#      "segments": [
#        {"op": "equal",  "base_start": 0, "base_end": 1,
#         "target_start": 0, "target_end": 1,
#         "base_digests": ["A"], "target_digests": ["A"]},
#        {"op": "insert", "base_start": 1, "base_end": 1,
#         "target_start": 1, "target_end": 2,
#         "base_digests": [], "target_digests": ["X"]},
#        {"op": "equal",  "base_start": 1, "base_end": 3,
#         "target_start": 2, "target_end": 4,
#         "base_digests": ["B", "C"], "target_digests": ["B", "C"]}
#      ]
#    }
# 单处插入只产生一个 insert 区段，其后的 B、C 仍在 equal 区段中

# 替换场景（base [A] -> target [B]）：多解时固定优先删除再插入
# => "segments": [
#      {"op": "delete", "base_start": 0, "base_end": 1,
#       "target_start": 0, "target_end": 0, "base_digests": ["A"], "target_digests": []},
#      {"op": "insert", "base_start": 1, "base_end": 1,
#       "target_start": 0, "target_end": 1, "base_digests": [], "target_digests": ["B"]}
#    ]
```

## 本地开发（不用 Docker）

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

运行测试（需额外安装 `pytest`、`httpx`）：

```bash
python3 -m pytest tests/ -q
```

## 项目结构

```
app/
  main.py      # FastAPI 应用与全部端点
  db.py        # SQLite 连接与版本化迁移器（启动时自动迁移；含失败拒绝启动逻辑）
  schemas.py   # Pydantic 请求/响应模型
tests/
  test_api.py          # 租约独占/过期、幂等、提交、交付凭证、分片验收队列、清单差异、
                       # 租户容量预留/配额/TTL 到期释放/取消幂等等端到端行为测试
  test_transactions.py # 统一事务边界：4xx/异常回滚、写入中途故障注入（连接可继续使用、
                       # 账务/分片/游标无半成品）、预留到期惰性取消独立持久化测试
  test_migrations.py   # 版本化迁移器：空库/旧库收敛同构、数据/索引/外键保留、
                       # 校验和变化/版本缺口/未来版本拒绝启动、失败整体回滚可重试、
                       # 并发（多线程/多进程等待复核）启动不重复 ALTER
Dockerfile
docker-compose.yml
requirements.txt
```
