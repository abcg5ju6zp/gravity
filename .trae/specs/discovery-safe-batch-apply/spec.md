# 发现结果安全批量应用（DHCP/DNS）- 产品需求文档

## Overview

- **Summary**: 为 discovery（子网发现）角色引入「不可变扫描结果 + 带版本固定的差异预览 + 可恢复的批量应用作业」机制：每轮扫描产生带稳定身份（scanID）与来源修订（etcd sourceRevision）的不可变快照；应用前计算仍可执行的差异并固定名称、地址、子网与目标记录版本；应用以 etcd 比较事务（CAS）领取，设备配置、DHCP 保留、正反向 DNS 记录共享同一应用身份并逐项确认；部分失败保留进度，重试只补齐未确认步骤；取消、重启、重复提交、新扫描与多节点并发均不得用旧状态覆盖新状态或删除后来创建的记录，冲突设备独立上报且不阻断其他设备。
- **Purpose**: 消除现有流程中「旧扫描覆盖扫描期间的人工编辑/新一轮扫描结果」「fire-and-forget 事件在 DHCP 与 DNS 之间留下半套配置」「失败重试重写已确认对象或波及无关设备」「并发/重启下状态互相覆盖」等数据安全问题。
- **Target Users**: 使用 Gravity 进行子网发现并将设备批量落为 DHCP 保留与 DNS 记录的网络管理员；以及在多节点集群中运行 discovery/dhcp/dns 角色的运维者。

## Goals

- 每轮扫描结果不可变、可追溯（稳定身份 + 来源修订 + 内容哈希），扫描与扫描之间、扫描与人工维护之间互不覆盖。
- 应用前提供仍可执行的差异预览；申请载荷固定设备快照与目标记录版本，服务端按固定版本领取。
- 一次批量应用在设备、DHCP 保留、正向 DNS、反向 DNS 四类对象上记录同一应用身份与逐项结果。
- 部分失败可恢复：逐项/逐步骤进度持久化；重试幂等补齐；冲突隔离不拖累独立设备。
- 对取消、重复提交、服务重启、新一轮扫描、多节点并发提供 fencing 与 CAS 保证；新流程不删除任何非本作业创建的记录。
- 现有行为不回归：只查看扫描结果、手工维护设备（删除）、未启用 DHCP 或 DNS 的安装、无冲突的单设备旧接口应用，全部保持现状。

## Non-Goals

- 不实现/修改 Web 前端（前端为独立仓库，不在本次工作区）；仅交付后端 REST API、etcd 数据模型、测试与文档。
- 不改变 DHCP 协议处理、IPAM 分配算法、DNS 解析与 zone 层级匹配逻辑。
- 不做扫描结果与应用作业的 UI 工作流编排；不引入新的调度系统（扫描触发方式不变）。
- 不做跨实例的事件总线：沿用进程内同步事件 + etcd 状态复制的现有架构。
- 不提供自动回滚（saga 补偿删除）：取消/失败后已确认步骤保留，不做级联删除。
- 不重构 OpenAPI Go 客户端生成流水线（`api/` 目录由 Docker 中的 openapi-generator 生成）；如环境允许重新生成 `schema.yml`，否则手工保持等价更新。

## Background & Context

现有实现（`pkg/roles/discovery`、`pkg/roles/dhcp`、`pkg/roles/dns`）：

- 扫描：子网 watcher 在 `/discovery/subnets/{name}` 发生 PUT 或初次加载时触发 `Subnet.RunDiscovery`（[subnet.go](file:///Users/ding/Documents/swe/0910125/project-03/pkg/roles/discovery/subnet.go)），已有基于 etcd session 的**每子网互斥锁**；nmap 扫描后对每个主机**盲写** `/discovery/devices/{ip}`（TTL=DiscoveryTTL），无扫描身份、无来源修订、无版本检查——晚结束的旧扫描会覆盖期间的人工删除/编辑与新一轮结果。
- 应用：`POST /api/v1/discovery/devices/apply`（[api_devices.go](file:///Users/ding/Documents/swe/0910125/project-03/pkg/roles/discovery/api_devices.go)）同步派发 `roles.dhcp.lease.create` 或 `roles.dns.record.create_forward/create_reverse` 事件（[device.go](file:///Users/ding/Documents/swe/0910125/project-03/pkg/roles/discovery/device.go)）；处理器只记录日志、不回传结果（[dhcp/event_handler.go](file:///Users/ding/Documents/swe/0910125/project-03/pkg/roles/dhcp/event_handler.go)、[dns/dhcp_event_handler.go](file:///Users/ding/Documents/swe/0910125/project-03/pkg/roles/dns/dhcp_event_handler.go)）；DHCP 链路 `Lease.afterPut` 再触发 DNS 正反向记录，zone 缺失时静默跳过；随后设备被删除。失败不可见、重试重复写、半套配置无进度。
- 存储：`storage.Client` 内嵌 etcd clientv3，代码库已有 Txn+Compare 先例 `CreateLeaseIfAbsent`（[leases.go](file:///Users/ding/Documents/swe/0910125/project-03/pkg/roles/dhcp/leases.go#L195-L249)，按 CreateRevision 判存）。
- 对象键：DHCP 保留 `/dhcp/leases/{mac}`；DNS 记录 `/dns/zones/{zone}/{name}/{TYPE}/{uid}`（uid 是多值记录区分键）；事件 `DispatchEvent` 为进程内同步调用（[events.go](file:///Users/ding/Documents/swe/0910125/project-03/pkg/instance/events.go)）。
- 测试基建：`pkg/tests/utils.go` 提供 etcd 重置、JSON 断言（按 JSON 相等比较，字段顺序无关）；现有 discovery 测试直接构造 dhcp/dns role 注册事件处理器。
- 已确认的决策：① 仅交付后端 API + 测试 + 文档；② 保留旧单设备应用接口，新增批量作业接口；③ 不可变扫描结果沿用子网 DiscoveryTTL，终态作业默认保留 30 天（可配置），进行中作业不过期。

## Functional Requirements

### 扫描：不可变结果与稳定身份

- **FR-1**：每轮 `RunDiscovery` 生成全局唯一 `scanID`，并在扫描开始前捕获 etcd 当前修订（sourceRevision，来自 etcd 响应头 `Header.Revision`）。
- **FR-2**：扫描完成后写入不可变结果：扫描元数据 `/discovery/scans/{subnet}/{scanID}` 与逐设备快照 `/discovery/scans/{subnet}/{scanID}/devices/{identifier}`；快照含 scanID、subnet、hostname、ip、mac、sourceRevision、observedAt 与内容哈希；快照键仅允许创建（CreateRevision=0 的 CAS），任何重写被拒绝。
- **FR-3**：现有 `/discovery/devices/{ip}`「最新视图」保留：扫描投影写入必须是条件写——仅当键不存在，或现存记录仍来自同轮/更旧扫描且未被应用领取时才更新；人工删除（键已不存在）不被复活*、人工/原始写入与已领取（ApplyID 非空）的设备不被覆盖；设备 TTL 租约在更新时保留。扫描快照仍如实记录 nmap 观测结果，与投影是否更新无关。
  - *注：扫描投影对「不存在即创建」维持现有行为；「不复活」仅适用于本轮扫描开始前捕获到、扫描期间被删除且被标记为人工来源的设备（见 FR-4）。
- **FR-4**：设备新增可追溯字段（均为 JSON 可选附加字段，旧数据反序列化零值兼容）：`source`（`scan`/`manual`，空值等同 `scan`）、`scanId`、`sourceRevision`、`subnet`、`contentHash`、`applyId`、`applyStatus`、`appliedAt`。当前唯一受支持的人工维护接口 `DELETE /discovery/devices` 语义不变（只删设备键，不级联）。
- **FR-5**：扫描元数据生命周期：`running`（开始时一次性创建）→ `finished`/`failed`（仅允许一次 CAS 翻转）；finished 后不可变；快照与 finished 元数据附加 DiscoveryTTL 租约；角色（重）启动时将超过阈值仍为 running 的陈旧扫描标记为 failed。

### 查询：扫描结果

- **FR-6**：新增 `GET /api/v1/discovery/scans`（可按 subnet 过滤）与 `GET /api/v1/discovery/scans/{scanId}/devices`，返回不可变元数据与快照。
- **FR-7**：现有 `GET /api/v1/discovery/devices` 响应字段保持不变，新增字段（scanId、source、sourceRevision、applyId、applyStatus）以可选/空值省略方式附加；不新增必填参数。

### 差异预览（dry-run，无写入）

- **FR-8**：新增 `POST /api/v1/discovery/applies/preview`：入参为 scanId、目标集合（`dhcp`、`dns` 任意组合）、dhcpScope、dnsZone、设备 identifier 列表（空=全部）。对每台设备返回：
  - 固定的设备快照（hostname/ip/mac/subnet、scanId、sourceRevision、内容哈希）与 live 设备键当前修订；
  - 每个目标步骤（dhcp 保留、dns 正向、dns 反向）的目标键、当前 create/mod 修订、动作（`create`/`keep`/`conflict`/`skip`）、不可执行原因码；
  - 设备级 `executable` 标志与原因（如：快照缺失/过期、live 设备已被人工改动、存在更新扫描、设备已被其他作业领取、目标记录版本不符、scope/zone 缺失、角色未启用、DHCP 目标缺少 MAC）。
- **FR-9**：预览通过新增的「ensure 事件」dry-run 模式委派 dhcp/dns 角色解析 scope/zone 与构造目标键（键布局与 JSON 结构的知识留在 dhcp/dns 包内）；对应角色未启用（无处理器）时步骤动作为 `skip`（role_unavailable），预览不报错。

### 应用作业：创建、固定与领取

- **FR-10**：新增 `POST /api/v1/discovery/applies`：客户端回传预览给出的固定项（设备快照哈希、live 设备修订、各目标键 expectedCreateRevision/expectedModRevision、目标集合、scope、zone），可带 `idempotencyKey`。服务端：校验 scan 存在且 finished、快照哈希一致；幂等键经独立索引 `/discovery/applies-idem/{key}`（仅创建 CAS）去重，重复提交返回同一作业；创建 `/discovery/applies/{applyID}` 并异步启动执行器，立即返回作业状态（200/202）。
- **FR-11**：作业持久化结构含：applyID、idempotencyKey、subnet、scanId、sourceRevision、targets、dhcpScope、dnsZone、status、fencingGeneration、nodeID、时间戳、逐项 items；每个 item 含设备固定快照、固定修订、结果状态与冲突原因；每个 step 含名称（device_mark/dhcp_reservation/dns_forward/dns_reverse）、目标键、固定版本、状态（pending/confirmed/skipped/conflict/failed/cancelled）、确认时间与细节。
- **FR-12**：领取与写入规则（每个步骤一个 etcd Txn）：
  1. 作业键 ModRevision 必须等于执行器持有的 fencing 版本（取消/重试/接管会推进版本，旧执行器事务失败即停）；
  2. device_mark：live 设备键 ModRevision 等于固定值且 scanId 一致——否则 device_changed_manual / device_changed_scan / device_claimed 冲突；写入仅附加 applyId/applyStatus/appliedAt，保留原 TTL 租约；
  3. dhcp/dns：目标键固定为「不存在」（CreateRevision=0）时必须仍不存在方可创建；固定为「存在」时 ModRevision 必须未变且内容与期望一致方可 keep；现存对象若带本作业 applyId 且内容一致，协调为 confirmed（幂等）；任何其他不一致 → target_changed/target_owned_other 冲突，**不写、不删**；
  4. 步骤对象写入后，执行器再以作业键 CAS 持久化进度；两步之间崩溃由重试对账（见 FR-14）。
- **FR-13**：同一 applyID 与 scanId（来源 provenance）写入四类对象：live 设备、DHCP 保留（Lease JSON 新增可选 provenance 字段）、正向记录、反向记录（Record JSON 新增可选 provenance 字段，位于值内；uid 键不变）。带 provenance 的保留创建时**不触发**现有 DHCP→DNS 自动链式派发（由作业显式拥有 DNS 步骤），但保留 IPAM 用量统计等其余 afterPut 行为；无 provenance 的旧链路行为不变。
- **FR-14**：部分失败与重试：单步失败只将该 step 置 failed/conflict 并继续后续步骤与其他独立设备；作业终态为 completed（全部 confirmed/skipped）、partial（存在 applied 且存在 conflict/failed/cancelled）、failed（仅基础设施性失败且无任何成功项）或 cancelled。`POST /applies/{id}/retry` 推进 fencingGeneration，仅对未 confirmed 步骤重新校验并执行；已 confirmed 步骤与未选中/无关设备绝不重写；目标已被他人改变则升级为冲突。

### 取消、重启、并发

- **FR-15**：`POST /api/v1/discovery/applies/{id}/cancel` 以 CAS 将 pending/running 置为 cancelling；执行器在每个 item/step 边界观测到后停止，已确认步骤保留、未执行步骤置 cancelled，最终态 cancelled；取消不删除任何已创建对象。
- **FR-16**：discovery 角色 Start 时恢复非终态作业：基于 etcd session 互斥锁 `/discovery/applies/locks/{applyID}` 接管（死亡节点 session 过期自动释放），并有定时兜底巡检；接管后按 FR-14 对账继续。
- **FR-17**：多节点/重复并发：同一作业同一时刻仅一个执行器持锁；不同作业对同一设备的竞争由目标键 CAS 裁决——先到者创建，后到者该设备冲突并继续其余设备；任何路径不得删除非本作业 provenance 的对象。
- **FR-18**：作业执行期间出现更新一轮扫描时，旧作业在 device_mark 与快照有效性校验处得到冲突（不覆盖新投影），其未触达的目标键保持 untouched；新扫描不回写、不删除任何作业 provenance 对象。

### 兼容性与部署

- **FR-19**：旧接口 `POST /api/v1/discovery/devices/apply`（to=dhcp|dns、事件派发后删除设备）与现有 `GET/DELETE devices`、subnets 全部接口路径与语义保持不变；现有测试不修改即通过。
- **FR-20**：未启用/未运行 DHCP 或 DNS 角色的安装：预览对应步骤 skip(role_unavailable)，作业执行时跳过对应步骤（skipped），不报错、不影响其余步骤；只装 DNS 或只装 DHCP 均可工作。
- **FR-21**：保留策略可配置（RoleConfig 新增 `applyRetentionSeconds`，默认 2592000=30 天）；终态作业与其幂等索引在终态时附加保留租约；非终态作业不过期；扫描结果沿用子网 DiscoveryTTL。
- **FR-22**：事件机制以**向后兼容的附加方式**扩展（事件结果回收器挂在 Event 上，`DispatchEvent` 签名不变；旧处理器零改动）。
- **FR-23**：更新运行时 OpenAPI 产物（`schema.yml`；环境允许时重新生成，`docs/static/schema.yaml` 同步）与 `docs/content/docs/discovery/_index.md`（扫描结果、预览、作业、冲突、保留说明）。

## Non-Functional Requirements

- **NFR-1（正确性）**：所有领取与状态推进必须基于 etcd 比较事务，禁止「先读后写」无防护写；并发正确性有可重复运行的自动化测试证据。
- **NFR-2（可恢复）**：任意步骤边界杀死进程后，作业可由本节点或其他节点接管，最终状态一次且仅一次生效（幂等对账），不产生重复对象。
- **NFR-3（向后兼容）**：etcd 中既有 lease/record/device JSON 无新字段可正常反序列化；旧 API 契约（路径、方法、必填参数、旧字段）不变；新增字段全部 omitempty/可选。
- **NFR-4（隔离性）**：单设备/单步骤冲突不影响同作业其他独立设备；批量 N 台设备的复杂度为每设备常数次 etcd 事务，无全表锁。
- **NFR-5（可观测）**：作业与步骤状态、冲突原因码可通过 API 读取；关键冲突/跳过路径有结构化日志（zap，沿用现有风格）。
- **NFR-6（可测试）**：核心持久化与状态机不依赖 nmap（扫描结果落盘逻辑可脱离 nmap 单测）；沿用 `pkg/tests` 基建，`go test ./pkg/roles/...` 通过。

## Constraints

- **Technical**: Go + etcd clientv3（Txn/Compare/concurrency session/mutex 现成可用）；swaggest/usecase 注册 REST；事件为进程内同步；DNS 记录 uid 是键的一部分；Lease/Record 新字段必须 `omitempty` 以保证现有 JSON 断言与旧数据兼容。
- **Business**: 不破坏既有 API 行为；不引入前端交付物；默认保留策略 30 天、扫描沿用 DiscoveryTTL。
- **Dependencies**: 测试与 schema 重新生成依赖本机/容器化 etcd（现有测试同一依赖）；nmap 仅端到端扫描需要，核心逻辑单测不依赖。

## Assumptions

- 「人工维护设备」指受支持 API：`DELETE /discovery/devices`，以及通过 dhcp/dns 各自 API 手工维护保留与记录；直接改 etcd 裸数据不在兼容承诺内。
- 同一进程内启用的角色即事件处理器可用（与现有单设备应用相同前提）；集群中状态经 etcd 复制，作业锁负责跨节点互斥。
- DNS 同一 name/type 允许多值（uid 区分）：固定版本只约束本作业 uid 对应的记录键；他人其他 uid 的并存记录不视为冲突，但预览需列出以提示管理员。
- 应用目标不做「覆盖式更新」：对外来（无本作业 provenance）对象只允许 keep，不允许改写；需要覆盖时由管理员先手工处理，接口随后报告可执行。
- 作业为子网内批量；跨子网一次性提交不在本期范围（可多次调用）。

## Acceptance Criteria

### AC-1：扫描产生稳定身份与来源修订
- **Type**: `rule`
- **Given**: 某子网已配置并触发一轮扫描
- **When**: 扫描结果落盘
- **Then**: 生成非空 scanID；扫描元数据记录扫描开始前捕获的 etcd sourceRevision、subnet、CIDR、running→finished 状态与时间戳
- **Pass Condition**: 单测中调用扫描结果持久化逻辑后，`/discovery/scans/{subnet}/{scanID}` 存在且字段含 scanID 与递增的 sourceRevision；状态成功翻转为 finished
- **Evidence**: 新增 Go 测试输出 + etcd 键断言（`tests.AssertEtcd`）

### AC-2：逐设备快照不可变
- **Type**: `rule`
- **Given**: 一轮已完成扫描的设备快照键
- **When**: 以相同 scanID+identifier 再次写入（模拟重复落盘/旧扫描回放）
- **Then**: 第二次写入因 CreateRevision≠0 被 CAS 拒绝，既有快照值不变
- **Pass Condition**: 测试断言重复写入返回冲突错误且原值字节不变
- **Evidence**: Go 测试

### AC-3：最新设备视图不被旧扫描覆盖
- **Type**: `rule`
- **Given**: live 设备已存在且（a）被人工标记/删除后重建标记，或（b）带更新 scanId，或（c）已被作业领取（applyId 非空）
- **When**: 一轮更旧/并发扫描将结果投影到 `/discovery/devices/{ip}`
- **Then**: 该设备键不被覆盖（值与修订不变）；同一扫描的不可变快照仍包含 nmap 观测结果
- **Pass Condition**: 三种场景各有测试断言设备键 ModRevision 不变，而 `/discovery/scans/.../devices/{ip}` 快照正常存在
- **Evidence**: Go 测试

### AC-4：设备 TTL 与人工删除行为保持
- **Type**: `rule`
- **Given**: 由扫描创建、带 DiscoveryTTL 租约的设备
- **When**: device_mark 或同轮扫描投影更新该键
- **Then**: 更新附加 etcd 租约选项保持原租约（键仍按 TTL 过期）；`DELETE /api/v1/discovery/devices` 只删除设备键，不触碰任何 DHCP/DNS 对象
- **Pass Condition**: 测试断言更新后 KeyValue.Lease 非零；删除设备后 lease/record 键仍然存在
- **Evidence**: Go 测试

### AC-5：扫描结果查询 API
- **Type**: `rule`
- **Given**: 至少两轮 finished 扫描
- **When**: 调用 `GET /scans`（按子网过滤）与 `GET /scans/{id}/devices`
- **Then**: 返回不可变元数据列表与对应快照；`GET /discovery/devices` 旧响应形状不变（新字段缺失时省略）
- **Pass Condition**: interactor 单测断言输出字段与可选字段省略行为
- **Evidence**: Go 测试

### AC-6：差异预览固定版本且无写入
- **Type**: `rule`
- **Given**: 一轮 finished 扫描、配置好的 dhcp scope 与 fwd/rev zone（以及另一个 zone 缺失/设备缺 MAC 的反例）
- **When**: 调用 `POST /applies/preview`
- **Then**: 每设备返回快照哈希、live 设备修订，每个步骤返回目标键与当前 create/mod 修订、动作（create/keep/conflict/skip）与原因码；调用前后 etcd 除纯读外无变化；不可执行设备 `executable=false` 且原因准确（zone_missing/scope_missing/device_changed_scan/role_unavailable/mac_required 等）
- **Pass Condition**: 测试断言预览包含可用于提交的全部固定字段，且 etcd 修订总数在预览前后不变
- **Evidence**: Go 测试

### AC-7：作业创建、快照校验与幂等去重
- **Type**: `rule`
- **Given**: 预览输出与（可选）idempotencyKey
- **When**: 以匹配的固定项提交作业；再以相同 idempotencyKey 重复提交；以及篡加快照哈希/过期 scanId 提交
- **Then**: 首次返回 applyID 与 pending/running 状态；重复返回**同一** applyID 且不产生第二个作业；篡改/过期请求被 4xx 拒绝且无写入
- **Pass Condition**: 三种提交各有测试断言（作业键数量、applyID 相等、拒绝状态码）
- **Evidence**: Go 测试

### AC-8：按固定版本领取，目标变更即冲突
- **Type**: `rule`
- **Given**: 预览固定目标键不存在（createRev=0）/存在（modRev=M）
- **When**: 领取前目标被人工创建/修改，或 live 设备被更新扫描/人工改动；随后执行作业
- **Then**: 对应 step 置 conflict（target_changed / device_changed_manual / device_changed_scan），目标键不被写或删，作业继续处理其他设备；无变更的设备正常 confirmed
- **Pass Condition**: 测试断言冲突项状态/原因、目标键保持人工值、其余设备对象被创建
- **Evidence**: Go 测试

### AC-9：四类对象共享应用身份
- **Type**: `rule`
- **Given**: 目标含 dhcp+dns 的作业成功执行
- **When**: 检查 live 设备、`/dhcp/leases/{mac}`、正向 A/AAAA、反向 PTR
- **Then**: 四类对象的 provenance 均含同一 applyId 与 scanId；作业 item 的四个步骤为 confirmed 且记录目标键与确认时间；正反向记录 uid 与预览固定键一致
- **Pass Condition**: 测试逐键断言 provenance.applyId 相等；作业 JSON 步骤状态齐全
- **Evidence**: Go 测试

### AC-10：半套配置可恢复，重试只补未确认步骤
- **Type**: `rule`
- **Given**: 反向 zone 缺失（反向步骤 skip/conflict）、正向与 DHCP 已 confirmed 的 partial 作业；修复 zone 后调用 retry
- **When**: 重试执行
- **Then**: 仅未 confirmed 步骤被执行；设备/租约/正向记录的写入修订不发生第二次变化（值与 ModRevision 不变）；反向记录创建后作业 completed；再次重试为纯对账、零写入
- **Pass Condition**: 重试前后记录已确认对象的 ModRevision；断言仅缺失对象被新增
- **Evidence**: Go 测试

### AC-11：取消不删除、不产生半成品新写入
- **Type**: `rule`
- **Given**: 多设备作业运行中
- **When**: 在步骤间隙调用 cancel
- **Then**: 作业终态 cancelled；已 confirmed 步骤对象全部保留；未执行步骤为 cancelled；不存在「写了对象但 provenance 缺失」的新对象；无任何删除操作
- **Pass Condition**: 测试断言终态、已建对象仍带 provenance 存在、未建键不存在
- **Evidence**: Go 测试

### AC-12：服务重启后接管与对账
- **Type**: `rule`
- **Given**: 作业在若干步骤 confirmed、若干 pending 时执行节点「宕机」（session 失效/角色重启）
- **When**: discovery 角色重新 Start（本节点或第二节点）
- **Then**: 恢复逻辑经 etcd 锁接管作业，对已带本作业 provenance 的对象对账为 confirmed（不重写），补齐剩余步骤直到终态；无重复对象
- **Pass Condition**: 测试以两个 discovery Role 实例共享同一 etcd 模拟接管，断言对象唯一且作业终态正确
- **Evidence**: Go 测试

### AC-13：多节点/多作业并发安全
- **Type**: `rule`
- **Given**: 两个执行器并发运行同一作业；以及两个不同作业包含同一设备
- **When**: 并发执行完成
- **Then**: 同作业仅一个持锁执行；跨作业竞争由 CAS 裁决：一个作业 applied，另一个该设备 target_conflict 且其他设备继续；全程无覆盖、无删除后来创建的对象
- **Pass Condition**: 并发测试（goroutine + 双 Role 实例）断言最终对象唯一、provenance 归属唯一、冲突项存在
- **Evidence**: Go 测试

### AC-14：执行期间出现更新扫描不被旧作业覆盖
- **Type**: `rule`
- **Given**: 作业基于 scan#1 固定并尚未执行某设备步骤
- **When**: scan#2 完成并更新 live 设备投影后，旧作业继续执行
- **Then**: 该设备 device_mark 冲突（device_changed_scan），scan#2 投影不被回滚；作业其他设备继续
- **Pass Condition**: 测试断言设备最终保持 scan#2 字段，旧作业项为冲突
- **Evidence**: Go 测试

### AC-15：角色缺失安装优雅跳过
- **Type**: `rule`
- **Given**: 仅启用 discovery+dns（无 dhcp 角色）与仅 discovery+dhcp（无 dns 角色）两种安装
- **When**: 预览并执行含两个目标的作业
- **Then**: 缺失角色的步骤为 skipped(role_unavailable)，其余步骤成功；API 不返回错误；作业可 completed
- **Pass Condition**: 两组角色装配的测试断言步骤状态与对象存在性
- **Evidence**: Go 测试

### AC-16：旧接口与旧测试零回归
- **Type**: `rule`
- **Given**: 现有 `TestDeviceApplyDHCP`、`TestDeviceApplyDHCPWithDNS`、`TestDeviceApplyDNSWithReverse`、`TestAPIDevicesDelete`、`TestDiscoveryConvert` 等
- **When**: 在不修改旧测试的情况下运行
- **Then**: 全部通过；旧 apply 仍为「派发旧事件 + 删除设备」、租约经旧链路自动产生正反向记录
- **Pass Condition**: `go test ./pkg/roles/discovery/... ./pkg/roles/dhcp/... ./pkg/roles/dns/...` 通过且旧测试文件未被修改
- **Evidence**: 测试命令输出

### AC-17：保留策略
- **Type**: `rule`
- **Given**: DiscoveryTTL 与默认 30 天作业保留（可通过 RoleConfig 覆盖）
- **When**: 扫描快照落盘、作业进入终态
- **Then**: 扫描元数据/快照 etcd 键带 DiscoveryTTL 租约；终态作业键与幂等索引带保留租约（默认 2592000s，配置值生效）；非终态作业无租约
- **Pass Condition**: 测试断言各键 Lease ID 非零（非终态作业为零），配置覆盖后授权 TTL 等于配置值
- **Evidence**: Go 测试

### AC-18：冲突隔离与可诊断性质量
- **Type**: `rubric`
- **Dimension**: 冲突分类与 API 可诊断性
- **Scale**: 1-5
- **Anchors**: 1 = 冲突只表现为 500/无信息；3 = 有成功/失败二分与单一错误信息；5 = 设备级与步骤级均有稳定原因码枚举（manual/scan/claim/target/scope/zone/role 等）、动作与当前版本，批量中逐设备隔离且文档齐备
- **Pass Threshold**: >= 4
- **Evidence**: API 输出结构 + 文档原因码表 + 测试覆盖每种原因码至少一例

### AC-19：实现与现有架构的一致性
- **Type**: `rubric`
- **Dimension**: 代码质量与架构契合度
- **Scale**: 1-5
- **Anchors**: 1 = 跨包复制键布局/JSON 结构、引入裸读裸写、事件签名破坏兼容；3 = 功能可用但存在跨层知识重复；5 = 键与序列化知识留在 dhcp/dns 包内（ensure 事件 dry-run/ensure 两阶段）、全部领取走 Txn、无 import 环、事件扩展向后兼容、新增字段 omitempty、命名与日志风格统一
- **Pass Threshold**: >= 4
- **Evidence**: 代码走查 + `go vet`/`go build ./...` + 独立审查

## Open Questions

- 无（三项关键决策已由用户确认：后端-only 交付；保留旧接口新增作业接口；扫描沿用 DiscoveryTTL、终态作业保留 30 天可配置）。
