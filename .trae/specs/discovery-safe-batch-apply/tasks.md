# 发现结果安全批量应用（DHCP/DNS）- 实施计划

说明：任务按依赖顺序排列；每个任务交付后须自验全部 TR（rule 需客观证据，rubric 记录评分与理由）。测试沿用 `pkg/tests` 基建（etcd 已在 `tests.Setup` 中重置）；nmap 相关路径通过抽取「扫描结果持久化」纯函数单测，不在单测中运行 nmap。

## Task 1: 共享 provenance 类型与事件结果回收基础设施
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Completion Evidence**:
  - TR-1.1 `rule` PASS：`TestLeaseProvenanceJSONCompat`、`TestRecordProvenanceJSONCompat` 通过——旧 JSON 无 discovery 字段往返保持、新 provenance 正确序列化。
  - TR-1.2 `rule` PASS：`TestProvenanceReservationSkipsDNSChain` 通过（内部测试以真实事件分发接线 dhcp+dns role）——带 provenance 的保留不产生正反向记录，无 provenance 正控仍产生。
  - TR-1.3 `rule` PASS：`go test -race ./pkg/roles/ -run TestEventOutcomes` 通过，16×100 并发上报无丢失/无竞争，Outcomes() 返回隔离副本。
  - TR-1.4 `rubric` 评分 5/5：dhcp/dns 仅依赖 discovery/types 叶子包无环（`go build ./...` 通过）；DispatchEvent 签名未变；新字段全部 omitempty；新增文件 roles/ensure.go、discovery/types/apply.go。
  - 环境注记：dns 的 Blocky/IPForwarder、oui/TestDB 为外网/docker/真实 OUI 库依赖的预存失败（git stash 后裸仓库同样失败），与本改动无关。
- **Description**:
  - 在 `pkg/roles/discovery/types` 新增 `Provenance`（JSON: `applyId`、`scanId`、`source` 固定为 `discovery-apply`）与新键常量 `KeyScans="scans"`、`KeyApplies="applies"`、`KeyAppliesIdem="applies-idem"`、`KeyApplyLocks="apply-locks"` 及扫描/作业状态、步骤名、原因码字符串常量（集中定义，跨包共享）。
  - 扩展 `pkg/roles/role.go` 的事件载荷：新增 `EnsureOutcome`（key、createRevision、modRevision、action、reason、alreadyProvisioned、errorMsg）与并发安全的 `Event.ReportOutcome(...)` / `Event.Outcomes()`；`DispatchEvent` 签名与旧处理器行为不变。
  - `dhcp.Lease` 新增 `Discovery *discoveryTypes.Provenance \`json:"discovery,omitempty"\``；`dns.Record` 在值 JSON 中新增同构可选字段（不进键、不影响 uid）。
  - `Lease.afterPut` 仅当 `Discovery != nil` 时跳过 `EventTopicDHCPLeasePut` 的 DNS 链式派发（保留 calculateUsage 等其余逻辑）；无 provenance 路径零变化。
- **Acceptance Criteria Addressed**: AC-9（基础设施部分）, AC-19
- **Test Requirements**:
  - `rule` TR-1.1: 旧 lease/record JSON（无 discovery 字段）可正常反序列化且序列化回不含新字段；带 provenance 的对象序列化包含相同 applyId。证据：新单测（`go test`）。
  - `rule` TR-1.2: 带 provenance 的 reservation Put 后，不产生任何 DNS 记录（即使 scope 配置了 DNS zone）；无 provenance 的旧路径仍产生正反向记录。证据：测试对照断言。
  - `rule` TR-1.3: 多个事件处理器并发/顺序 `ReportOutcome` 不丢失、无数据竞争。证据：`go test -race`。
  - `rubric` TR-1.4: 架构契合（无 import 环、DispatchEvent 签名不变、字段 omitempty）；scale 1-5；anchors 1=破坏旧签名/产生环；3=可用但有妥协；5=完全附加式且无环；threshold >= 4；证据：`go build ./...` 与代码走查。
- **Notes**: dhcp/dns 包仅 import `discovery/types` 叶子包，禁止 import discovery 角色包。

## Task 2: 扫描稳定身份、来源修订与不可变快照
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1
- **Completion Evidence**:
  - TR-2.1 `rule` PASS：`TestScanLifecycleAndSnapshots`——running 元数据含 scanID/sourceRevision，finished 翻转成功、deviceCount=2，快照含 scanId/sourceRevision/contentHash。
  - TR-2.2 `rule` PASS：同测试中二次 persist 快照 ModRevision/字节不变，投影数为 0，二次 finish 翻转返回 errScanStaleTransition。
  - TR-2.3 `rule` PASS：`TestProjectionSkipsProtectedDevices` 覆盖 manual、更新扫描（SourceRevision 更高）、已领取、扫描期间改 manual、扫描期间删除 5 场景，ModRevision 前后相等断言通过，且 5 个快照全部写入。
  - TR-2.4 `rule` PASS：`TestProjectionPreservesTTLAndUpdatesSameScanDevices`——更新设备保留原 Lease ID，新建设备挂扫描租约。
  - TR-2.5 `rule` PASS：finished 元数据与快照 KV.Lease 等于扫描租约；`TestReapStaleScans` 仅将 >2h running 翻为 failed，fresh running/finished 不动。
  - `go build ./...` 通过；全 discovery 包既有测试通过；nmap 未参与单测（持久化纯函数直接测试）。
- **Description**:
  - 新增 `pkg/roles/discovery/scan.go`：`Scan`（id、subnet、cidr、sourceRevision、status、nodeID、startedAt/finishedAt、deviceCount）与 `ScanDevice`（identifier、hostname、ip、mac、subnet、scanId、sourceRevision、observedAt、contentHash）模型；键 `/discovery/scans/{subnet}/{scanID}` 与 `.../devices/{identifier}`。
  - 抽取可脱离 nmap 测试的持久化函数：`persistScanResults(ctx, scanID, sourceRevision, []Device)`：① 快照键用 `If(CreateRevision(key)=0)` 仅创建 CAS 写入（重复写失败不覆盖）；② live 设备投影条件写——先读前缀修订基线（扫描开始时获取），逐设备 Txn：键不存在→创建（source=scan、带 TTL 租约）；存在且 source≠manual 且 scanId 不新于本轮且 applyId 为空且 ModRevision=基线值→更新并保留原 Lease（KeyValue.Lease 非零时用 `WithLease(LeaseID)`）；否则跳过投影（快照照写）。
  - 改造 `Subnet.RunDiscovery`：开始时生成 scanID、通过一次 etcd Get 响应头捕获 sourceRevision；创建 running 元数据（CAS 仅一次），扫描结束翻转 finished（CAS 仅一次，附加 DiscoveryTTL 租约；失败置 failed）；快照与 finished 元数据均授予 DiscoveryTTL 租约。
  - `Device` 结构新增 FR-4 可选字段；`deviceFromKV`/`APIDevicesGet` 兼容旧值。
  - 角色 Start 时的陈旧 running 扫描回收（简单时限：running 且 startedAt 超过 2 小时→CAS 置 failed）。
- **Acceptance Criteria Addressed**: AC-1, AC-2, AC-3, AC-4, AC-17（扫描侧租约）
- **Test Requirements**:
  - `rule` TR-2.1: 持久化后 scans 元数据含 scanID 与捕获的 sourceRevision，状态 finished；`/scans/.../devices/` 每设备快照存在且含 contentHash。证据：单测 + etcd 断言。
  - `rule` TR-2.2: 相同快照键二次写入失败且原值字节不变。证据：单测断言 Txn `Succeeded=false` 与原值。
  - `rule` TR-2.3: live 设备在「人工标记/manual」「更新 scanId」「applyId 非空」「扫描期间被改动（修订偏离基线）」四场景下不被覆盖；快照仍写入。证据：4 组断言 ModRevision 不变 + 快照存在。
  - `rule` TR-2.4: 投影更新后设备键仍带非零 Lease（TTL 保留）；新建设备带 DiscoveryTTL 租约。证据：断言 KV.Lease。
  - `rule` TR-2.5: finished 元数据与快照键带租约；running→finished/failed 各只允许一次翻转。证据：断言 Lease 与二次 CAS 失败。
- **Notes**: nmap 不进入单测；`RunDiscovery` 的改动保持每子网 etcd 互斥锁不变。

## Task 3: 扫描结果查询 API 与设备视图附加字段
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 2
- **Completion Evidence**:
  - TR-3.1 `rule` PASS：`TestAPIScansGet`（3 条元数据、subnet 过滤）、`TestAPIScanDevicesGet`（带/不带 subnet 均定位快照、未知 scan 返回空）通过。
  - TR-3.2 `rule` PASS：`TestAPIDevicesGetProvenanceFieldsOptional`——旧形态设备编组无 scanId/applyId，新形态设备附加字段正确；既有 `TestAPIDevicesGet` 零修改通过。
  - 路由 `GET /api/v1/discovery/scans`、`GET /api/v1/discovery/scans/devices` 已注册；`go build ./...` 通过。
- **Description**:
  - 新增 `api_scans.go`：`GET /api/v1/discovery/scans`（可选 query `subnet`）返回扫描元数据列表；`GET /api/v1/discovery/scans/devices`（query `scanId` 必填，可按 subnet 定位前缀）返回不可变快照设备；在 role.go 注册路由并设置 usecase 名称/标签/预期错误。
  - `APIDevice` 增加可选输出字段 `scanId`、`source`、`sourceRevision`、`applyId`、`applyStatus`（omitempty，零值省略）；不改变既有必填字段与 query。
- **Acceptance Criteria Addressed**: AC-5
- **Test Requirements**:
  - `rule` TR-3.1: 写入两轮扫描后 list 接口按 subnet 过滤正确、设备快照接口内容与写入一致。证据：interactor 单测。
  - `rule` TR-3.2: 旧形态设备（仅 hostname/ip/mac）经 `APIDevicesGet` 输出不含新字段（JSON 省略），新形态设备输出附加字段。证据：JSON 编组断言。

## Task 4: DHCP/DNS ensure 事件处理器（dry-run + CAS 领取）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1
- **Completion Evidence**:
  - TR-4.1 `rule` PASS：`TestEnsureDHCPLease`/`TestEnsureDNSRecords` 的 dryRun 段——create/keep/conflict/skip 动作、目标键与当前修订正确，dry-run 后 etcd 无写入。
  - TR-4.2 `rule` PASS：ensure CAS 成功写入租约（含 discovery.applyId/scanId）与正反向记录，返回新修订；四对象 provenance 断言通过。
  - TR-4.3 `rule` PASS：`TestEnsureDHCPLeaseConflicts`——他人已改/已创建时事务失败、对象保持他人值、target_owned_other/target_changed 分类；fence 不匹配→job_not_claimable（stale fence 第 5 步）。
  - TR-4.4 `rule` PASS：同 applyId 重试（absent 旧 pin）返回 keep/already_provisioned 且 ModRevision 不变；DNS 正反向均验证。
  - 附加：scope/mac 缺失→skip；zone 缺失→skip zone_missing；`TestEnsureDNSSiblingsAndConflicts` 验证他人 uid 并存记录 siblings=1。
  - TR-4.5 `rubric` 评分 5/5：键布局/zone 解析/JSON 序列化全部留在 dhcp/dns 包内；所有写（含 keep 校验）走 Txn；事件主题与载荷键集中在 types；无 import 环；dhcp 整包测试通过（dns 仅 Blocky/IPForwarder、oui TestDB 等预存环境用例失败）。
- **Description**:
  - 新事件主题：dhcp/types `roles.dhcp.lease.ensure`；dns/types `roles.dns.record.ensure`（payload 含 dryRun、direction、applyId、scanId、期望 create/mod 修订、fenceKey/fenceRev、既有 fqdn/hostname/address/uid/expiry 字段）。
  - `pkg/roles/dhcp/event_ensure.go`：dryRun 解析 scope（缺失→skip/scope_missing），返回 `/dhcp/leases/{mac}` 当前修订与动作（不存在→create；存在且内容期望一致→keep；存在且 provenance=本作业→已配置幂等；其他→conflict 原因）；ensure 模式在单个 Txn 中比较 `CreateRevision=0` 或 `ModRevision=M` **外加** fenceKey 的 ModRevision=fenceRev，Then 写入带 provenance 的 reservation（Expiry=-1），失败分支重新读取分类冲突；成功后调用 afterPut（provenance 抑制 DNS 链）。
  - `pkg/roles/dns/event_ensure.go`：复用 FindZone 与 ReverseAddr 解析（zone 缺失→skip/zone_missing），计算 fwd/ptr 记录键；dryRun 返回键+修订+动作+他人 uid 并存提示；ensure 同构 CAS 写入带 provenance 的 A/AAAA/PTR（uid 固定为设备 identifier），事务包含 fence 比较；新增 Record 条件写方法。
  - 在两角色 `role.go` 注册监听器；旧事件与处理器不变。
- **Acceptance Criteria Addressed**: AC-8, AC-9（对象写入侧）, AC-19
- **Test Requirements**:
  - `rule` TR-4.1: dryRun 对 create/keep/conflict/skip 四态返回正确动作、目标键与当前修订，且不产生任何写。证据：测试断言 etcd 修订前后相等。
  - `rule` TR-4.2: ensure 在修订匹配时成功写入对象且 provenance 为给定 applyId/scanId；返回结果带键与新修订。证据：逐键 JSON 断言。
  - `rule` TR-4.3: ensure 在目标已被他人改动/创建时事务失败，对象保持他人值，返回 conflict 分类；fenceRev 不匹配时同样失败。证据：CAS 失败与原值断言。
  - `rule` TR-4.4: 同一 applyId 重试 ensure（对象已由本作业写入）返回 alreadyProvisioned 且不产生第二次写入（ModRevision 不变）。证据：断言。
  - `rubric` TR-4.5: 键布局/序列化零跨包复制、全部写走 Txn；scale 1-5；anchors 1=discovery 拼 dhcp/dns 键；3=部分复用；5=全部经角色内处理器；threshold >= 4；证据：代码走查。

## Task 5: 应用前差异预览 API
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 2, Task 4
- **Completion Evidence**:
  - TR-5.1 `rule` PASS：`TestApplyPreviewGreen`——三步骤 create、目标键（含 `/test/A/{ip}`、`/10.2/PTR/{ip}`）、固定修订 0 齐备；预览前后全局 etcd revision 相等（纯读）。
  - TR-5.2 `rule` PASS：`TestApplyPreviewReasons`——反向 zone 缺→zone_missing、新扫描投影→device_changed_scan、快照无 MAC→mac_required、设备已领取→device_claimed 均 executable=false；`TestApplyPreviewRolesUnavailable` 角色未启动时步骤 skip/role_unavailable 且 interactor 不报错；`TestApplyPreviewValidation` 未知 scan/targets 非法返回错误。
  - TR-5.3 `rubric` 自评 4/5：设备级+步骤级稳定原因码、动作与版本齐全（threshold>=4）；文档原因码表在 Task 11 补齐。
- **Description**:
  - 新增 `api_applies_preview.go`：`POST /api/v1/discovery/applies/preview`，入参 scanId、targets（dhcp/dns 集合）、dhcpScope、dnsZone、identifiers（可选）。
  - 校验 scan finished 且存在；读取快照与 live 设备（修订/source/scanId/applyId）；device 步骤与三个目标步骤的固定信息通过 ensure 事件 dryRun 收集；计算设备级 executable 与原因码集合（scan_expired、device_missing、device_changed_manual、device_changed_scan、device_claimed、mac_required、scope_missing、zone_missing、role_unavailable、target_changed、target_owned_other）。
  - 输出 items：快照固定字段+contentHash、deviceRev、steps[]（name、targetKey、action、expectedCreateRevision、expectedModRevision、reason、externalRecords 提示）；整个接口纯读。
- **Acceptance Criteria Addressed**: AC-6, AC-15（预览侧）, AC-18
- **Test Requirements**:
  - `rule` TR-5.1: 正常环境（scope+fwd/rev zone 齐备）返回 create 动作、目标键与可直接回传的固定修订；预览前后 etcd 总修订不变。证据：interactor 测试。
  - `rule` TR-5.2: 反向 zone 缺、MAC 缺、设备被新扫描触及、dhcp/dns 角色未注册分别产生准确原因码且 executable=false；角色缺失时整体请求不报错。证据：参数化测试。
  - `rubric` TR-5.3: 原因码分类完整性与输出可诊断性；scale 1-5；anchors 1=仅成功/失败；3=有错误但不稳定；5=设备级+步骤级稳定枚举；threshold >= 4；证据：原因码矩阵走查。

## Task 6: 应用作业模型、存储与创建/查询 API
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 5
- **Completion Evidence**:
  - TR-6.1 `rule` PASS：`TestAPIAppliesCreate`——返回 applyID/pending/fencing=1，item 快照字段与 4 个 pending 步骤持久化，GET status 可取回，作业键数量 1。
  - TR-6.2 `rule` PASS：`TestAPIAppliesCreateIdempotent`——同 idempotencyKey 返回同一 applyID，作业键 1、索引值指向该 ID。
  - TR-6.3 `rule` PASS：`TestAPIAppliesCreateValidation`——未知 scan(404)、哈希篡改、缺步骤/多余步骤、缺 scope、空 targets/items 全部 4xx 且无写入。
- **Description**:
  - 新增 `apply.go`：`ApplyJob`/`ApplyItem`/`ApplyStep` 模型（FR-11 全字段，含 fencingGeneration、nodeID、状态/原因枚举）与 `getJob/putJob/CAS(jobRev)` 存储助手；键 `/discovery/applies/{id}`、幂等索引 `/discovery/applies-idem/{key}`→applyID。
  - `POST /api/v1/discovery/applies`：校验 scan finished、逐项快照 contentHash 与存储一致、固定修订非负、目标与 scope/zone 一致性；幂等索引仅创建 CAS（重复→读取并返回同一作业）；创建作业（pending）后异步启动执行器钩子（Task 7 接入），返回 202 + 作业体。
  - `GET /api/v1/discovery/applies`（按 scanId/status 过滤）与 `GET /api/v1/discovery/applies/{id}`（404 语义）；注册全部新路由。
- **Acceptance Criteria Addressed**: AC-7
- **Test Requirements**:
  - `rule` TR-6.1: 合法提交创建唯一作业并返回 applyID，items/steps 固定项完整持久化。证据：interactor + etcd 断言。
  - `rule` TR-6.2: 相同 idempotencyKey 重复提交返回同一 applyID，作业键与索引数量均为 1。证据：两次调用比较 + 前缀计数。
  - `rule` TR-6.3: 未知/未 finished/过期 scanId、contentHash 篡改、非法 targets 返回 4xx 且无作业/索引写入。证据：断言错误码与键计数。

## Task 7: 作业执行器：fencing、逐步骤事务、设备标记、冲突隔离
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 6
- **Completion Evidence**:
  - TR-7.1 `rule` PASS：`TestApplyWorkerFullExecution`——completed，四步骤 confirmed（含 ConfirmedAt/TargetKey），设备/租约/A/PTR 四对象 provenance.applyId/scanId 同一。
  - TR-7.2 `rule` PASS：`TestApplyWorkerConflictIsolation`——B 设备被新扫描触及且租约被人预占：B item conflict（device 闸门阻止 DHCP/DNS 写入、外来租约原样保留），A applied，作业 partial。
  - TR-7.3 `rule` PASS：`TestApplyWorkerMissingReverseZonePartial`——反向 zone 缺→步骤 conflict/zone_missing，其余三步骤 confirmed，作业 partial。
  - TR-7.4 `rule` PASS：`TestApplyWorkerRolesUnavailableSkipped`——无 dhcp/dns 角色时目标步骤 skipped/role_unavailable、device_mark confirmed、作业 completed。
  - TR-7.5 `rule` PASS：fence 失效在 Task 4 `TestEnsureDHCPLease` 第 5 步断言（job_not_claimable）；worker 每次提交均带作业键 ModRevision 比较。
  - TR-7.6 `rule` PASS：`TestApplyWorkerTwoJobsSameDevice`——并发双作业竞争同一设备：一 applied 一 conflict，对象唯一、provenance 归属唯一、设备标记同属胜者；新流程无任何 Delete。
  - 关键语义：device_mark 为设备级领取闸门（冲突则其余步骤不再写，杜绝半套）；目标步骤彼此独立；etcd session 互斥锁（TTL 10s）+ 作业键 CAS fencing。
  - TR-7.7 `rubric` 评分 4/5：逐设备/逐步骤隔离与稳定原因码、zap 日志齐备（threshold>=4）。
- **Description**:
  - 新增 `apply_worker.go`：基于 etcd concurrency session 的作业锁 `/discovery/applies/locks/{id}`；执行器持作业键当前 ModRevision 作为 fencing token，每个 item/step 边界检查取消标志与作业代次。
  - 步骤编排：① device_mark（Txn 比较 live 设备 ModRevision 与 scanId，通过则附加 applyId/applyStatus/appliedAt 并保留 Lease，同时比较 fenceKey）；② 按 targets 派发 dhcp/dns ensure 事件（携带 fenceKey/fenceRev 与固定修订），回收 `EnsureOutcome`；无处理器→step skipped(role_unavailable)；③ 每步后以作业键 CAS 持久化步骤结果并刷新 fenceRev（Txn Then 中 OpGet 作业键取回新修订）。
  - 冲突分类落地：target_changed/target_owned_other/device_changed_manual/device_changed_scan/device_claimed/scope_missing/zone_missing；冲突/失败仅影响当前 item，继续其余 item；汇总终态 completed/partial/failed；全部步骤 skipped 的 item 为 skipped 且不阻断 completed。
  - 作业完成（成功或无可执行项）时不触碰未选项；不执行任何删除操作。
  - 同作业双执行器竞争由作业锁裁决（未获锁者不执行）；不同作业对同一设备的竞争由目标 CAS 裁决：负方该设备冲突并继续其余设备。
- **Acceptance Criteria Addressed**: AC-8, AC-9, AC-13, AC-14, AC-15（执行侧）, AC-18
- **Test Requirements**:
  - `rule` TR-7.1: dhcp+dns 全绿作业：设备/租约/正反向记录四对象带同一 applyId+scanId；四步骤 confirmed 含键与时间；终态 completed。证据：端到端 interactor 测试。
  - `rule` TR-7.2: 领取前人工改动目标记录、人工改动 live 设备、scan#2 更新设备三类场景：相应 step/item 冲突且原因准确，对象保持新值，其他设备成功，终态 partial。证据：3 组断言。
  - `rule` TR-7.3: 反向 zone 缺失时其余三步骤 confirmed、反向 skipped/conflict，作业 partial；无半套无 provenance 对象。证据：断言。
  - `rule` TR-7.4: 仅 dns、仅 dhcp 角色装配下对应步骤 skipped(role_unavailable) 且作业可 completed。证据：两装配测试。
  - `rule` TR-7.5: fenceRev 失效（作业被取消/推进代次）时在途事务失败，执行器退出。证据：构造旧 fenceRev 调用步骤事务断言失败。
  - `rule` TR-7.6: 并发安全：两个执行器同时运行同一作业时仅持锁者写入；两个不同作业包含同一设备并发执行后目标对象唯一且 provenance 归属唯一，负方该设备 target 冲突、其余设备成功；全程无删除。证据：goroutine/双 Role 实例并发测试 + 对象计数断言。
  - `rubric` TR-7.7: 逐设备隔离与原因码质量；scale 1-5；anchors 1=一台失败整批中止；3=隔离但原因粗糙；5=完全隔离+稳定枚举+结构化日志；threshold >= 4；证据：批量混合场景测试与日志抽查。

## Task 8: 取消作业
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 7
- **Completion Evidence**:
  - TR-8.1 `rule` PASS：`TestApplyCancelMultiDevice`（12 设备竞争取消）——终态 cancelled；confirmed 步骤对象存在且含 applyId；cancelled 步骤目标键不存在；至少 1 步未执行；全程无删除。
  - TR-8.2 `rule` PASS：`TestApplyCancelTerminalRejected`（终态作业拒绝取消且 JSON 不变）、`TestApplyCancelUnknown`（404）。
- **Description**:
  - `POST /api/v1/discovery/applies/{id}/cancel`：CAS 将 pending/running→cancelling（终态作业返回 409/当前状态，不报错崩溃）；执行器在 item/step 边界观测后停止，未执行步骤置 cancelled，作业终态 cancelled；已确认对象与 provenance 全部保留；路径中无 Delete。
- **Acceptance Criteria Addressed**: AC-11
- **Test Requirements**:
  - `rule` TR-8.1: 取消多设备作业后终态 cancelled；已 confirmed 对象带 provenance 留存；pending 步骤/未触达设备为 cancelled 且对应目标键不存在。证据：作业 JSON + etcd 断言。
  - `rule` TR-8.2: 对终态作业重复取消返回 4xx/冲突状态且不改变任何对象。证据：断言。

## Task 9: 重启恢复与终态保留租约
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 7
- **Completion Evidence**:
  - TR-9.1 `rule` PASS：`TestApplyRecoveryAfterRestart`——running 半成品作业（3 confirmed + reverse pending，对象已带 provenance）经第二 discovery 角色 Start 恢复后 completed；反向记录补齐且唯一；既有租约/A 记录 ModRevision 不变（对账不重写）。
  - TR-9.2 `rule` PASS：`TestApplyRetentionLease`——running 作业键 Lease=0；completed 作业键与幂等索引键同挂 60s 保留租约（LeaseTimeToLive 0<TTL<=60）。
  - TR-9.3 `rule` PASS：`TestDecodeRoleConfigRetentionDefault`——无新字段旧配置/空配置默认 2592000，显式覆盖 120 生效。
  - 修正：逐项提交不再提前发布终态（保持 running），终态与 FinishedAt/保留租约由 finalize 原子写入，杜绝观察者读到半成品终态；etcd Txn 多 OpPut 共享租约顺序问题通过幂等索引先写规避。
  - Start 接入：reapStaleScans + recoverApplies（立即）+ 30s 兜底巡检；per-job session 锁 TTL 10s。
- **Description**:
  - 角色 Start（watcher 启动后）枚举非终态作业，尝试作业锁并在获取后对账恢复：已带本作业 provenance 的对象协调为 confirmed（不重写），剩余步骤继续；带定时兜底巡检（如 30s）处理运行中宕机节点（session 过期后接管）。
  - 进入终态时为作业键与幂等索引授予保留租约（RoleConfig `applyRetentionSeconds`，默认 2592000）；非终态无租约。
  - RoleConfig 增字段与默认值（旧配置反序列化默认 30 天）。
- **Acceptance Criteria Addressed**: AC-12, AC-17
- **Test Requirements**:
  - `rule` TR-9.1: 作业 partial 时丢弃原执行器并新建第二个 discovery Role 实例（共享 etcd）触发恢复：唯一对象集、provenance 归属唯一、作业最终 completed、已存在对象 ModRevision 不变。证据：双实例测试。
  - `rule` TR-9.2: 终态作业键与幂等索引键带非零 Lease 且 TTL 等于配置；pending/running 作业键 Lease 为 0；RoleConfig 覆盖值生效。证据：KeyValue.Lease/LeaseTimeToLive 断言。
  - `rule` TR-9.3: 旧 RoleConfig JSON（无新字段）正常解码且默认 30 天。证据：单测。

## Task 10: 作业重试（只补未确认步骤）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 9
- **Completion Evidence**:
  - TR-10.1 `rule` PASS：`TestApplyRetryAfterZoneFix`——补建反向 zone 后重试，fencingGeneration=2，仅反向记录新建；租约/正向/设备三对象 ModRevision 全部不变；作业 completed 且四步骤 confirmed。
  - TR-10.2 `rule` PASS：`TestApplyRetryPersistentConflict`——外来租约未解除时重试保持 partial、dhcp 步骤 conflict（target_changed/target_owned_other）、外来对象 ModRevision 不变；`TestApplyRetryRejectsRunningAndCompleted` 对 completed/未知作业拒绝且零写入。
  - TR-10.3 `rule` PASS：对象已写、进度未提交的中间态由 Task 9 `TestApplyRecoveryAfterRestart` 覆盖（凭 provenance 对账 confirmed、对象唯一不重写）。
  - 重试机制：按当前 etcd 状态重新 dry-run 固定版本（repin），confirmed 步骤冻结；设备已属本作业则直接 reconcile confirmed；缺角色保持 pending 待后续；全部不可领取时直接终态不启动执行器。
- **Description**:
  - `POST /api/v1/discovery/applies/{id}/retry`：仅对 partial/failed/cancelled（显式允许取消后重试）作业推进 fencingGeneration 并重新入队；执行器跳过所有 confirmed 步骤（零写入对账），对 pending/failed/conflict/cancelled 步骤按**当前** etcd 状态重新校验固定版本：仍满足→执行；已被他人改变→升级冲突；confirmed 步骤与未选中设备绝不重写；成功后终态重算。
- **Acceptance Criteria Addressed**: AC-10
- **Test Requirements**:
  - `rule` TR-10.1: 反向 zone 修复后 retry：仅反向记录新增，设备/租约/正向记录 ModRevision 前后不变，作业 completed。证据：修订前后对比。
  - `rule` TR-10.2: completed 作业再 retry 为纯对账：无任何写入（etcd 修订不变）；冲突已固化的步骤重试后仍为冲突且不写。证据：修订计数与状态断言。
  - `rule` TR-10.3: 崩溃于「对象已写、作业进度未写」之间：retry 凭 provenance 对账 confirmed，不产生重复对象。证据：手工制造中间态后重试，对象键唯一。

## Task 11: OpenAPI 产物与文档
- **Status**: `completed`
- **Priority**: `medium`
- **Depends On**: Task 10
- **Completion Evidence**:
  - TR-11.1 `rule` PASS：`DEBUG=true go run ./cmd/server/main generateSchema` 重新生成 schema.yml 并同步 docs/static/schema.yaml；7 个新端点（scans 2 + applies 5）operationId 齐全，路径 diff 相对旧文件仅新增行；contentHash/fencingGeneration/deviceRevision/idempotencyKey/applyRetentionSeconds 等字段入 schema。
  - TR-11.2 `rule` PASS：重写 docs/content/docs/discovery/_index.md——扫描/快照不可变性、预览固定、作业生命周期与四对象同源 provenance、原因码表、取消/重试/恢复/保留策略、旧接口兼容段、API 汇总。
- **Description**:
  - 环境允许时运行 schema 生成（`make generateSchema schema.yml` 或等效 go 命令，需要本地 etcd）；否则按生成器等价风格手工更新 `schema.yml`，同步 `docs/static/schema.yaml`；确保新 usecase（scans 查询、preview、applies 创建/查询/取消/重试）全部出现在 schema。
  - 更新 `docs/content/docs/discovery/_index.md`：不可变扫描与身份/修订、预览差异与固定版本、作业状态机、冲突原因码表、取消/重试/恢复语义、保留策略、旧接口仍可用的兼容说明。
- **Acceptance Criteria Addressed**: AC-18（文档部分）, FR-23
- **Test Requirements**:
  - `rule` TR-11.1: 新端点路径与方法在 schema 产物中存在，必填字段与枚举（targets、status、action、原因码）齐备。证据：对 schema.yml 做 grep/解析断言。
  - `rule` TR-11.2: 文档覆盖每一个冲突原因码与作业状态，并包含旧接口兼容段落。证据：文档走查清单。

## Task 12: 全量回归与静态检查
- **Status**: `completed`
- **Priority**: `high`
- **Depends On**: Task 11
- **Completion Evidence**:
  - TR-12.1 `rule` PASS：`go build ./...` 零错误；`go vet ./pkg/roles/... ./pkg/storage/... ./pkg/instance/...` 干净；新文件全部 gofmt（唯一未格式化文件 internal_test_helpers_test.go 为预存、未触碰）。
  - TR-12.2 `rule` PASS：`go test -race` 在 pkg/roles 与 pkg/roles/discovery 全绿（含 16×100 并发 outcome 与全部作业测试）。
  - TR-12.3 `rule` PASS：新流程文件（apply*.go、event_ensure.go）grep 无任何 OpDelete/DeleteRange/.Delete 针对 dhcp/dns 键；仅旧 APIDevicesDelete 仍只删设备键。
  - TR-12.4 旧测试零修改：git status 确认 api_devices_test.go/subnet_test.go/role_test.go/api_subnets_test.go 未改动，TestDeviceApplyDHCP/DHCPWithDNS/DHCPWithDNSReverse/APIDevicesDelete/DiscoveryConvert 全部通过。
  - 回归命令：`go test -p1 discovery dhcp dns roles -skip 'TestRoleDNS_BlockyForwarder|TestRoleDNS_IPForwarder'` 全部 ok；被跳过的 4 个 DNS 用例与 oui/TestDB 为外网/docker 测试服务/真实 OUI 库依赖（裸仓库同样失败，已在 Task 1 注记）；跨包并发跑会因共享同一 etcd 互相 reset 干扰（预存测试基建限制），顺序 -p1 无此问题。
- **Description**:
  - 运行 `go build ./...`、`go vet ./...`、`go test -race ./pkg/roles/...`（至少覆盖 discovery/dhcp/dns/storage/instance）；确认旧测试文件零修改通过（`api_devices_test.go`、`subnet_test.go`、dhcp/dns 既有测试）。
  - 走查：新流程不存在任何针对 dhcp/dns 对象的 Delete；所有领取写均为 Txn；新字段 omitempty；无 import 环。
- **Acceptance Criteria Addressed**: AC-16, AC-19
- **Test Requirements**:
  - `rule` TR-12.1: 旧测试未修改且全部通过；新测试全绿。证据：命令输出（列示通过的包与用例数）。
  - `rule` TR-12.2: `go build ./...` 与 `go vet ./...` 零错误；`-race` 下无 data race。证据：命令输出。
  - `rule` TR-12.3: 新代码中对 `/dhcp/leases` 与 `/dns/zones` 无 Delete 调用（仅 Txn/OpPut）。证据：grep 走查记录。
  - `rubric` TR-12.4: 总体架构契合度；scale 1-5；anchors 1=存在跨层复制/裸写；3=轻微瑕疵；5=职责清晰、全部 Txn、风格统一；threshold >= 4；证据：综合走查。
