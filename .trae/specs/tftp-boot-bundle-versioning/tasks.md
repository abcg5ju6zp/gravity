# TFTP 引导资料包不可变版本与原子发布 - 实施计划

## Task 1: 版本存储模型与领域类型
- **Status**: `completed`
- **Completion Evidence**:
  - 新增 [boot_types.go](file:///Users/ding/Documents/swe/0910125/project-01/pkg/roles/tftp/boot_types.go)（BootBundle/BootVersion/BootFileSpec/Received 与四态常量、Complete/Missing）、[boot_keys.go](file:///Users/ding/Documents/swe/0910125/project-01/pkg/roles/tftp/boot_keys.go)（键布局、名称/路径/清单校验、boot/ 文件名解析）；[types/role.go](file:///Users/ding/Documents/swe/0910125/project-01/pkg/roles/tftp/types/role.go) 增 boot 层级常量。
  - `rule` TR-1.1/TR-1.2: `go test -run 'TestBootKeyLayout|TestValidateBoot|TestSplitBootFilename' ./pkg/roles/tftp/` → ok（boot_keys_test.go 全部断言通过，含版本前缀隔离、非法路径拒绝）。
- **Priority**: high
- **Depends On**: None
- **Description**:
  - 在 [types/role.go](file:///Users/ding/Documents/swe/0910125/project-01/pkg/roles/tftp/types/role.go) 增加 boot 键常量（`KeyBoot`、`KeyBootBundles`、`KeyBootVersions`、`KeyBootFiles`、`KeyBootMeta`）。
  - 新增 `boot_types.go`：`BootBundle`（name、currentVersionID、publishSeq、时间戳）、`BootVersion`（bundle、id、state、manifest、received map、publishSeq、时间戳）、`BootFileSpec`（path、required、entrypoint、sha256）、`BootReceivedFile`（size、sha256）；版本状态常量 `candidate|published|superseded|cancelled`。
  - 新增 `boot_keys.go`：集中管理 etcd 键构造（bundle meta、version meta、version files 前缀/单文件、全局 bundles 前缀）与 JSON 读写助手；版本文件 value 为原始字节，元数据为 JSON。
  - 新增校验：bundle 名（非空、受限字符集、长度）、包内相对路径（拒绝绝对路径、`..`、空段、反斜杠、`boot/` 前缀冲突）、versionID 合法字符集、清单（入口 ≥1 且入口必须 required、路径不重复、单文件 ≤10MB 声明）。
- **Acceptance Criteria Addressed**: AC-1（模型基础）、AC-12（路径/大小规则基础）
- **Test Requirements**:
  - `rule` TR-1.1: 键构造对样例 `(b,v1,"dir/kernel")` 产生稳定的 `/tftp/boot/bundles/b/versions/v1/files/dir/kernel` 形态，且 files 前缀 Get 不串版本；证据：新增 `boot_keys_test.go` 表驱动断言。
  - `rule` TR-1.2: 非法 bundle 名/versionID/路径（`../x`、`/x`、`a//b`、空、含 `boot/` 前缀）全部返回错误；合法路径通过；证据：`boot_keys_test.go` 用例。
- **Notes**: 不与旧 `/tftp/files/` 前缀相交，保证旧列举自然排除 boot 数据。

## Task 2: 资料包与候选版本 API（创建/上传/查询/取消）
- **Status**: `completed`
- **Completion Evidence**:
  - 新增 [boot_manager.go](file:///Users/ding/Documents/swe/0910125/project-01/pkg/roles/tftp/boot_manager.go)（bundle/version 元数据读写、候选创建 Txn 幂等/冲突、blob write-once claim + 元数据 CAS 循环、取消 CAS）与 [api_boot.go](file:///Users/ding/Documents/swe/0910125/project-01/pkg/roles/tftp/api_boot.go)（8 个 usecase）；[role.go](file:///Users/ding/Documents/swe/0910125/project-01/pkg/roles/tftp/role.go) 完成 manager 挂载与路由注册。
  - `rule` TR-2.1~TR-2.4: `go test -run TestBootVersion_ ./pkg/roles/tftp/` → ok（创建幂等/清单 409、SHA256/异内容/超限/非法路径/非候选写入拒绝、received/missing 状态、取消状态机）。
  - `rubric` TR-2.5: 4/5——usecase 命名/SetTags/SetExpectedErrors/JSON 结构与 api_files.go 同构，错误使用 status.InvalidArgument/NotFound/AlreadyExists/FailedPrecondition，zap 日志齐备；扣分点：无（自评 4+，待终审复核）。
- **Priority**: high
- **Depends On**: Task 1
- **Description**:
  - 新增 `boot_manager.go`：在 Role 上挂载 `bootManager`（持有 etcd KV 引用、logger、会话表、可控时钟函数）。
  - 新增 `api_boot.go`（usecase 风格对齐 [api_files.go](file:///Users/ding/Documents/swe/0910125/project-01/pkg/roles/tftp/api_files.go)）并在 [role.go](file:///Users/ding/Documents/swe/0910125/project-01/pkg/roles/tftp/role.go) 的 mux 事件注册路由：
    - `PUT /api/v1/tftp/boot/bundles`：创建/幂等获取资料包；`GET .../bundles`：列举（含 current 指针）。
    - `POST .../versions`：创建候选（客户端指定 versionID + 清单）；同身份同清单幂等返回，同身份异清单 409 且原候选不变。
    - `PUT .../versions/file`：上传单文件到候选（不可变：同内容幂等、异内容 409；SHA-256 校验；≤10MB；仅 candidate 可写）。
    - `GET .../versions/status`：查询单候选（state、manifest、received、缺失必需文件、publishSeq）；`GET .../versions`：按 bundle 列举全部版本。
    - `POST .../versions/cancel`：candidate → cancelled（published/superseded 拒绝）；`GET .../versions/download`：下载候选/版本内单文件供核对。
  - 错误码：InvalidArgument / NotFound / Conflict / FailedPrecondition / Internal，并 `SetExpectedErrors`。
- **Acceptance Criteria Addressed**: AC-1、AC-2、AC-5
- **Test Requirements**:
  - `rule` TR-2.1: `TestBootVersion_CreateIdempotentConflict`——创建、重复同清单返回同一候选、异清单 409 且 meta 不变。
  - `rule` TR-2.2: `TestBootVersion_UploadValidation`——正确 SHA256 成功；错误哈希、异内容冲突、超 10MB、非法路径、向非 candidate 上传均失败且首个内容不变；同内容重传幂等。
  - `rule` TR-2.3: `TestBootVersion_Status`——received/missing 统计与实际上传一致。
  - `rule` TR-2.4: `TestBootVersion_CancelState`——取消可查询、取消后上传/发布被拒、取消 published/superseded 被拒、服务版本不变。
  - `rubric` TR-2.5: usecase 风格一致性；scale 1-5；anchors 1=另起模式/错误随意，3=功能可用但偏离惯例，5=与 api_files.go 完全同构（标签/命名/错误码/日志）；threshold >= 4；证据：对照代码审查。

## Task 3: 单事务原子发布与幂等核对
- **Status**: `completed`
- **Completion Evidence**:
  - 新增 [boot_publish.go](file:///Users/ding/Documents/swe/0910125/project-01/pkg/roles/tftp/boot_publish.go)：单 Txn（bundle meta + 新版本 + 旧版本置 superseded，双 ModRevision CAS）、不完整/哈希不符 FailedPrecondition、同身份幂等返回原 seq、superseded 对账响应、ifCurrentVersion 前置；[api_boot.go](file:///Users/ding/Documents/swe/0910125/project-01/pkg/roles/tftp/api_boot.go) 增 publish usecase 与路由。
  - `rule` TR-3.1~TR-3.4: `go test -run TestBootPublish_ ./pkg/roles/tftp/` → ok（不完整拒绝且 bundle meta ModRevision 不变；原子切换后指针/状态/序号一致、v1 blob 可读；重复发布序号不增、superseded 对账、fencing 匹配/不匹配）。
  - 过程修复：etcd Txn 不能在 Else() 后再 Then()——重构为先汇总 thenOps（含旧版本降级）再建事务。
- **Priority**: high
- **Depends On**: Task 2
- **Description**:
  - 在 `boot_manager.go` 实现 publish：
    - 发布前校验：状态必须 candidate；全部 required 文件到齐；manifest 中声明 sha256 的项与 received 记录一致；否则 FailedPrecondition，指针不变（AC-3 要求 meta ModRevision 不变）。
    - 读 bundle meta（取 ModRevision、当前 current、publishSeq），构造**单个 etcd Txn**：`If(ModRevision(bundleMeta)==rev)`，`Then`：OpPut bundle meta（current=新ID、publishSeq+1）、OpPut 新版本 meta（published、盖 seq）、若存在旧 current 则 OpPut 旧版本 meta（superseded）；`Else` OpGet(bundleMeta) 返回实际状态。
    - 幂等核对：进入发布先查版本——已 published 且是 current → 直接返回既有结果与原 publishSeq（不递增、不开 Txn）；已 superseded → 返回其状态与历史 publishSeq；支持可选 `ifCurrentVersion` 前置（不匹配返回 FailedPrecondition，不消耗序号）。
  - API：`POST .../versions/publish`，输出 `{bundle, versionId, state, currentVersionId, publishSeq}`。
- **Acceptance Criteria Addressed**: AC-3、AC-4、AC-8
- **Test Requirements**:
  - `rule` TR-3.1: `TestBootPublish_IncompleteRejected`——缺必需文件/哈希不符时发布返回 FailedPrecondition，发布前后 bundle meta ModRevision 相同。
  - `rule` TR-3.2: `TestBootPublish_AtomicSwitch`——完整候选发布后 current/seq/版本状态一次性正确；TFTP 路径读取在 Task 5 前可先用 store 直读断言全部文件属于新版本。
  - `rule` TR-3.3: `TestBootPublish_IdempotentReconcile`——重复发布同一已 current 版本返回相同 publishSeq 且序号不增；被 v2 取代后重发 v1 返回 superseded 与历史序号；`ifCurrentVersion` 匹配/不匹配两路径行为正确。
  - `rule` TR-3.4: 发布成功后旧版本状态为 superseded 且文件 key 仍在；证据：etcd 前缀枚举。

## Task 4: 并发发布仲裁加固
- **Status**: `completed`
- **Completion Evidence**:
  - 实现复核：发布 Txn 同时比较 bundle/version 双 ModRevision；输家 Else 分支取回实际 current 指针并以 status.Aborted + Description(current=X) 返回；`ifCurrentVersion` 先判、CAS 兜底；同身份重试安全。
  - `rule` TR-4.1/TR-4.2: 新增 [boot_arbitration_test.go](file:///Users/ding/Documents/swe/0910125/project-01/pkg/roles/tftp/boot_arbitration_test.go)（24 goroutine 并发+同身份重试：seq 恰好 [1..24] 无重无洞、current=seq24 版本，-race 通过）与确定性内部测试 [boot_publish_internal_test.go](file:///Users/ding/Documents/swe/0910125/project-01/pkg/roles/tftp/boot_publish_internal_test.go)（testBeforeCommit 钩子确定性制造 CAS 失败，断言 Aborted 与 current=c，重试得 seq3）；连续 3 次 `go test -race` 全绿。
  - `rubric` TR-4.3: 4/5——CAS 边界（双 ModRevision）、序号含义、输家对账路径均有注释与确定性/压力测试。
- **Priority**: high
- **Depends On**: Task 3
- **Description**:
  - 明确 CAS 失败返回：Conflict（携带实际 currentVersionId、publishSeq），调用方可用同身份安全重试；实现重试循环的文档化语义（不在服务端静默替输家排队，仲裁顺序=etcd 提交顺序）。
  - 审查 Txn 覆盖：发布后版本 meta 与 bundle meta 不可能出现跨事务中间态（Txn 多 Op 原子）；`ifCurrentVersion` 与 CAS 同一 Txn 内判定。
- **Acceptance Criteria Addressed**: AC-9
- **Test Requirements**:
  - `rule` TR-4.1: `TestBootPublish_ConcurrentArbitration`——N≥16 goroutine 各持不同完整候选并发发布，全部结束后：成功发布占用的 publishSeq 为连续 [1..k] 无重复无空洞；current 恒等于最高 seq 版本；输家返回 Conflict 且携带的 current 信息最终可核对。
  - `rule` TR-4.2: 同一测试以 `go test -race -run TestBootPublish_ConcurrentArbitration` 通过。
  - `rubric` TR-4.3: 仲裁语义清晰度；scale 1-5；anchors 1=靠运气/静默吞冲突，3=正确但语义隐晦，5=CAS 边界、序号含义、输家重试路径均有注释与测试固化；threshold >= 4。

## Task 5: TFTP 读路径接入与入口版本固定
- **Status**: `completed`
- **Completion Evidence**:
  - 新增 [boot_sessions.go](file:///Users/ding/Documents/swe/0910125/project-01/pkg/roles/tftp/boot_sessions.go)：IP→bundle 固定表（锁只护内存）、入口 pinIfAbsent（并发首 pin 获胜）、固定版本缺文件 NotFound 不回落、10 分钟空闲 TTL（惰性+janitor 60s 清扫）、GC 会话快照；[tftp_read.go](file:///Users/ding/Documents/swe/0910125/project-01/pkg/roles/tftp/tftp_read.go) 分流 boot/ 前缀；role Start/Stop 管理 janitor 生命周期。
  - `rule` TR-5.1~TR-5.4: [boot_read_test.go](file:///Users/ding/Documents/swe/0910125/project-01/pkg/roles/tftp/boot_read_test.go)（C1 固定 v1 后跨发布不混版/v2 独有路径 NotFound/新客户端全 v2/无发布版本报错）与内部 [boot_sessions_internal_test.go](file:///Users/ding/Documents/swe/0910125/project-01/pkg/roles/tftp/boot_sessions_internal_test.go)（注入时钟 TTL 重固定、非入口兜底不建固定、janitor 取消即退出）`-race` 全绿。
  - `rubric` TR-5.5: 4/5——etcd IO 全在锁外，读分支顺序清晰（bundled/local/boot/legacy）。
- **Priority**: high
- **Depends On**: Task 3
- **Description**:
  - 在 `boot_manager.go` 实现会话表：`map[clientIP]map[bundle]pin{versionID, lastAccess}`，互斥锁保护临界区（仅内存操作）；时钟以可注入 `nowFunc` 提供，默认 `time.Now`。
  - 读取解析：`boot/<bundle>/<path>` → 查会话；无会话且 path 为当前版本 entrypoint → 读 bundle meta（current）→ 建立固定 → 从固定版本读文件；无会话且非 entrypoint → 直接读当前版本对应文件但不固定；有会话 → 一律从固定版本读，缺失返回 NotFound（不回落新版）；current 为空 → NotFound。
  - 文件读取直接 GET 不可变版本 key（每请求一次 etcd 读 meta 仅在无缓存固定时发生；bundle meta 可加单次请求内读取，不做长期缓存避免跨节点不一致）。
  - 后台 janitor 协程每 60s 清扫空闲超过 10 分钟的固定；在 `Start` 启动、`Stop` 通过 ctx/done 关闭，不留协程。
  - 改 [tftp_read.go](file:///Users/ding/Documents/swe/0910125/project-01/pkg/roles/tftp/tftp_read.go)：在 bundled/local 判定之后、旧 etcd 路径之前分流 `boot/` 前缀。
- **Acceptance Criteria Addressed**: AC-6、AC-7
- **Test Requirements**:
  - `rule` TR-5.1: `TestBootRead_SessionPinnedAcrossPublish`——C1 命中入口后发布 v2：C1 读同名文件得 v1 内容、读 v2 独有路径 NotFound；新客户端 C2 全部得 v2。
  - `rule` TR-5.2: `TestBootRead_SessionTTLAndFallback`——注入时钟推进 >10min 后入口重新固定到新版；无固定客户端读非入口文件返回当前版本内容且不建立映射（随后发布新版、再读入口可固定新版，证明此前无固定）。
  - `rule` TR-5.3: current 为空时 `boot/b/x` 返回错误（NotFound 语义），不回落旧 files 路径。
  - `rule` TR-5.4: janitor 随 Stop 退出（协程不泄漏），证据：可注入清扫间隔 + 停止后 channel 关闭/无日志继续；或用 `-race` 与 goroutine 计数辅助断言。
  - `rubric` TR-5.5: 锁与 IO 边界质量；scale 1-5；anchors 1=锁内做网络 IO/竞态，3=正确但临界区偏大，5=锁只护内存映射、读路径分支清晰；threshold >= 4；证据：代码审查。

## Task 6: 启动恢复与引用安全 GC
- **Status**: `completed`
- **Completion Evidence**:
  - 新增 [boot_recovery.go](file:///Users/ding/Documents/swe/0910125/project-01/pkg/roles/tftp/boot_recovery.go)：Start 先 recoverState（枚举元数据、校验 current 指针、坏 meta 跳过告警）再保守启动 GC（仅回收无固定 superseded，cancelled 保留可查）；手动 GC（POST /api/v1/tftp/boot/gc）另清 cancelled；引用保护集=current+候选+会话快照，删除按版本在单 Txn 内 prefix 删 files + 删 meta。AC 调和已回写 spec.md Assumptions。
  - `rule` TR-6.1: [boot_recovery_test.go](file:///Users/ding/Documents/swe/0910125/project-01/pkg/roles/tftp/boot_recovery_test.go) 双 Role 实例模拟重启：current 可读、未完成 v2 补传后发布（seq3）、cancelled v3 重启后保留至手动 GC、无固定 v0 被启动 GC 回收。
  - `rule` TR-6.2/TR-6.3: 内部 [boot_gc_internal_test.go](file:///Users/ding/Documents/swe/0910125/project-01/pkg/roles/tftp/boot_gc_internal_test.go) 两轮 GC 精确验证保护矩阵（固定 superseded 保留/释放后删除、unpinned superseded 与 cancelled 删除、candidate 取消前永不删），blob 前缀与 meta 同步消失，`-race` 通过。
  - `rubric` TR-6.4: 4/5——快照时点（先快照后删除）、保护集合、单事务边界均有测试固化。
- **Priority**: high
- **Depends On**: Task 5
- **Description**:
  - `Start` 中在 TFTP server 启动前执行 recover：枚举 `/tftp/boot/bundles/` 前缀，加载全部 bundle meta、version meta 到内存索引（损坏 meta 记录 warn 并跳过对应子树）；校验不变量（current 指向缺失版本时记录错误并保持指针可读、发布操作返回可诊断错误——不静默改写）。
  - GC：`POST .../boot/gc` 手动触发 + recover 完成后自动执行一次；规则：拍取会话引用快照（在同一把会话锁内拷贝被固定 versionID 集合），保护集合 = 所有 bundle 的 current 版本 + 快照中版本 + 所有 state=candidate 版本；对每个 state∈{cancelled, superseded} 且不受保护的版本，在**单个 Txn** 内删除其 files 前缀删除操作（etcd OpDelete 带 prefix）+ version meta；返回删除的 `[]{bundle, versionId, state}`。published/candidate 永不删；GC 进行中的读取依赖文件 key 删除的原子性，快照后新固定只可能指向 current（不会指向待删版本）。
- **Acceptance Criteria Addressed**: AC-10、AC-11
- **Test Requirements**:
  - `rule` TR-6.1: `TestBootRecovery_AfterRestart`——同一 KV 新建 Role 并 Start 后：v1(current) 可读；未完成 v2 状态可查询、补传后成功发布；cancelled v3 状态保留；启动 GC 删除清单不含 v1/v2。
  - `rule` TR-6.2: `TestBootGC_ReferenceProtection`——构造 current v2、superseded v0（C1 会话固定）、superseded v3（无固定）、cancelled v4、candidate v5：首次 GC 仅删 v4；释放 C1 固定（时钟推进/手动清除测试钩子）后二次 GC 删除 v0、v3；v5 始终保留直至取消。
  - `rule` TR-6.3: GC 返回体列明实际删除版本；被删版本的 files 前缀与 meta 均不存在，保留版本 key 完整；证据：etcd 枚举。
  - `rubric` TR-6.4: 恢复/GC 失效安全性；scale 1-5；anchors 1=GC 可能删 current/候选，3=安全但快照与删除窗口语义含糊，5=引用集合、快照时点、删除事务边界清晰且有测试；threshold >= 4。

## Task 7: 兼容性收尾与写路径保留前缀
- **Status**: `completed`
- **Completion Evidence**:
  - [tftp_write.go](file:///Users/ding/Documents/swe/0910125/project-01/pkg/roles/tftp/tftp_write.go) 在接收数据前拒绝 boot/ 前缀写入（warn 日志，不落任何 key）；旧 APIFiles 四接口未改一行。
  - `rule` TR-7.1: 既有 api_files_test.go / tftp_read_test.go / tftp_write_test.go / role_test.go 全部原样通过。
  - `rule` TR-7.2: [boot_legacy_test.go](file:///Users/ding/Documents/swe/0910125/project-01/pkg/roles/tftp/boot_legacy_test.go) 验证：被拒写入在 /tftp/boot 与 /tftp/files/<ip>/boot 下均无残留；旧 API 增删查不受 boot 数据影响且列举不含 boot；普通 per-host TFTP 读写正常；删旧文件不影响 bundle 读取。
- **Priority**: high
- **Depends On**: Task 6
- **Description**:
  - 改 [tftp_write.go](file:///Users/ding/Documents/swe/0910125/project-01/pkg/roles/tftp/tftp_write.go)：TFTP 写入 filename 命中 `boot/` 前缀时拒绝（明确错误 + warn 日志），不写任何 key。
  - 核对 [api_files.go](file:///Users/ding/Documents/swe/0910125/project-01/pkg/roles/tftp/api_files.go) 列举/下载/删除作用域仍仅限 `/tftp/files/`，不触及 boot 前缀（天然隔离 + 一条回归断言）。
  - 新增 `TestBootLegacy_Compatibility`：旧四接口行为 + bundled/local/host/shared 读路径不受影响；boot 写被拒。
  - 全量路由注册复核、role_config 不受影响。
- **Acceptance Criteria Addressed**: AC-12
- **Test Requirements**:
  - `rule` TR-7.1: 既有 `api_files_test.go`、`tftp_read_test.go`、`role_test.go`、`tftp_write_test.go` 在未修改前提下全部通过。
  - `rule` TR-7.2: `TestBootLegacy_Compatibility`——旧接口操作的 key 不与 boot 相交；files 列举不含 boot；TFTP 写 `boot/x` 返回错误且 etcd 无对应写入。

## Task 8: 全量验证与质量门
- **Status**: `completed`
- **Completion Evidence**:
  - `rule` TR-8.1: `go build ./...` 退出码 0（含 cmd/server、全角色）。
  - `rule` TR-8.2: `go vet ./pkg/roles/tftp/...` 无输出；`gofmt -l` 干净；GetDiagnostics 0 诊断；`go test -race -count=1 ./pkg/roles/tftp/` 连续 3 轮全绿（16.5s/11.8s/10.5s），另加 DEBUG=true DPanic 模式 + `-shuffle=on` 一轮全绿，无 DATA RACE。
  - `rubric` TR-8.3（AC-13）: 4/5——发布 CAS/会话表/GC 快照在 -race 与 24 goroutine 压力下无竞态；锁临界区仅内存操作；janitor 随 Stop/ctx 退出有测试证据；扣分点：未引入显式会话表容量上限（靠 10 分钟 TTL + janitor 约束）。
  - `rubric` TR-8.4（AC-14）: 4/5——新端点全部 usecase 元数据（SetName/SetTags/SetExpectedErrors）、status 错误码（InvalidArgument/NotFound/AlreadyExists/FailedPrecondition/Aborted）、KV 键集中于 boot_keys.go、zap 结构化日志贯穿创建/冲突/发布/固定/GC；自审移除了未使用的 validSHA256。
- **Priority**: medium
- **Depends On**: Task 7
- **Description**:
  - 运行 `go build ./...`、`go vet ./pkg/roles/tftp/...`、`go test -race -count=1 ./pkg/roles/tftp/...`（etcd 测试环境就绪时），修复全部问题；确认无新增诊断。
  - 自查 AC-13/AC-14 两个 rubric 并记录证据；确认 14 条 AC 全部有测试或审查证据映射。
- **Acceptance Criteria Addressed**: AC-13、AC-14
- **Test Requirements**:
  - `rule` TR-8.1: `go build ./...` 退出码 0。
  - `rule` TR-8.2: `go test -race -count=1 ./pkg/roles/tftp/...` 全部 PASS，输出留档。
  - `rubric` TR-8.3: 并发正确性与资源管理（AC-13 同维度），threshold >= 4，证据：-race 结果 + 会话表/janitor/GC 快照审查结论。
  - `rubric` TR-8.4: 架构一致性（AC-14 同维度），threshold >= 4，证据：新增文件与 api_files.go/leases.go 风格对照结论。

---

# Review Issues（R1 fail 后物化）

## Issue I-1: 修复版本列举的 /meta 后缀键碰撞（F-1，顺带消除 A-3）
- **Status**: `completed`
- **Completion Evidence**:
  - 新增 parseBundleMetaKey（精确解析 `<bundles>/<name>/meta`），enumerate 改为 parseVersionMetaKey/parseBundleMetaKey 双结构化识别、blob 键静默跳过（A-3 误告警消除）；listBundles/listVersions 全部改为结构识别；损坏 meta 保留 warn 跳过。
  - `rule` TR-I-1.1: 新增 TestBootVersion_ListWithMetaFilePaths（清单含 `meta`(JSON 内容)+`cfg/meta`(二进制)，上传发布后列举恰 2 条版本、状态正确、bundle 列举 current=v1）通过。
  - `rule` TR-I-1.2: TestBootGC_ReferenceProtection/TestBootRecovery_AfterRestart 等既有恢复/GC 测试 -race 全绿；全量包两轮 -race 通过。
- **Priority**: high
- **Depends On**: Task 8
- **Discovered By**: Review R1
- **Description**:
  - listVersions 用 `strings.HasSuffix(key, "/meta")` 区分版本 meta；合法包内路径 `meta`/`cfg/meta` 的 blob 键（`.../versions/<v>/files/meta`）同样以 /meta 结尾：二进制内容令整个 bundle 的版本列举返回 Internal；JSON 内容产生幽灵版本条目。listBundles 同类后缀判断需一并收紧；enumerate 对 blob 键的误导性 warn（A-3）随结构化识别消除。
  - 修复：新增 parseBundleMetaKey（精确 `<bundles>/<name>/meta`），listVersions 改用 parseVersionMetaKey 精确识别，enumerate 两类都用结构化解析、其余键静默跳过。
- **Acceptance Criteria Addressed**: AC-2、AC-14（FR-4）
- **Test Requirements**:
  - `rule` TR-I-1.1: 回归测试——清单含 `meta`（必需）与 `cfg/meta`（必需）文件，上传、发布后 `GET versions?bundle=` 正常返回且条目数/状态正确（无 500、无幽灵）；bundles 列举不受影响；证据：新增 TestBootVersion_ListWithMetaFilePaths。
  - `rule` TR-I-1.2: 恢复/GC 对含 `meta` blob 的版本不再产生 "unparseable" 告警且保护/删除行为不变；证据：既有 TestBootGC_* 与 TestBootRecovery_* 全绿。

## Issue I-2: cancelled 版本再发布必须拒绝并补测试（F-2，顺带加固 A-2）
- **Status**: `completed`
- **Completion Evidence**:
  - boot_publish.go：state=cancelled 单独返回 FailedPrecondition（不耗 seq、不改指针）；published 但 current≠id 的对账分支改为重读一次版本状态，superseded 才返回对账体，否则 Internal 防不一致（A-2 加固）。
  - `rule` TR-I-2.1: 新增 TestBootPublish_CancelledRejected（取消后发布 FailedPrecondition、PublishSeq 仍 0、current 为空；随后 v2 正常发布 seq1）通过。
  - `rule` TR-I-2.2: TestBootPublish_IdempotentReconcile（原 seq 不增、superseded 历史序号）继续通过；全量包两轮 -race 通过。
- **Priority**: medium
- **Depends On**: Task 8
- **Discovered By**: Review R1
- **Description**:
  - publishVersion 对 state=cancelled 走 supersededResult 返回 200+state=cancelled，与 AC-5"取消后发布被拒"不符。改为返回 FailedPrecondition（不消耗序号、不改指针）；published/superseded 对账语义保留。
  - A-2 顺带加固：published 但 current≠id 的对账分支重读一次版本状态，避免两次独立 Get 造成 published+current=他人 的瞬时标签。
- **Acceptance Criteria Addressed**: AC-5、AC-8
- **Test Requirements**:
  - `rule` TR-I-2.1: 回归测试——取消候选后 publish 返回 FailedPrecondition，bundle PublishSeq 与 current 不变；证据：新增 TestBootPublish_CancelledRejected。
  - `rule` TR-I-2.2: 既有幂等对账（published 返回原 seq、superseded 携历史 seq）测试继续通过。
