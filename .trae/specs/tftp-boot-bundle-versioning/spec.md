# TFTP 引导资料包不可变版本与原子发布 - 产品需求文档

## Overview
- **Summary**: 为 TFTP 角色增加"引导资料包（boot bundle）"能力：把一次网络引导所需的内核、initrd、iPXE 脚本及关联配置作为一个具有稳定身份的不可变版本整体接收、校验、原子发布，并为 TFTP 读取提供会话级版本固定、崩溃恢复与引用安全的垃圾回收。
- **Purpose**: 消除逐文件更新期间同一次 PXE 引导读到跨版本文件组合的问题；消除发布响应丢失、节点重启清理导致刚发布内容丢失的问题；在并发发布下给出确定仲裁。
- **Target Users**: 管理待装机设备引导资料的 Gravity 集群管理员（通过 HTTP API 操作）；执行 TFTP 网络引导的待装机节点（TFTP 客户端）。

## Goals
- 一组引导资料以"具名资料包 + 客户端指定稳定版本 ID"的不可变版本接收；必需文件全部到齐且校验通过前，不影响线上版本。
- 单一发布点（一个 etcd 事务）原子切换当前版本指针；失败、取消、内容冲突只留下可查询的候选状态。
- TFTP 客户端首次命中入口文件时固定所用版本，同一次引导后续读取全部来自该版本，与并发发布隔离。
- 发布重试按原身份幂等核对；并发发布由单调发布序号 + CAS 事务确定仲裁。
- 服务重启后恢复未完成候选与当前发布指针；GC 只删除未被当前版本、活跃读取会话、可恢复候选引用的内容。
- 现有单文件上传/下载/删除/列举接口与未使用资料包的普通 TFTP 文件读写完全兼容。

## Non-Goals
- 不提供 Web UI（现有 TFTP 管理无前端页面，本次仅 HTTP API + TFTP 协议路径）。
- 不重新生成 `api/` 目录下的 OpenAPI 客户端代码（独立模块，与本服务端构建无关）。
- 不做跨集群资料包同步/分发（沿用 etcd 既有的集群复制语义）。
- 不改变 bundled/、local/ 前缀语义与 DHCP/DNS 等其他角色。
- 不做资料包按主机 IP 的覆盖版本（资料包为全局共享具名实体）。

## Background & Context
- 代码位置：[pkg/roles/tftp](file:///Users/ding/Documents/swe/0910125/project-01/pkg/roles/tftp/role.go)，存储为 etcd KV（封装见 [pkg/storage/client.go](file:///Users/ding/Documents/swe/0910125/project-01/pkg/storage/client.go)）。
- 现状：普通文件存于 `/tftp/files/<hostIP|shared>/<name>`；HTTP 单文件接口 `GET/POST/DELETE /api/v1/tftp/files` 与 `/download`；TFTP 读路径按 `bundled/`、`local/`、etcd 三级查找（[tftp_read.go](file:///Users/ding/Documents/swe/0910125/project-01/pkg/roles/tftp/tftp_read.go)）；单 key 上限 10MB。
- etcd 支持 Txn CAS（参考 [leases.go](file:///Users/ding/Documents/swe/0910125/project-01/pkg/roles/dhcp/leases.go) 中 `CreateRevision` 比较用法），API 层使用 swaggest/rest `usecase` 元数据风格，错误用 `github.com/swaggest/usecase/status`。
- 测试基建：`tests.Setup(t)` 提供真实 etcd 与上下文；现有 TFTP 测试以 `tftp_test` 外部包 + mock `OutgoingTransfer` 直接调用 `role.Reader(...)`。
- TFTP 协议服务端可见的客户端身份只有 IP:Port（pin/tftp 的 `OutgoingTransfer.RemoteAddr()`）；一次 PXE 引导依次拉取入口程序、脚本、内核、initrd，期间服务端可能并发发布新版。

## Functional Requirements

### 资料包与版本模型
- **FR-1**: 管理员可创建全局具名资料包（bundle），TFTP 访问路径为 `boot/<bundle名>/<包内相对路径>`；`boot/` 为保留前缀。
- **FR-2**: 管理员在某资料包下用客户端指定的稳定 `versionID` 创建候选版本，提交文件清单：每项含相对路径、是否必需、是否入口文件（entrypoint）、可选 SHA-256；入口文件必须是必需项，入口至少 1 个。
- **FR-3**: 版本与文件内容不可变：文件首次写入后，同路径再次上传相同内容幂等成功，不同内容返回冲突且原内容不变；同一 `versionID` 重复创建但清单不同返回冲突，原候选不变。
- **FR-4**: 候选版本可随时查询：状态、清单、已收文件（含大小/哈希）、缺失必需文件、发布序号；未完成候选在发布前一直保留。
- **FR-5**: 管理员可取消候选版本；取消后状态可查询且不可再上传/发布，仅候选状态允许取消。

### 校验与原子发布
- **FR-6**: 只有全部必需文件到齐、所有已声明 SHA-256 校验通过的候选才能发布；否则返回明确前置条件错误，当前版本不变。
- **FR-7**: 发布在单个 etcd 事务内完成：写入资料包当前版本指针、单调递增 `publishSeq`、新版本置 `published`、旧当前版本置 `superseded`；外部观察只可能是切换前或切换后。
- **FR-8**: 发布接口按 `(bundle, versionID)` 幂等：响应丢失后用同一身份重发，若该版本已发布则返回既有发布结果（含实际 `publishSeq`），不重复递增序号；若该版本已被更新版本取代，返回其状态与历史序号供核对。
- **FR-9**: 并发发布以 etcd CAS（基于资料包元数据 ModRevision）仲裁：提交序号即仲裁顺序，每次成功发布占用一个单调 `publishSeq`，任一时刻只有一个当前版本；失败方得到冲突结果与当前实际版本信息，可决定是否重试。
- **FR-10**: 发布请求可携带 `ifCurrentVersion` 前置条件；当前指针不匹配时拒绝发布且不消耗序号。

### TFTP 读取版本固定
- **FR-11**: 客户端首次读取当前已发布版本清单中声明的入口文件时，在内存中建立 `客户端IP → (资料包, 版本ID)` 固定映射并记录访问时间；该映射对同一资料包后续所有文件读取生效，文件一律从固定版本的不可变 key 读取。
- **FR-12**: 固定期间管理员发布新版本不影响该客户端：固定版本中存在的文件读旧版；固定版本中不存在的路径返回未找到，绝不跨版本混用。
- **FR-13**: 固定映射空闲 TTL 为 10 分钟（期间任一该包请求刷新计时），过期后下一次入口读取重新解析并固定到届时当前版本。
- **FR-14**: 无固定映射时对非入口文件的读取，直接返回当前版本中的该文件但不建立固定（用于 TTL 过期后引导仍在继续的兜底）；资料包从无已发布版本时 `boot/` 路径返回未找到。

### 恢复与回收
- **FR-15**: 服务启动时从 etcd 重建资料包元数据、当前指针与全部候选（含未完成、已取消、已取代版本）状态；未完成候选可继续上传或发布。
- **FR-16**: GC 在管理员显式调用 API 时以及每次服务启动恢复后执行；仅删除不被以下引用保护的版本内容（版本元数据 + 全部文件 key）：当前版本、活跃读取会话固定的版本、所有候选（candidate）版本；返回实际删除的版本清单。
- **FR-17**: 取消（cancelled）与无会话固定的已取代（superseded）版本方可被 GC；GC 执行时点拍取会话引用快照，正在进行的读取不受影响。

### 兼容性
- **FR-18**: 现有 `GET /api/v1/tftp/files`（列举）、`/download`、`POST /api/v1/tftp/files`（上传）、`DELETE /api/v1/tftp/files` 的路径、入参与行为保持不变，且不展示/不触碰 `boot/` 数据。
- **FR-19**: TFTP 对 `bundled/`、`local/` 及按 hostIP/shared 存储的普通文件读写行为保持不变；普通 TFTP 写入不受影响（`boot/` 前缀的 TFTP 写入被拒绝并记录日志，数据仍只经 API 进入资料包）。
- **FR-20**: 资料包文件同样受单 key 10MB 上限约束；bundle 名与包内相对路径做合法性校验（拒绝 `..`、绝对路径、空段、保留前缀冲突等）。

## Non-Functional Requirements
- **NFR-1**: 并发正确性：版本发布 CAS、会话表、GC 引用快照在 `go test -race` 下无数据竞争；锁粒度不覆盖 etcd IO 的长等待。
- **NFR-2**: 可读性/可维护性：版本状态机、键布局、仲裁逻辑集中在独立文件中，新代码遵循现有 usecase/日志/错误码风格。
- **NFR-3**: 可观测：候选创建、上传冲突、发布仲裁结果、版本固定/过期、GC 删除均有结构化日志。
- **NFR-4**: 资源安全：后台 TTL 清扫协程随 `Role.Stop()` 退出；会话表大小受 TTL 约束，无界增长可控。
- **NFR-5**: 测试覆盖核心不变量：不完整不可发布、原子切换、读取不混版、幂等核对、并发仲裁、重启恢复、GC 保护、旧接口兼容。

## Constraints
- **Technical**: Go（module `beryju.io/gravity`）；存储仅 etcd KV 与内存态；单 value ≤10MB；测试依赖可用的 etcd（沿用 `tests.Setup`）。
- **Business**: 不破坏既有 API 与 TFTP 客户端行为；`boot/` 为新保留路径前缀（既有 per-host 空间中若存在同名 `boot/*` 文件，将改走资料包解析——视为命名空间保留，文档中注明）。
- **Dependencies**: pin/tftp/v3（提供 RemoteAddr）、etcd clientv3 Txn、swaggest/rest usecase。

## Assumptions
- 资料包全局共享，不按主机隔离（用户已确认）。
- 版本 ID 由客户端在创建候选时指定并作为幂等身份；发布顺序由 etcd 提交顺序 + 单调 `publishSeq` 仲裁（用户已确认）。
- 会话固定按客户端 IP、入口命中建立、空闲 10 分钟 TTL（用户已确认）。
- GC 仅手动 API + 启动时触发（用户已确认）。
- 重启后内存会话表为空：客户端重新命中入口时固定到届时当前版本；因此旧的 superseded 版本在启动 GC 时若无其他引用即被回收。
- 入口文件可为多个（如 BIOS/UEFI 双入口），但均必须为必需文件。
- AC-10 与 AC-11/FR-16 的调和（不改变范围）：启动 GC 采取保守策略——重启后读取会话必然全部消失，无固定的 superseded 版本对新引导已不可达，启动时回收；但 cancelled 版本在重启恢复后仍保留可查询（AC-10 明确要求 v3 状态保留），只有管理员显式调用 GC 接口时才一并清除 cancelled 版本。

## Acceptance Criteria

### AC-1: 候选版本创建与幂等身份
- **Type**: `rule`
- **Given**: 已存在资料包 `b`
- **When**: 以 `versionID=v1` 提交合法清单创建候选；随后用同一 `(b,v1)` 再次创建（相同清单）；再以同一 ID 提交不同清单
- **Then**: 首次返回候选状态；重复提交返回同一候选且不产生重复数据；清单冲突返回 409，原候选清单不变
- **Pass Condition**: 三种调用的返回码与 etcd 中 `/tftp/boot/bundles/b/versions/v1/meta` 内容完全符合预期
- **Evidence**: 新增单元测试 `TestBootVersion_CreateIdempotentConflict`

### AC-2: 分片接收、校验与候选可查询
- **Type**: `rule`
- **Given**: 含必需/可选文件及 SHA-256 声明的候选
- **When**: 逐文件上传（含错误哈希、同路径重复相同/不同内容、超过 10MB、非法路径）
- **Then**: 正确哈希与重复相同内容成功；错误哈希/内容冲突/超限/非法路径返回明确错误且已存内容不变；状态查询准确反映已收与缺失必需文件
- **Pass Condition**: 候选查询输出的 received/missing 与实际上传一致，冲突时首个写入内容保持不变
- **Evidence**: 新增单元测试 `TestBootVersion_UploadValidation`、`TestBootVersion_Status`

### AC-3: 不完整候选不可发布
- **Type**: `rule`
- **Given**: 必需文件未到齐或哈希校验未通过的候选
- **When**: 调用发布
- **Then**: 返回前置条件失败错误，资料包当前指针保持原值（或仍为空），候选状态不变
- **Pass Condition**: 发布被拒且 etcd 中资料包 meta 未被修改（ModRevision 不变）
- **Evidence**: 新增单元测试 `TestBootPublish_IncompleteRejected`

### AC-4: 完整候选单事务原子发布
- **Type**: `rule`
- **Given**: 完整且校验通过的候选 v1
- **When**: 发布成功
- **Then**: 单 etcd 事务内 current=v1、publishSeq=1、v1=published；随后对 `boot/b/<各文件>` 的读取全部返回 v1 内容
- **Pass Condition**: 发布返回 publishSeq；版本状态与指针在一次 Txn 后全部落位；TFTP 逐文件读取内容逐字节等于 v1 上传内容
- **Evidence**: 新增单元测试 `TestBootPublish_AtomicSwitch`

### AC-5: 失败与取消只留下可查询候选
- **Type**: `rule`
- **Given**: 候选 v1 与已发布 current=v0
- **When**: 取消 v1（成功）；尝试取消 v0（被拒）；取消后再上传/发布 v1（被拒）
- **Then**: v1 状态=cancelled 且仍可查询，v0 服务不受影响；v0 取消被拒；v1 上传/发布被拒
- **Pass Condition**: 各操作返回码符合状态机，current 全程为 v0
- **Evidence**: 新增单元测试 `TestBootVersion_CancelState`

### AC-6: TFTP 会话入口固定与发布隔离
- **Type**: `rule`
- **Given**: current=v1（含入口 entry 与 kernel），客户端 C1 已读取入口
- **When**: 管理员发布 v2（同名文件内容不同）；C1 随后读取 kernel；C1 请求仅 v2 存在的路径；新客户端 C2 读取入口与 kernel
- **Then**: C1 的 kernel 内容为 v1 版本；v2 独有路径对 C1 返回未找到；C2 全部读到 v2
- **Pass Condition**: 四个读取结果与版本归属完全一致，无跨版本混合
- **Evidence**: 新增单元测试 `TestBootRead_SessionPinnedAcrossPublish`

### AC-7: 会话 TTL 过期与无固定兜底
- **Type**: `rule`
- **Given**: current=v1 时 C1 命中入口被固定
- **When**: 发布 v2；等待超过 10 分钟空闲（测试中注入可控时钟）后 C1 再读入口；另一场景无固定的客户端直接读取非入口文件
- **Then**: 过期后入口重新固定到 v2 且后续读 v2；无固定时非入口读取直接返回当前版本内容且不产生固定映射
- **Pass Condition**: 固定映射按空闲 TTL 失效；兜底读取不建立映射（随后发布新版再读入口可固定新版）
- **Evidence**: 新增单元测试 `TestBootRead_SessionTTLAndFallback`

### AC-8: 发布幂等核对
- **Type**: `rule`
- **Given**: 候选 v1 首次发布已成功（占用 publishSeq=7）
- **When**: 调用方因响应丢失用同一 `(b,v1)` 重发发布；再模拟 v1 已被 v2 取代后重发
- **Then**: 第一种返回既有成功结果与 publishSeq=7 且序号不再增加；第二种返回 v1 的 superseded 状态与其历史序号，current 仍为 v2
- **Pass Condition**: 重复发布前后 publishSeq 不变；返回内容与 etcd 实际状态一致
- **Evidence**: 新增单元测试 `TestBootPublish_IdempotentReconcile`

### AC-9: 并发发布确定仲裁
- **Type**: `rule`
- **Given**: N 个 goroutine 分别持有不同完整候选并发发布，部分 goroutine 在冲突后用同身份重试
- **When**: 全部调用结束
- **Then**: publishSeq 恰好被成功提交次数单调占用、无空洞无重复；任一时刻 current 唯一；最终 current 为最后一次成功提交的版本；每个输家得到冲突结果与实际 current 信息
- **Pass Condition**: 发布历史序号集合等于连续区间 [1,k]；最终指针与最高序号版本一致；`go test -race` 通过
- **Evidence**: 新增单元测试 `TestBootPublish_ConcurrentArbitration -race`

### AC-10: 重启恢复候选与当前指针
- **Type**: `rule`
- **Given**: 已发布 v1、未完成候选 v2、已取消候选 v3 均在 etcd
- **When**: 用同一 KV 构造新的 Role 实例并执行启动恢复
- **Then**: current=v1 可直接服务；v2 可查询并可继续上传后发布；v3 状态保留；恢复后执行启动 GC 不删除 v1/v2
- **Pass Condition**: 新实例上 TFTP 读 v1 成功；补全 v2 后可成功发布；GC 删除清单不含 v1/v2
- **Evidence**: 新增单元测试 `TestBootRecovery_AfterRestart`

### AC-11: 引用安全的垃圾回收
- **Type**: `rule`
- **Given**: v0=superseded 且仍被 C1 会话固定、v3=superseded 无固定、v4=cancelled、v5=candidate、v2=current
- **When**: 调用 GC
- **Then**: 仅 v4 被删除；v0/v3/v5/v2 内容保留（v0 受会话保护，v3 受候选保护？——以状态规则为准：candidate/current/会话固定者保留）；C1 固定释放后再次 GC，v0、v3 被删；删除清单准确返回
- **Pass Condition**: 两次 GC 后 etcd 中保留/删除的版本 key 集合与引用规则逐版本一致
- **Evidence**: 新增单元测试 `TestBootGC_ReferenceProtection`

### AC-12: 旧接口与普通 TFTP 兼容
- **Type**: `rule`
- **Given**: 既有 per-host 普通文件与资料包共存
- **When**: 调用旧单文件上传/列举/下载/删除；TFTP 读写普通文件、bundled/、local/ 文件；TFTP 写入 `boot/x`
- **Then**: 旧接口行为与现状一致且列举不含 boot 数据；普通 TFTP 读写路径不变；`boot/` 写入被拒绝且不落任何资料包 key
- **Pass Condition**: 既有相关测试全部通过 + 新增兼容性断言
- **Evidence**: 既有 `api_files_test.go`、`tftp_read_test.go` 全绿；新增 `TestBootLegacy_Compatibility`

### AC-13: 并发代码质量
- **Type**: `rubric`
- **Dimension**: 并发正确性与资源管理
- **Scale**: 1-5
- **Anchors**: 1 = 存在竞态/死锁或协程泄漏；3 = 正确但锁覆盖 IO 等待、注释不足；5 = 锁临界区精简、TTL/GC/发布并发语义清晰且有测试固化、Stop 可干净退出
- **Pass Threshold**: >= 4
- **Evidence**: `-race` 全套测试结果、代码审查会话表与 GC 快照实现

### AC-14: 与现有架构的一致性
- **Type**: `rubric`
- **Dimension**: API/存储/日志风格一致性
- **Scale**: 1-5
- **Anchors**: 1 = 另起一套模式、错误处理随意；3 = 功能可用但命名/分层偏离现有惯例；5 = usecase 元数据、status 错误码、KV 键布局、zap 日志与现有角色完全同构
- **Pass Threshold**: >= 4
- **Evidence**: 对照 [api_files.go](file:///Users/ding/Documents/swe/0910125/project-01/pkg/roles/tftp/api_files.go) 与 DHCP Txn 用法的代码审查

## Open Questions
- 无（4 项关键歧义已由用户确认，见 Assumptions）。
