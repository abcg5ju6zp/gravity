# TFTP 引导资料包不可变版本与原子发布 - Independent Review

- [x] CP-R1: 候选版本的稳定身份、不可变接收与校验
  - **Type**: `rule`
  - **Covers**: AC-1、AC-2、AC-5
  - 复核要点：同 (bundle,versionID) 创建幂等；异清单 409 且原候选不变；blob write-once（同内容幂等/异内容冲突且首个内容保留）；SHA256/10MB/路径校验；取消仅限 candidate 且可查询。
  - **Evidence**: R1 pass——独立审读 boot_manager.go 状态机 + TestBootVersion_* 全通过；F-2（取消后发布语义）转为 I-2 修复。

- [x] CP-R2: 单事务原子发布、幂等核对与并发仲裁
  - **Type**: `rule`
  - **Covers**: AC-3、AC-4、AC-8、AC-9
  - 复核要点：不完整候选 FailedPrecondition 且 bundle meta 不变；发布为单 Txn（指针+seq+新旧版本状态）；同身份重发不增 seq、superseded 对账；ifCurrentVersion；双 ModRevision CAS、输家 Aborted 携 actual current；高并发 seq 连续唯一。
  - **Evidence**: R1 pass——boot_publish.go L110-L147 单 Txn 逐行确认；确定性 CAS 失败测试 + 24 goroutine 仲裁测试 -race 通过；A-2 瞬时标签随 I-2 顺带加固。

- [x] CP-R3: TFTP 读取入口版本固定与 TTL
  - **Type**: `rule`
  - **Covers**: AC-6、AC-7
  - 复核要点：入口命中按客户端 IP 固定；固定期间跨发布不混版、缺失路径 NotFound 不回落；无固定非入口读当前版本但不建 pin；10 分钟空闲 TTL 惰性+janitor 过期；无发布版本报错。
  - **Evidence**: R1 pass——readBootFile/pinIfAbsent 分支独立推演确认锁外 IO；boot_read_test 与注入时钟内部测试通过。

- [x] CP-R4: 重启恢复与引用安全 GC
  - **Type**: `rule`
  - **Covers**: AC-10、AC-11
  - 复核要点：启动恢复 current/候选/取消状态；启动 GC 仅回收无固定 superseded、cancelled 保留；手动 GC 另清 cancelled；保护集=current+候选+会话快照；删除含 files 前缀+meta。
  - **Evidence**: R1 pass——GC 快照窗口竞态独立推演安全；双实例恢复与两轮 GC 矩阵测试通过。

- [x] CP-R5: 旧接口与普通 TFTP 路径兼容
  - **Type**: `rule`
  - **Covers**: AC-12
  - 复核要点：单文件四接口行为/命名空间不变且列举不含 boot；bundled/local/host/shared 读写不变；boot/ TFTP 写入在接收数据前拒绝且无任何 key 落盘。
  - **Evidence**: R1 pass——git diff 证实 api_files.go 零改动；TestBootLegacy_Compatibility 与全部既有测试通过。

- [x] CP-U1: 并发正确性与资源管理
  - **Type**: `rubric`
  - **Covers**: AC-13
  - **Scale**: 1-5
  - **Anchors**: 1 = 竞态/死锁/协程泄漏；3 = 正确但锁覆盖 IO 或注释不足；5 = 锁临界区精简、TTL/GC/发布语义清晰有测试固化、Stop 干净退出
  - **Pass Threshold**: >= 4
  - **Evidence**: R1 4/5——-race 全套（含 24 goroutine、shuffle、DEBUG DPanic）无竞态；扣分项：会话表无硬容量上限、CAS 输家信息仅在 description（A-1 advisory）。

- [x] CP-U2: 与现有架构的一致性
  - **Type**: `rubric`
  - **Covers**: AC-14
  - **Scale**: 1-5
  - **Anchors**: 1 = 另起模式/错误随意；3 = 可用但偏离惯例；5 = usecase 元数据/status 错误码/键布局/zap 日志与现有角色同构
  - **Pass Threshold**: >= 4
  - **Evidence**: R1 4/5——同构性确认；F-1（列举后缀碰撞）转 I-1 修复，修复后该扣分消除。

## Review History

### Review R2
- **Result**: `pass`
- **Evidence**: 全新只读评审者（fresh context）独立验证 I-1/I-2 修复并复核全部检查点：
  - F-1：parseVersionMetaKey 要求去前缀恰 4 段（seg1=versions、seg3=meta），blob 键恒 ≥5 段，结构上不可碰撞；parseBundleMetaKey 恰 2 段互斥；评审者以 21 个自构边界 key（files/meta、files/a/meta/b、深层路径、bundle/version 同名 meta、旧 /tftp/files 键等）实测分类正确；空/损坏 value 三处入口均 warn-skip 不 500；TestBootVersion_ListWithMetaFilePaths PASS。
  - F-2：cancelled 在任何 Txn 前返回 FailedPrecondition（不耗 seq），TestBootPublish_CancelledRejected PASS（拒绝后 seq=0、v2 随后占 seq1）；A-2 重读分支经线性一致性推演成立。
  - 实跑：go build ./...、go vet、gofmt 干净；`go test -race -count=1 ./pkg/roles/tftp/` ok（另含 shuffle 轮与 30/30 逐用例 -v）；14 条 AC 证据映射逐一核对。
  - CP-R1..CP-R5 pass；CP-U1/U2 均 4/5。
  - Advisory（不阻塞，记录为 backlog）：R2-A1 列举前缀扫描会拉取 blob value，规模增大后建议 WithKeysOnly/meta 独立键空间；R1-A1 CAS 输家 current/seq 建议结构化字段。
  - R2-A2（trivial 死代码 bootDeletedVersion）已在评审后立即清理并重新通过全量 -race 验证。

### Review R1
- **Result**: `fail`
- **Evidence**: 独立评审（fresh subagent，只读）：CP-R1..R5 全 pass、CP-U1/U2 均 4/5；`go build ./...`、`go vet`、gofmt、`go test -race ./pkg/roles/tftp/` 32/32 PASS（另加 shuffle、DEBUG 两轮）。Actionable 发现：
  - F-1（medium）：合法包内路径 `meta`/`cfg/meta` 的 blob 键以 `/meta` 结尾，被 listVersions 的后缀匹配误判为版本 meta——二进制内容致列举 500，JSON 内容致幽灵条目。→ Issue I-1。
  - F-2（low）：cancelled 版本再发布返回 200+state=cancelled，与 AC-5"发布被拒"不符且缺测试。→ Issue I-2。
  Advisory（不阻塞）：A-1 输家信息结构化（current/seq 仅在 description）；A-2 幂等对账两次独立 Get 的瞬时标签（随 I-2 顺带加固）；A-3 enumerate 对 blob 键误告警（随 I-1 消除）。
  未覆盖：多进程共享 etcd 的 pin 行为（设计为进程内存态，Non-Goal）、真实网络 TFTP 压测、tftp 包以外测试（go build 全绿）。
