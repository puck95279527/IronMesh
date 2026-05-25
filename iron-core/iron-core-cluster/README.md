# iron-core-cluster

`iron-core-cluster` 是 IronMesh 的最小 Raft 集群库，目标是提供简单、稳定、可复用的集群启动、成员维护和查询能力。

它当前服务于注册节点、网关节点和业务节点的集群协作，但库本身不绑定具体业务逻辑。

## 目标边界

| 项目 | 说明 |
|---|---|
| 不做持久化 | Raft log、vote、state machine、snapshot 继续使用内存实现。 |
| 不扩散功能 | 优先保证现有启动、TCP 通信、成员维护、日志和运行稳定性。 |
| 库不默认阻塞 | `start()` 等待当前节点完成起盘或加入集群后返回运行句柄。 |
| 服务壳显式阻塞 | `src/bin/*` 通过 `wait_forever()` 决定是否常驻运行。 |
| 固定 Boot 起盘 | 配置中唯一 `is_boot_node = true` 的注册节点负责首次起盘。 |
| 不自动移除 voter | `cluster-reg-*` 投票节点只探测和记录日志，不自动从 membership 移除。 |
| learner 自动移除 | learner 节点 TCP 不可达后，由 leader 确认并移出集群。 |
| HTTP 仅做查询 | `/raft/metrics` 继续返回 OpenRaft 原始 metrics，不做展示层过滤。 |

## 当前状态

| 分类 | 状态 | 说明 |
|---|---|---|
| 起盘机制 | 已完成 | 唯一 `is_boot_node = true` 节点负责首次 `initialize`，其它注册节点等待并加入。 |
| 本机起盘锁 | 已移除 | 不再使用 `4999` 端口锁，避免跨机器部署时语义混乱。 |
| 启动流程 | 已整理 | `IronRaftClusterManagerFlow` 保持 5 个启动阶段，常驻等待交给服务壳。 |
| 库启动 API | 已完成 | `IronRaftClusterManager::start()` 返回 `IronRaftClusterHandle`。 |
| 后台任务 | 已收口 | TCP、HTTP、维护任务统一注册进 `JoinSet`，由运行句柄托管。 |
| learner/voter 探测 | 已优化 | 维护任务并发确认节点可达性，learner 移除仍串行执行。 |
| voter 日志 | 已节流 | voter 长期不可达时按 `VOTER_UNREACHABLE_LOG_INTERVAL` 控制日志频率。 |
| TCP 边界 | 已完成 | frame 长度限制、读写超时、最大连接数限制已经集中处理。 |
| 常量维护 | 已完成 | Raft、启动流程、TCP、join、维护任务常量集中在 `iron_raft_constants.rs`。 |
| metrics 查询 | 保持 | `/raft/metrics` 直接暴露 OpenRaft 真实 metrics，便于观察真实集群关系。 |
| 存储模型 | 保持 | 内存 Raft log、state machine、snapshot 是当前明确设计目标。 |

## 仍需优化

| 优先级 | 分类 | 优化项 | 当前状态 | 建议处理 |
|---|---|---|---|---|
| P2 | 测试 | 自动化集群场景测试 | 当前主要手动验证 | 优先补充，覆盖节点加入、learner 移除、leader 切换。 |
| P2 | 配置 | timeout 和 interval 配置化 | 当前已集中常量化 | 多环境运行需要调参时再配置化。 |
| P2 | 网络协议 | 认证与鉴权 | 当前无认证 | 跨环境生产部署时再评估是否引入。 |

## 维护原则

| 原则 | 说明 |
|---|---|
| 最小优先 | 只围绕已有集群能力做稳定性和可维护性优化。 |
| TCP 优先 | 集群内部通信优先使用 TCP，HTTP 只作为人工验证查询入口。 |
| voter 保守 | voter 不做自动移除，避免破坏多数派。 |
| learner 可清理 | learner 不参与投票，可以在确认不可达后由 leader 自动移除。 |
| 参数集中 | 新增运行参数优先放入 `iron_raft_constants.rs`，便于统一调参。 |
