# iron-core-cluster

`iron-core-cluster` 是 IronMesh 的集群控制面核心库，负责集群起盘、节点加入、Raft 选主、Raft TCP 通信、集群业务数据写入转发、本地状态读取和调试查询。

它服务于注册节点、网关节点和业务节点之间的集群协作，但不承载具体业务逻辑。

## 定位

| 项目 | 说明 |
|---|---|
| 不做持久化 | Raft log、vote、state machine、snapshot 继续使用内存实现。 |
| 不扩散功能 | 优先保证现有启动、TCP 通信、成员维护、日志和运行稳定性。 |
| TCP 优先 | 集群内部通信优先使用 TCP，HTTP 只作为人工验证查询入口。 |
| 固定起盘节点 | 配置中唯一 `is_boot_node = true` 的注册节点负责首次起盘。 |
| 不自动移除 voter | `cluster-reg-*` 投票节点只探测和记录日志，不自动从 membership 移除。 |
| learner 自动移除 | learner 节点的 Raft TCP 复制连接断开后，由 leader 直接移出集群。 |
| 库不默认阻塞 | `start()` 等待当前节点完成起盘或加入集群后返回运行句柄。 |

## 当前能力

| 分类 | 状态 | 说明 |
|---|---|---|
| 集群起盘 | 可用 | 唯一起盘节点初始化 Raft 集群，其它注册节点等待并加入。 |
| 节点加入 | 可用 | 注册节点加入为 voter，普通节点加入为 learner。 |
| 启动流程 | 可用 | `IronClusterManager::start()` 完成起盘或加入后返回运行句柄。 |
| 后台任务 | 可用 | TCP、HTTP、维护任务统一注册进 `JoinSet`，由运行句柄托管。 |
| Raft 通信 | 可用 | Raft RPC 使用 TCP frame 传输，客户端复用 TCP 连接。 |
| 写入路径 | 可用 | 非 leader 节点可转发集群业务数据写入到 leader。 |
| 本地状态读取 | 可用 | `IronClusterHandler` 可读取当前节点已经 apply 的本地状态机数据。 |
| learner 断线移除 | 可用 | 基于 Raft TCP 复制连接失败事件移除 learner，不做定时嗅探。 |
| voter 断线处理 | 保持 | voter 长期不可达时只记录断线日志，不自动从 membership 移除。 |
| 快照恢复 | 可用 | 完整快照元信息携带 membership 投票配置和节点真实 TCP 地址。 |
| TCP 边界 | 可用 | frame 长度限制、读写超时、空闲超时、最大连接数限制已经集中处理。 |
| 调试查询 | 可用 | HTTP 只暴露 `/health` 和 `/raft/metrics`，便于人工验证。 |
| 存储模型 | 保持 | 内存 Raft log、state machine、snapshot 是当前明确设计目标。 |

## 运行与验证

| 项目 | 说明 |
|---|---|
| 服务壳阻塞 | 外层启动壳通过 `wait_shutdown()` 决定是否常驻运行。 |
| lab 验证入口 | 固定本机端口的集群启动器放在 `iron-zenith-lab/iron-zenith-cluster-lab`。 |
| 查询入口 | `/health` 用于健康检查，`/raft/metrics` 返回 OpenRaft 原始 metrics。 |
| learner 移除验证 | 本机启动 `reg1/reg2/reg3/gate5` 后，停止 `gate5`，leader 应将节点 5 从 membership 移除。 |
| 写入验证 | 非 leader 节点写入集群业务数据时，应通过已缓存 TCP 连接转发到 leader。 |

## 生产化关注项

| 编号 | 分类 | 当前状态 | 影响 | 后续方向 |
|---|---|---|---|---|
| 1 | 自动化验证 | `cargo test -p iron-core-cluster` 当前仍是 0 tests | 只能证明编译通过，不能证明集群行为稳定 | 补充集群行为测试，覆盖节点加入、learner 移除、leader 切换和写入转发。 |
| 2 | TCP 客户端并发 | 单目标连接由一个 mutex 串行读写 | 控制面可接受，高并发写入会排队 | 后续单独设计业务写入连接池或 TCP 多路复用。 |
| 3 | 网络协议 | Raft RPC、快照、业务写入都走 JSON frame | 简单易调试，但不是高性能传输格式 | 需要更高吞吐时再评估二进制协议。 |
| 4 | 本地状态读取 | `local_state_machine_data()` clone 整个状态机 | 数据量变大后会增加读路径开销 | 后续按业务读取模型拆分轻量查询接口。 |
| 5 | 认证与鉴权 | 当前集群 TCP 和 HTTP 查询入口无认证 | 跨机器或跨环境部署时存在访问边界风险 | 生产部署前设计节点身份认证、连接鉴权和查询入口访问控制。 |
| 6 | 配置 | timeout 和 interval 当前已集中常量化 | 多环境运行时可能需要按环境调参 | 需要多环境部署时再做配置化。 |

当前 `iron-core-cluster` 已经能支撑本机和小规模 IronMesh 集群验证。集群起盘、节点加入、TCP 通信、写入转发、快照恢复和 learner 自动移除路径已经跑通。

它还没有完全达到生产级别。主要风险集中在自动化测试覆盖、高并发写入能力、网络协议性能和认证鉴权边界。

## 扩容机制方向

| 机制 | 方向 |
|---|---|
| 节点加入 | 网关节点和业务节点启动后自动 join 为 learner。 |
| 服务注册 | 服务实例、能力标签、负载信息写入 `IronClusterData`。 |
| 路由分发 | 网关从本地状态机快照读取服务拓扑，减少集中查询压力。 |
| 节点退出 | learner 断线自动移除，主动退出后续通过控制面 API 完成。 |
| 热扩容 | 新业务节点加入后，由 leader 写入服务实例，网关本地状态同步后自动参与路由。 |
| 压力均衡 | 状态机记录节点负载，网关按策略选择后端。 |
| 控制面写入 | 少量强一致写入进入 Raft，本地读优先走状态机快照。 |

## 维护原则

| 原则 | 说明 |
|---|---|
| 最小优先 | 只围绕已有集群能力做稳定性和可维护性优化。 |
| TCP 优先 | 集群内部通信优先使用 TCP，HTTP 只作为人工验证查询入口。 |
| voter 保守 | voter 不做自动移除，避免破坏多数派。 |
| learner 可清理 | learner 不参与投票，可以在 Raft TCP 复制连接断开后由 leader 自动移除。 |
| 参数集中 | 新增运行参数优先放入 `iron_raft_constants.rs`，便于统一调参。 |
