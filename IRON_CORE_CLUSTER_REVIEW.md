# iron-core-cluster 评审报告

## 当前定位

`iron-core-cluster` 是 IronMesh 当前的最小集群控制面库。它负责集群节点起盘、节点加入、Raft 选主、Raft TCP 通信、集群业务数据写入转发、本地状态读取和调试查询入口。

这个库当前服务于注册节点、网关节点和业务节点之间的集群协作，但不应该承载具体业务逻辑。它的核心价值是把 IronMesh 内部集群协作能力沉淀为可复用基础设施。

## 现有能力评价

| 类别 | 当前能力 | 评价 |
|---|---|---|
| 集群起盘 | 唯一 boot 节点负责初始化最小 Raft 集群 | 方向合理，适合当前固定注册节点起盘模型 |
| 节点加入 | 普通节点通过 TCP 请求 leader 加入为 learner，注册节点可提升为 voter | 主流程已经跑通，但加入后的维护能力不足 |
| Raft 通信 | `AppendEntries`、`Vote`、完整快照通过 TCP frame 传输 | 使用 OpenRaft 是正确方向，TCP 长连接也符合项目边界 |
| 业务写入 | 外部通过 `IronClusterHandle::write_cluster_data(...)` 写入集群数据 | API 已开始收口，非 leader 能转发到 leader |
| TCP 连接 | 客户端缓存目标节点 TCP stream，服务端单连接循环处理多请求 | 具备连接复用基础，不是每次写入都重新建连 |
| 本地状态读取 | `IronClusterHandle::local_state_machine_data()` 读取当前节点已 apply 的状态机数据 | 简单直接，适合验证阶段观察本地数据 |
| HTTP 查询 | `/health` 和 `/raft/metrics` 提供人工调试查询 | 定位清晰，但不应该被当作正式业务 API |
| 文档说明 | README 描述了目标、边界和当前状态 | 部分描述已经和源码不一致，需要修正 |

## 问题与缺陷总表

| 编号 | 类别 | 问题 | 证据位置 | 影响 | 生产级评价 |
|---|---|---|---|---|---|
| 1 | 快照恢复 | 快照 membership 恢复时硬编码 `127.0.0.1:500{node_id}` | `raft/network/tcp/iron_raft_tcp_server.rs` | 端口规则变化或跨机器部署后，快照恢复出的节点地址错误 | 严重，不达生产 |
| 2 | learner 移除 | learner 自动移除已恢复为 TCP 断线事件驱动，不做定时嗅探 | `raft/network/tcp/iron_raft_tcp_client.rs`、`raft/cluster/manager/iron_raft_cluster_manager_support.rs` | learner 停止后，leader 在现有 Raft TCP 复制连接失败时移除 membership | 已修正，需人工验证 |
| 3 | leader 切换写入 | 非 leader 写入只按本地 metrics 找 leader，失败后不重新获取 leader 重试 | `cluster_api/iron_cluster_handle.rs` | leader 切换或 metrics 滞后时，业务写入容易失败 | 中高风险 |
| 4 | 写入超时 | `client_write` 复用 `JOIN_NODE_TIMEOUT = 500ms` | `raft/network/tcp/iron_raft_tcp_client.rs`、`iron_raft_constants.rs` | Raft 写入包含复制提交，500ms 容易误判超时 | 中高风险 |
| 5 | TCP 空闲连接 | 服务端等待下一帧 header 没有空闲超时 | `raft/network/tcp/iron_raft_tcp_frame.rs` | 空闲连接可长期占用连接名额 | 中高风险 |
| 6 | 连接上限 | `MAX_TCP_CONNECTIONS = 256` 只有拒绝新连接，没有连接清理策略 | `raft/network/tcp/iron_raft_tcp_server.rs` | 半连接或空闲连接多时，正常节点可能连不上 | 中风险 |
| 7 | 启动加入流程 | boot 节点加入 learner/voter 时内部无限重试 | `raft/cluster/manager/iron_raft_cluster_manager_support.rs` | 半通半坏状态下启动流程可能长期卡住 | 中风险 |
| 8 | TCP 客户端并发 | 单目标连接由一个 mutex 串行读写 | `raft/network/tcp/iron_raft_tcp_client.rs` | 控制面没问题，高并发写入会排队 | 性能限制 |
| 9 | JSON 协议 | Raft RPC、快照、业务写入都走 JSON frame | `raft/network/tcp/*` | 简单易调试，但序列化成本较高 | 可接受但非高性能 |
| 10 | 本地快照读取 | `local_state_machine_data()` clone 整个状态机 | `cluster_api/iron_cluster_handle.rs` | 数据量变大后读路径有额外内存和延迟成本 | 当前可接受，后续需优化 |
| 11 | 状态机锁粒度 | apply 时锁住整个 state machine | `raft/storage/iron_raft_state_machine_store.rs` | 当前 KV 小数据没问题，复杂业务模型会影响 apply 延迟 | 当前可接受 |
| 12 | 文档一致性 | README 仍描述维护任务、learner 自动移除、voter 日志节流 | `iron-core-cluster/README.md` | 后续开发和运维会误判实际能力 | 必须修正 |
| 13 | 自动化验证 | `cargo test -p iron-core-cluster` 为 0 tests | cargo test 输出 | 当前只能证明编译通过，不能证明集群行为稳定 | 不达生产 |
| 14 | 对外错误表达 | 转发写入失败主要包装成 `std::io::Error` | `cluster_api/iron_cluster_handle.rs`、`iron_raft_tcp_client.rs` | 上层难以区分 leader 变化、超时、网络断开、远端拒绝 | 中风险 |
| 15 | HTTP 查询 | `/raft/metrics` 直接暴露 OpenRaft metrics | `raft/query/iron_raft_query.rs` | 适合调试，不适合作为稳定业务 API | 可接受，定位需明确 |

## learner 自动移除结论

当前 `iron-core-cluster` 已恢复 learner 自动移除。

新的实现不做定时嗅探，不做失败次数累计。leader 基于 OpenRaft TCP 复制连接的真实失败事件判断 learner 断线。

当前源码只有加入 learner：`add_learner(...)`。

当前源码只有 boot 节点提升 voter：`change_membership(AddVoterIds(...))`。

当前源码通过 `ChangeMembers::RemoveNodes(...)` 移除断线 learner，不自动移除 voter。

## 生产级别结论

`iron-core-cluster` 目前已经能支撑本机和小规模 IronMesh 集群验证，架构方向正确，TCP 长连接和 Raft 基础路径已经跑通。但它还没有达到生产级别。

主要原因不是缺少空泛能力，而是现有功能路径里存在快照地址恢复错误、leader 切换写入失败处理不足、TCP 空闲连接占坑、启动加入流程可能无边界等待、自动化行为测试为 0 等具体问题。

这个库现在适合作为 IronMesh 集群能力的验证基础继续推进，但如果要进入生产级别使用，应该优先修正上表中“严重”和“中高风险”的问题。
