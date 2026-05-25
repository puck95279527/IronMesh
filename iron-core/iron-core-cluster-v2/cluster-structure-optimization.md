# iron-core-cluster-v2 结构优化备忘

本文档记录 `iron-core-cluster-v2` 当前推荐的代码组织方向：阶段清晰、方法稳定、参数少、日志清楚，不把启动逻辑继续切得过碎。

## 总原则

| 原则 | 说明 |
|---|---|
| 入口少 | `IronRaftClusterManager::start()` 是库模式主入口，`run()` 只作为阻塞包装。 |
| 库不阻塞 | `start()` 在当前节点完成起盘或加入集群后返回运行句柄。 |
| 服务壳阻塞 | `src/bin/*` 通过 `IronRaftClusterHandle::wait_forever()` 显式常驻运行。 |
| 阶段稳定 | 启动过程固定为 5 个阶段，常驻等待不再属于库启动阶段。 |
| 方法不碎 | 阶段方法保持线性叙事，避免为了“更细”而拆出大量小方法。 |
| 参数少 | 能通过 `IronRaftClusterManager` 上下文取得的内容，不在方法之间反复传递。 |
| 日志做路标 | 关键阶段必须能通过日志读出集群启动链路。 |
| 常量集中 | Raft、启动流程、TCP、join、维护任务常量集中管理，避免散落在各文件里。 |
| 语义单一 | `is_boot_node = true` 表示唯一首次起盘节点，`IronRaftNodeRole::Boot` 暂时表示注册投票节点。 |

## 当前启动阶段

| 顺序 | 方法 | 作用 |
|---|---|---|
| 1 | `validate_topology` | 校验当前节点、注册节点表和唯一首次起盘节点。 |
| 2 | `build_raft_runtime` | 创建 Raft 实例、TCP 服务对象和本节点运行基础对象。 |
| 3 | `spawn_runtime_services` | 启动 Raft TCP、调试 HTTP、leader 维护后台任务，并注册进 `JoinSet`。 |
| 4 | `bootstrap_or_join_cluster` | 先尝试加入已有集群；只有唯一起盘节点允许初始化新集群。 |
| 5 | `join_remaining_boot_nodes` | 起盘成功后，把其他注册节点逐个加入为 voter。 |

## 当前文件职责

| 文件 | 职责 |
|---|---|
| `iron_raft_cluster_manager.rs` | 管理器入口，提供 `start()` 和兼容阻塞包装 `run()`。 |
| `iron_raft_cluster_handle.rs` | 集群运行句柄，持有 Raft 句柄和后台任务集合。 |
| `iron_raft_cluster_manager_flow.rs` | 启动阶段流程，方法按阶段顺序排列。 |
| `iron_raft_cluster_manager_support.rs` | 配置加载、Raft 配置、后台任务、加入节点、维护任务等辅助动作。 |
| `iron_raft_constants.rs` | Raft、启动流程、TCP、join、维护任务等可调常量。 |
| `iron_raft_node.rs` | Raft 节点数据结构与节点身份判断。 |
| `iron_raft_tcp_frame.rs` | TCP frame 编解码、最大长度限制、读写超时保护。 |
| `iron_raft_tcp_server.rs` | Raft TCP 服务端、连接数限制和 join 请求处理。 |
| `iron_raft_tcp_client.rs` | Raft TCP 客户端、OpenRaft 网络接口和 join 请求发送。 |

## 后续结构判断

| 判断 | 建议 |
|---|---|
| `Flow` 是否继续拆文件 | 暂不拆。当前 5 个启动阶段清晰，继续拆会增加跳转成本。 |
| `Support` 是否继续拆文件 | 暂不拆。除非维护任务明显膨胀，再考虑按职责拆分。 |
| 是否继续集中常量 | 保持集中。先集中为常量，后续需要多环境运行时再配置化。 |
| 是否重命名 `boot_nodes` | 暂不改。它当前表示注册投票节点表，后续大重构时再统一命名。 |
| 是否删除 `IronRaftNodeRole::Boot` | 暂不删。当前仍用于区分注册投票节点和普通 learner 节点。 |
| 是否新增复杂 shutdown | 暂不新增。当前只通过句柄托管任务，不做优雅停止协议。 |

## 需要记住的判断

- 不是方法越多越好。
- 不是方法越少越好。
- 最合适的是：方法数量适中、阶段明确、参数少、日志清楚、文件职责稳定。
- 固定 Boot 起盘后，不再引入冷静期、分布式锁或 `4999` 本机锁。
- 起盘后的一致性由 Raft 负责，`is_boot_node` 只影响首次初始化。
- 库负责启动并返回运行句柄，服务进程负责决定是否阻塞。
- P0/P1 保护已经集中在 frame/server/handle/support 层，后续调参优先改 `iron_raft_constants.rs`。
