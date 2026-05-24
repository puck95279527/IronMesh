## RaftServiceRole

| 类型 | 成员 | 作用 |
|---|---|---|
| `RaftServiceRole` | `Leader` | Raft Leader，负责接收写请求、追加日志，并把日志复制给其他 Raft 节点。 |
| `RaftServiceRole` | `Follower` | Raft Follower，有投票权，接收 Leader 的日志复制，并在选举时参与投票。 |
| `RaftServiceRole` | `Candidate` | Raft Candidate，有投票权，表示节点正在发起选举并向其他节点请求投票。 |
| `RaftServiceRole` | `Learner` | Raft Learner，没有投票权，只接收日志复制，通常用于新节点追赶数据或扩容过渡。 |

## BizServiceKind

| 类型 | 成员 | 作用 |
|---|---|---|
| `BizServiceKind` | `Registry` | 注册表业务服务。 |
| `BizServiceKind` | `Gate` | 网关业务服务。 |
| `BizServiceKind` | `Auth` | 登录注册业务服务。 |
| `BizServiceKind` | `GamePdk` | 跑得快业务服务。 |
| `BizServiceKind` | `GameDdz` | 斗地主业务服务。 |

## BizService

| 类型 | 字段 | 作用 |
|---|---|---|
| `BizService` | `name` | 业务端点名称，例如 `ctrl`、`data`、`http`、`ws`、`admin`。 |
| `BizService` | `addr` | 业务端点地址，例如 `10.0.0.8:8888`。 |

## IronClusterService

| 类型 | 字段 | 作用 |
|---|---|---|
| `IronClusterService` | `raft_id` | Raft 服务实例 ID，只有 registry Raft 节点有值。 |
| `IronClusterService` | `raft_role` | Raft 当前角色，worker 服务为 `None`。 |
| `IronClusterService` | `raft_addr` | Raft 通信地址，worker 服务为 `None`。 |
| `IronClusterService` | `raft_epoch` | Raft 实例启动代次，worker 服务为 `None`。 |
| `IronClusterService` | `raft_alive_at_ms` | Raft 最近心跳时间，worker 服务为 `None`。 |
| `IronClusterService` | `biz_kind` | 业务服务类型。 |
| `IronClusterService` | `biz_service_id` | 业务服务实例 ID，例如 `game_pdk-1001`。 |
| `IronClusterService` | `biz_services` | 当前实例暴露的业务端点列表。 |

## ClusterCommand

| 类型 | 成员 | 作用 |
|---|---|---|
| `ClusterCommand` | `Upsert(IronClusterService)` | 注册或更新服务。 |
| `ClusterCommand` | `Offline.biz_service_id` | 标记下线的业务服务实例 ID。 |

## ClusterFrameKind

| 类型 | 成员 | 作用 |
|---|---|---|
| `ClusterFrameKind` | `RegisterService` | 工作节点注册服务。 |
| `ClusterFrameKind` | `Heartbeat` | 工作节点心跳。 |
| `ClusterFrameKind` | `RaftAppend` | Raft 日志复制请求。 |
| `ClusterFrameKind` | `RaftVote` | Raft 投票请求。 |
| `ClusterFrameKind` | `Error` | 协议错误响应。 |

## ClusterFrameHeader

| 类型 | 字段 | 作用 |
|---|---|---|
| `ClusterFrameHeader` | `kind` | TCP 帧类型。 |
| `ClusterFrameHeader` | `body_len` | TCP 帧 body 字节长度。 |

## ClusterSeedConfig

| 类型 | 字段 | 作用 |
|---|---|---|
| `ClusterSeedConfig` | `registry_nodes` | TOML 中的 registry 种子节点列表。 |
| `ClusterSeedConfig` | `debug_http` | registry 验证 HTTP 配置。 |

## ClusterRegistryNodeConfig

| 类型 | 字段 | 作用 |
|---|---|---|
| `ClusterRegistryNodeConfig` | `raft_node_id` | registry Raft 节点 ID。 |
| `ClusterRegistryNodeConfig` | `tcp_addr` | registry TCP 地址。 |

## ClusterDebugHttpConfig

| 类型 | 字段 | 作用 |
|---|---|---|
| `ClusterDebugHttpConfig` | `http_addr` | 验证查询 HTTP 地址。 |

## ClusterRegistryRuntimeNode

| 类型 | 字段 | 作用 |
|---|---|---|
| `ClusterRegistryRuntimeNode` | `raft_node_id` | 当前 Raft 节点 ID。 |
| `ClusterRegistryRuntimeNode` | `tcp_addr` | 当前 TCP 监听地址。 |
| `ClusterRegistryRuntimeNode` | `raft` | 当前节点 Raft 句柄。 |
| `ClusterRegistryRuntimeNode` | `store` | 当前节点 Raft 存储。 |

## ClusterError

| 类型 | 成员 | 作用 |
|---|---|---|
| `ClusterError` | `Io` | 文件或网络 IO 错误。 |
| `ClusterError` | `EnvVar` | 环境变量读取错误。 |
| `ClusterError` | `Toml` | TOML 配置解析错误。 |
| `ClusterError` | `SerdeJson` | JSON 编解码错误。 |
| `ClusterError` | `AddrParse` | 网络监听地址解析错误。 |
| `ClusterError` | `RaftConfig` | OpenRaft 配置错误。 |
| `ClusterError` | `RaftFatal` | OpenRaft 致命错误。 |
| `ClusterError` | `RaftInitialize` | OpenRaft 初始化错误。 |
| `ClusterError` | `RaftWrite` | OpenRaft 写入错误。 |
| `ClusterError` | `RaftRead` | OpenRaft 线性读错误。 |
| `ClusterError` | `SeedConfigNotFound` | 种子配置文件未找到。 |
| `ClusterError` | `RuntimeDirNotFound` | 运行目录无法从构建输出目录推导。 |
| `ClusterError` | `InvalidFrameKind` | TCP 帧类型无法识别。 |
| `ClusterError` | `Protocol` | TCP 协议消息不符合预期。 |

## IronRaftTypeConfig

| 类型 | 字段 | 作用 |
|---|---|---|
| `IronRaftTypeConfig` | `D` | Raft 写命令类型，当前为 `ClusterCommand`。 |
| `IronRaftTypeConfig` | `R` | Raft 写命令返回类型，当前为 `()`。 |
| `IronRaftTypeConfig` | `NodeId` | Raft 节点 ID 类型。 |
| `IronRaftTypeConfig` | `Node` | Raft 节点网络信息类型。 |
| `IronRaftTypeConfig` | `Entry` | Raft 日志条目类型。 |
| `IronRaftTypeConfig` | `SnapshotData` | Raft 快照数据类型。 |

## IronRaftStore

| 类型 | 字段 | 作用 |
|---|---|---|
| `IronRaftStore` | `inner` | Raft 日志和状态机共享数据。 |

## IronRaftStoreInner

| 类型 | 字段 | 作用 |
|---|---|---|
| `IronRaftStoreInner` | `vote` | 当前节点保存的投票状态。 |
| `IronRaftStoreInner` | `committed` | 当前节点保存的提交位置。 |
| `IronRaftStoreInner` | `logs` | 当前节点内存 Raft 日志。 |
| `IronRaftStoreInner` | `last_purged_log_id` | 已清理的最后日志 ID。 |
| `IronRaftStoreInner` | `last_applied_log_id` | 状态机已应用的最后日志 ID。 |
| `IronRaftStoreInner` | `last_membership` | 状态机已应用的最后成员配置。 |
| `IronRaftStoreInner` | `registry` | 状态机中的 `BTreeMap<String, IronClusterService>`。 |
| `IronRaftStoreInner` | `snapshot` | 当前状态机快照。 |

## IronClusterRaftState

| 类型 | 字段 | 作用 |
|---|---|---|
| `IronClusterRaftState` | `last_applied_log_id` | 状态机已应用的最后日志 ID。 |
| `IronClusterRaftState` | `last_membership` | 状态机当前成员配置。 |
| `IronClusterRaftState` | `registry` | 状态机当前服务注册表。 |

## IronRaftNetworkFactory

| 类型 | 字段 | 作用 |
|---|---|---|
| `IronRaftNetworkFactory` | 无字段 | 创建指向目标 Raft 节点的网络客户端。 |

## IronRaftNetwork

| 类型 | 字段 | 作用 |
|---|---|---|
| `IronRaftNetwork` | `target_node` | 目标 Raft 节点网络信息。 |

## API

| API | 参数 | 返回值 | 作用 |
|---|---|---|---|
| `run_registry_cluster_from_local_toml` | 无 | `Result<(), ClusterError>` | 启动注册中心，并从可执行文件旁边读取集群种子 TOML。 |
| `run_worker_from_local_toml` | `biz_kind: BizServiceKind` | `Result<(), ClusterError>` | 启动工作节点，并从可执行文件旁边读取集群种子 TOML。 |
| `copy_cluster_seed_config_to_runtime_dir` | 无 | `Result<(), ClusterError>` | 复制集群种子配置到服务运行目录。 |
| `GET /iron/cluster/health` | 无 | `ok` | 注册中心验证 HTTP 健康检查。 |
| `GET /iron/cluster/services` | 无 | `BTreeMap<String, IronClusterService>` | 查询注册中心当前可见服务状态表。 |
