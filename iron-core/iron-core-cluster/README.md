## IronClusterError

| 类型 | 成员 | 作用 |
|---|---|---|
| `IronClusterError` | `Io` | 文件或网络 IO 错误。 |
| `IronClusterError` | `EnvVar` | 环境变量读取错误。 |
| `IronClusterError` | `Toml` | TOML 配置解析错误。 |
| `IronClusterError` | `SerdeJson` | JSON 编解码错误。 |
| `IronClusterError` | `AddrParse` | 网络监听地址解析错误。 |
| `IronClusterError` | `RaftConfig` | OpenRaft 配置错误。 |
| `IronClusterError` | `RaftFatal` | OpenRaft 致命错误。 |
| `IronClusterError` | `RaftInitialize` | OpenRaft 初始化错误。 |
| `IronClusterError` | `RaftWrite` | OpenRaft 写入错误。 |
| `IronClusterError` | `RaftRead` | OpenRaft 线性读错误。 |
| `IronClusterError` | `SeedConfigNotFound` | 种子配置文件未找到。 |
| `IronClusterError` | `RuntimeDirNotFound` | 运行目录无法从构建输出目录推导。 |
| `IronClusterError` | `InvalidFrameKind` | TCP 帧类型无法识别。 |
| `IronClusterError` | `Protocol` | TCP 协议消息不符合预期。 |
| `IronClusterError` | `InvalidNumberEnv` | 数字环境变量无法解析。 |

## IronClusterNodeRole

| 类型 | 成员 | 作用 |
|---|---|---|
| `IronClusterNodeRole` | `Gateway` | 网关节点。 |
| `IronClusterNodeRole` | `Business` | 业务节点。 |
| `IronClusterNodeRole` | `Control` | 控制节点。 |

## IronClusterState

| 类型 | 成员 | 作用 |
|---|---|---|
| `IronClusterState` | `Unknown` | 状态未知。 |
| `IronClusterState` | `Starting` | 启动中。 |
| `IronClusterState` | `Healthy` | 健康。 |
| `IronClusterState` | `Offline` | 离线。 |

## IronClusterEndpointProtocol

| 类型 | 成员 | 作用 |
|---|---|---|
| `IronClusterEndpointProtocol` | `Tcp` | TCP 协议。 |
| `IronClusterEndpointProtocol` | `Http` | HTTP 协议。 |

## IronClusterServiceKind

| 类型 | 成员 | 作用 |
|---|---|---|
| `IronClusterServiceKind` | `Gateway` | 网关服务。 |
| `IronClusterServiceKind` | `Auth` | 登录注册服务。 |
| `IronClusterServiceKind` | `Ddz` | 斗地主服务。 |
| `IronClusterServiceKind` | `Pdk` | 跑得快服务。 |

## IronClusterSeedConfig

| 类型 | 字段 | 作用 |
|---|---|---|
| `IronClusterSeedConfig` | `registry_nodes` | TOML 中的注册中心种子节点列表。 |
| `IronClusterSeedConfig` | `debug_http` | 注册中心验证 HTTP 配置。 |

## IronClusterRegistryNodeConfig

| 类型 | 字段 | 作用 |
|---|---|---|
| `IronClusterRegistryNodeConfig` | `raft_node_id` | 注册中心 Raft 节点 ID。 |
| `IronClusterRegistryNodeConfig` | `tcp_addr` | 注册中心 TCP 监听地址。 |

## IronClusterDebugHttpConfig

| 类型 | 字段 | 作用 |
|---|---|---|
| `IronClusterDebugHttpConfig` | `http_addr` | 验证查询 HTTP 监听地址。 |

## IronClusterRegistryConfig

| 类型 | 字段 | 作用 |
|---|---|---|
| `IronClusterRegistryConfig` | `cluster_id` | 集群 ID。 |
| `IronClusterRegistryConfig` | `cluster_token` | 集群内部共享密钥。 |
| `IronClusterRegistryConfig` | `registry_nodes` | 注册中心 Raft 节点列表。 |
| `IronClusterRegistryConfig` | `debug_http_addr` | 验证查询 HTTP 监听地址。 |

## IronClusterWorkerConfig

| 类型 | 字段 | 作用 |
|---|---|---|
| `IronClusterWorkerConfig` | `cluster_id` | 集群 ID。 |
| `IronClusterWorkerConfig` | `cluster_token` | 集群内部共享密钥。 |
| `IronClusterWorkerConfig` | `node_id` | 当前工作节点 ID。 |
| `IronClusterWorkerConfig` | `node_role` | 当前工作节点角色。 |
| `IronClusterWorkerConfig` | `service_name` | 当前服务名称。 |
| `IronClusterWorkerConfig` | `registry_nodes` | 注册中心种子节点列表。 |

## IronClusterRegistryRuntimeNode

| 类型 | 字段 | 作用 |
|---|---|---|
| `IronClusterRegistryRuntimeNode` | `raft_node_id` | 当前 Raft 节点 ID。 |
| `IronClusterRegistryRuntimeNode` | `tcp_addr` | 当前 TCP 监听地址。 |
| `IronClusterRegistryRuntimeNode` | `raft` | 当前节点 Raft 句柄。 |
| `IronClusterRegistryRuntimeNode` | `store` | 当前节点 Raft 存储。 |

## IronRegistryDebugHttpState

| 类型 | 字段 | 作用 |
|---|---|---|
| `IronRegistryDebugHttpState` | `nodes` | 注册中心运行节点列表。 |

## IronClusterEndpointRecord

| 类型 | 字段 | 作用 |
|---|---|---|
| `IronClusterEndpointRecord` | `name` | 连接名称。 |
| `IronClusterEndpointRecord` | `protocol` | 连接协议。 |
| `IronClusterEndpointRecord` | `host` | 连接地址。 |
| `IronClusterEndpointRecord` | `port` | 连接端口。 |

## IronClusterServiceRecord

| 类型 | 字段 | 作用 |
|---|---|---|
| `IronClusterServiceRecord` | `node_id` | 服务所在节点 ID。 |
| `IronClusterServiceRecord` | `service_name` | 服务名称。 |
| `IronClusterServiceRecord` | `state` | 服务状态。 |
| `IronClusterServiceRecord` | `endpoints` | 服务连接端点。 |

## IronClusterRegistry

| 类型 | 字段 | 作用 |
|---|---|---|
| `IronClusterRegistry` | `metadata_version` | 注册表元数据版本。 |
| `IronClusterRegistry` | `services` | 当前服务注册记录。 |

## IronClusterCommand

| 类型 | 成员 | 作用 |
|---|---|---|
| `IronClusterCommand` | `RegisterService` | 注册或更新服务。 |
| `IronClusterCommand` | `UnregisterService.node_id` | 下线服务所在节点 ID。 |
| `IronClusterCommand` | `UnregisterService.service_name` | 下线服务名称。 |

## IronClusterCommandResult

| 类型 | 字段 | 作用 |
|---|---|---|
| `IronClusterCommandResult` | `metadata_version` | 注册表元数据版本。 |

## IronClusterFrameKind

| 类型 | 成员 | 作用 |
|---|---|---|
| `IronClusterFrameKind` | `RegisterService` | 工作节点注册服务。 |
| `IronClusterFrameKind` | `Heartbeat` | 工作节点心跳。 |
| `IronClusterFrameKind` | `UnregisterService` | 工作节点下线服务。 |
| `IronClusterFrameKind` | `RaftAppend` | Raft 日志复制请求。 |
| `IronClusterFrameKind` | `RaftVote` | Raft 投票请求。 |
| `IronClusterFrameKind` | `Error` | 协议错误响应。 |

## IronClusterFrameHeader

| 类型 | 字段 | 作用 |
|---|---|---|
| `IronClusterFrameHeader` | `kind` | TCP 帧类型。 |
| `IronClusterFrameHeader` | `body_len` | TCP 帧 body 字节长度。 |

## IronRaftTypeConfig

| 类型 | 字段 | 作用 |
|---|---|---|
| `IronRaftTypeConfig` | `D` | Raft 写命令类型。 |
| `IronRaftTypeConfig` | `R` | Raft 写命令返回类型。 |
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
| `IronRaftStoreInner` | `registry` | 状态机中的服务注册表。 |
| `IronRaftStoreInner` | `snapshot` | 当前状态机快照。 |

## IronRaftSnapshotData

| 类型 | 字段 | 作用 |
|---|---|---|
| `IronRaftSnapshotData` | `last_applied_log_id` | 快照包含的最后应用日志 ID。 |
| `IronRaftSnapshotData` | `last_membership` | 快照包含的最后成员配置。 |
| `IronRaftSnapshotData` | `registry` | 快照包含的服务注册表。 |

## IronRaftNetworkFactory

| 类型 | 字段 | 作用 |
|---|---|---|
| `IronRaftNetworkFactory` | `cluster_token` | 集群内部共享密钥。 |

## IronRaftNetwork

| 类型 | 字段 | 作用 |
|---|---|---|
| `IronRaftNetwork` | `target` | 目标 Raft 节点 ID。 |
| `IronRaftNetwork` | `target_node` | 目标 Raft 节点网络信息。 |
| `IronRaftNetwork` | `cluster_token` | 集群内部共享密钥。 |

## API

| API | 参数 | 返回值 | 作用 |
|---|---|---|---|
| `run_registry_cluster_from_local_toml` | 无 | `Result<(), IronClusterError>` | 启动注册中心，并从可执行文件旁边读取集群种子 TOML。 |
| `run_worker_from_local_toml` | `service_kind: IronClusterServiceKind` | `Result<(), IronClusterError>` | 启动工作节点，并从可执行文件旁边读取集群种子 TOML。 |
| `copy_cluster_seed_config_to_runtime_dir` | 无 | `Result<(), IronClusterError>` | 复制集群种子配置到服务运行目录。 |
| `GET /iron/cluster/health` | 无 | `ok` | 注册中心验证 HTTP 健康检查。 |
| `GET /iron/cluster/services` | 无 | `IronClusterRegistry` | 查询注册中心当前可见服务注册表。 |
