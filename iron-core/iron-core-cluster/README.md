## IronClusterError

| 类型 | 成员 | 作用 |
|---|---|---|
| `IronClusterError` | `Io` | 文件或网络监听错误。 |
| `IronClusterError` | `EnvVar` | 环境变量读取错误。 |
| `IronClusterError` | `Toml` | TOML 配置解析错误。 |
| `IronClusterError` | `Reqwest` | HTTP 客户端请求错误。 |
| `IronClusterError` | `AddrParse` | 网络监听地址解析错误。 |
| `IronClusterError` | `RaftConfig` | OpenRaft 配置错误。 |
| `IronClusterError` | `RaftFatal` | OpenRaft 致命错误。 |
| `IronClusterError` | `RaftInitialize` | OpenRaft 初始化错误。 |
| `IronClusterError` | `RaftWrite` | OpenRaft 写入错误。 |
| `IronClusterError` | `RaftRead` | OpenRaft 线性读错误。 |
| `IronClusterError` | `SeedConfigNotFound` | 种子配置文件未找到。 |
| `IronClusterError` | `RuntimeDirNotFound` | 运行目录无法从构建输出目录推导。 |
| `IronClusterError` | `InvalidPeerUrl` | 种子节点地址无法转换成监听地址。 |
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
| `IronClusterSeedConfig` | `peers` | TOML 中的 Raft 种子节点列表。 |

## IronClusterPeer

| 类型 | 字段 | 作用 |
|---|---|---|
| `IronClusterPeer` | `raft_node_id` | 对端 Raft 节点 ID。 |
| `IronClusterPeer` | `node_id` | 对端 IronMesh 节点 ID。 |
| `IronClusterPeer` | `http_url` | 对端控制面 HTTP 地址。 |

## IronClusterConfig

| 类型 | 字段 | 作用 |
|---|---|---|
| `IronClusterConfig` | `cluster_id` | 集群 ID。 |
| `IronClusterConfig` | `raft_node_id` | 当前 Raft 节点 ID。 |
| `IronClusterConfig` | `node_id` | 当前 IronMesh 节点 ID。 |
| `IronClusterConfig` | `node_role` | 当前节点角色。 |
| `IronClusterConfig` | `service_name` | 当前服务名称。 |
| `IronClusterConfig` | `http_addr` | 当前控制面监听地址。 |
| `IronClusterConfig` | `cluster_token` | 集群内部共享密钥。 |
| `IronClusterConfig` | `peers` | 从本地 TOML 读取的种子节点。 |

## IronClusterRuntime

| 类型 | 字段 | 作用 |
|---|---|---|
| `IronClusterRuntime` | `config` | 当前节点启动配置。 |
| `IronClusterRuntime` | `registry` | 当前节点本地注册表。 |
| `IronClusterRuntime` | `http_client` | 集群控制面 HTTP 客户端。 |

## IronClusterHttpState

| 类型 | 字段 | 作用 |
|---|---|---|
| `IronClusterHttpState` | `cluster_token` | 集群内部共享密钥。 |
| `IronClusterHttpState` | `registry` | 当前节点本地注册表。 |

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
| `IronRaftNetworkFactory` | `http_client` | Raft RPC HTTP 客户端。 |

## IronRaftNetwork

| 类型 | 字段 | 作用 |
|---|---|---|
| `IronRaftNetwork` | `target` | 目标 Raft 节点 ID。 |
| `IronRaftNetwork` | `target_node` | 目标 Raft 节点网络信息。 |
| `IronRaftNetwork` | `cluster_token` | 集群内部共享密钥。 |
| `IronRaftNetwork` | `http_client` | Raft RPC HTTP 客户端。 |

## API

| API | 参数 | 返回值 | 作用 |
|---|---|---|---|
| `run_cluster_service_from_local_toml` | `service_kind: IronClusterServiceKind` | `Result<(), IronClusterError>` | 启动指定服务，并从可执行文件旁边读取集群种子 TOML。 |
| `copy_cluster_seed_config_to_runtime_dir` | 无 | `Result<(), IronClusterError>` | 复制集群种子配置到服务运行目录。 |
| `GET /iron/cluster/health` | 无 | `ok` | 集群控制面健康检查。 |
| `GET /iron/cluster/services` | `x-iron-cluster-token` | `IronClusterRegistry` | 查询当前节点已应用的服务注册表。 |
| `POST /iron/cluster/register` | `x-iron-cluster-token`、`IronClusterServiceRecord` | `IronClusterRegistry` | 通过 Raft 写入服务注册记录。 |
| `POST /iron/cluster/raft/append` | `x-iron-cluster-token`、`AppendEntriesRequest<IronRaftTypeConfig>` | `AppendEntriesResponse<u64>` | 接收 OpenRaft 日志复制请求。 |
| `POST /iron/cluster/raft/vote` | `x-iron-cluster-token`、`VoteRequest<u64>` | `VoteResponse<u64>` | 接收 OpenRaft 投票请求。 |
