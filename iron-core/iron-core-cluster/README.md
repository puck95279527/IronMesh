# iron-core-cluster

`iron-core-cluster` 是 IronMesh 的集群核心模块，负责最小化集群控制面能力。

当前模块提供 TCP 集群通信、Raft 选举与 membership、节点加入、learner 断线恢复和 Raft metrics 查询。它面向 IronMesh 内部的网关、注册节点和业务节点复用，不绑定任何具体业务系统。

当前 Raft 存储是内存版实现，只用于保存最小集群控制面状态，不保存业务数据，也不作为通用数据库使用。

## 源码结构

| 目录 | 职责 |
|---|---|
| `src/api/` | 对外启动入口，包含 `IronController` 和 `IronHandler`。 |
| `src/control_plane/` | 节点配置、启动流程、运行时、voter 和 learner 管理。 |
| `src/raft/network/` | Raft TCP 客户端、服务端、连接缓存和帧协议。 |
| `src/raft/storage/` | 内存版 Raft log store 和 state machine。 |
| `src/query/` | HTTP metrics 查询入口。 |
| `src/utils/` | 雪花 ID 等内部工具。 |

## 集群机制与原理

集群节点分为 voter 和 learner。

voter 来自 `cluster-boot.toml` 中的固定配置。voter 参与 Raft 投票和 leader 选举，用于形成最小一致性控制面。

learner 是动态加入节点，不参与投票。网关节点、业务节点等扩展角色可以作为 learner 加入集群，获得集群 membership 和控制面同步能力。

learner 启动时使用雪花 ID 生成新的 `node_id`。当 learner 的本地 Raft TCP 复制连接断开时，当前 learner runtime 会退出，随后在库内部重新创建 learner，以新的 `node_id` 再次加入集群。

leader 负责接收 `JoinCluster` 请求。leader 会先校验 learner 广播的 TCP 地址是否可达，再把节点写入 Raft membership。

当 leader 发现 learner 的 Raft 复制连接失败时，会尝试把旧 learner 从 membership 中移除，并清理对应的 Raft TCP 共享连接缓存，避免旧节点残留。

集群内部通信优先使用 TCP 长连接。HTTP 只作为人工验证和查询入口，例如查询 Raft metrics，不作为集群内部主通信通道。

## 当前阶段与边界

当前已经实现：

- 最小 voter/learner 集群。
- TCP Raft 通信。
- learner 动态加入。
- learner 断线后以新 `node_id` 重建并重新加入。
- leader 移除旧 learner。
- Raft TCP 连接缓存复用和旧缓存清理。
- Raft metrics HTTP 查询。

当前没有包含：

- 业务数据持久化。
- 完整服务注册模型。
- 认证鉴权。
- 跨机部署说明。
- 完整生产运维手册。

这些边界是当前阶段的设计取舍。`iron-core-cluster` 当前只记录和同步最小集群控制面状态，业务数据和具体业务逻辑不进入本模块。
