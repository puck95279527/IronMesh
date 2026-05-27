# 状态机泛型化演进计划

本文档记录 `iron-core-cluster` 状态机从固定 `IronClusterState` 逐步演进到全链路泛型化的计划。

当前阶段的目标不是一次性重构全部控制面、网络协议和对外 API，而是先把状态机存储层从具体数据结构中解耦出来，降低后续数据面频繁变化对 Raft 基础设施的影响。

## 背景

当前状态机链路如下：

| 层级 | 当前类型 | 职责 |
|---|---|---|
| Raft 存储层 | `IronRaftStateMachineStore` | 保存 Raft 已 apply 的状态机数据、membership、snapshot。 |
| 状态数据 | `IronClusterState` | 表示当前集群状态机数据。 |
| 状态字段 | `BTreeMap<String, String>` | 保存当前验证用的 key/value。 |

这个结构在验证阶段可用，但 `IronClusterState` 后续很可能频繁变化，例如加入服务注册、能力标签、节点负载、路由拓扑、租约状态等。若 Raft 存储层长期绑定 `IronClusterState`，后续每次数据面演进都会牵动核心存储代码。

## 总体方向

最终希望形成如下边界：

| 边界 | 目标 |
|---|---|
| Raft 基础设施 | 负责启动、选主、复制、snapshot、membership、TCP 通信。 |
| 状态机范式 | 规定状态数据如何应用命令、如何序列化快照。 |
| 默认状态实现 | 提供当前最小集群状态实现，保证现有验证流程不破坏。 |
| 业务状态实现 | 后续可由具体控制面数据模型实现状态机范式。 |

演进原则：

| 原则 | 说明 |
|---|---|
| 小步推进 | 每一阶段只改变一个明确边界。 |
| 保持可编译 | 每一步完成后都运行 `cargo check -p iron-core-cluster`。 |
| 保持外部 API 稳定 | 第一阶段不让 `IronClusterManager` 和 `IronClusterHandler` 对外暴露泛型。 |
| 不提前泛化协议 | 第一阶段 TCP RPC、Raft request、response 继续使用当前具体类型。 |
| 为后续留出口 | 第一阶段的 trait 设计不能阻塞后续命令和响应泛型化。 |

## 阶段一：状态机存储层泛型化

本阶段只让 `IronRaftStateMachineStore` 支持泛型状态数据，其他链路继续使用默认类型。

| 项目 | 当前 | 目标 |
|---|---|---|
| 状态机存储 | `IronRaftStateMachineStore` 固定保存 `IronClusterState` | 改为 `IronRaftStateMachineStore<S = IronClusterState>`。 |
| 状态机数据 | 存储层直接依赖 `IronClusterState` | 存储层依赖 trait 约束。 |
| 命令类型 | `IronRaftRequest` | 暂时保持不变。 |
| 响应类型 | `IronClusterWriteResponse` | 暂时保持不变。 |
| Manager/Handler | 非泛型 | 暂时保持不变。 |
| TCP RPC | 固定请求响应枚举 | 暂时保持不变。 |

建议新增状态机范式 trait：

```rust
pub trait IronRaftStateMachineData:
    Clone + Default + serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static
{
    fn apply_raft_request(
        &mut self,
        request: IronRaftRequest,
    ) -> IronClusterWriteResponse;
}
```

阶段一完成后的预期结构：

| 类型 | 变化 |
|---|---|
| `IronClusterState` | 实现 `IronRaftStateMachineData`。 |
| `IronRaftStateMachineStore<S>` | 使用 `Arc<Mutex<S>>` 保存状态机数据。 |
| `IronClusterStateReader` | 可以继续使用默认 `IronRaftStateMachineStore`，或同步改成内部泛型但对外默认。 |
| `IronRaftTypeConfig` | 暂不变，仍使用 `IronRaftRequest` 和 `IronClusterWriteResponse`。 |

验收标准：

| 验收项 | 说明 |
|---|---|
| 编译通过 | `cargo check -p iron-core-cluster` 通过。 |
| API 不变 | 现有 `IronClusterManager::add_voter/add_learner/start` 不改变调用方式。 |
| 写入行为不变 | `IronClusterDataCommand::Set` 仍能写入默认状态机。 |
| snapshot 行为不变 | 默认状态机仍能序列化、安装和读取快照。 |

## 阶段二：扩展默认状态数据结构

状态机存储层泛型化稳定后，默认 `IronClusterState` 已经直接持有当前验证数据。后续可按服务注册、节点负载、路由拓扑等明确子域继续扩展。

推荐方向：

| 当前 | 后续方向 |
|---|---|
| `IronClusterState { values: BTreeMap<String, String> }` | 保留最小验证数据，按需要增加更明确的子域结构。 |

注意事项：

| 风险 | 说明 |
|---|---|
| JSON 形状变化 | 后续新增子域时，`local_state_machine_data()` 和 snapshot JSON 会随状态结构变化。 |
| 未来兼容 | 当前无持久化，兼容成本较低；以后做持久化前应明确 snapshot 版本。 |

## 阶段三：状态读取链路泛型化

当默认状态模型稳定后，把读取链路也逐步泛型化。

| 项目 | 方向 |
|---|---|
| `IronClusterStateReader` | 改为 `IronClusterStateReader<S>`。 |
| `local_state_machine_data()` | 内部支持返回 `S`，外部暂时仍通过默认类型暴露。 |
| 对外 handler | 暂时保留默认 `IronClusterState` 返回值。 |

这一阶段仍不要求 `IronClusterHandler` 对外泛型化，避免影响调用方。

## 阶段四：命令和响应范式化

当状态模型不再只是当前最小 KV 后，再把命令和响应从固定类型演进为 trait 关联类型。

可能的方向：

```rust
pub trait IronRaftStateMachineData:
    Clone + Default + serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static
{
    type Command;
    type Response;

    fn apply_command(&mut self, command: Self::Command) -> Self::Response;
}
```

该阶段会影响：

| 类型 | 影响 |
|---|---|
| `IronRaftRequest` | 需要拆分默认命令和泛型命令。 |
| `IronClusterWriteResponse` | 需要评估是否成为默认响应。 |
| `IronRaftTypeConfig` | 需要引入泛型或类型别名。 |
| `IronRaftTcpRpcRequest/Response` | 需要支持泛型命令和响应，或拆出协议层包装。 |

## 阶段五：全链路泛型化

最终阶段把 Manager、Handler、Raft type config、TCP RPC、Request/Response 全链路泛型化。

| 项目 | 目标 |
|---|---|
| `IronClusterManager` | 支持默认类型，也允许指定自定义状态机类型。 |
| `IronClusterHandler` | 支持读取自定义状态机快照和写入自定义命令。 |
| `IronRaftTypeConfig` | 根据状态机范式绑定命令、响应和 snapshot 数据。 |
| TCP RPC | 能传输自定义命令和响应，或以稳定 envelope 包装。 |
| 默认实现 | 继续提供开箱即用的默认状态机，保证 lab 验证简单。 |

这一阶段牵动较大，应在阶段一到阶段四稳定后再执行。

## 当前优先级

当前只推进阶段一。

执行阶段一前，需要先确认以下变更表：

| 类型或方法 | 计划变更 |
|---|---|
| 新增 trait | 新增状态机范式 trait，用于约束泛型状态数据。 |
| `IronClusterState` | 实现状态机范式 trait。 |
| `IronRaftStateMachineStore` | 改为泛型结构，默认类型为 `IronClusterState`。 |
| `IronRaftStateMachineStore::state_machine` | 从 `Arc<Mutex<IronClusterState>>` 改为 `Arc<Mutex<S>>`。 |
| `apply` | 从直接匹配 `IronRaftRequest` 改为委托给状态机范式 trait。 |
| snapshot 序列化 | 从序列化固定 `IronClusterState` 改为序列化 `S`。 |
| snapshot 安装 | 从反序列化固定 `IronClusterState` 改为反序列化 `S`。 |

阶段一不改变：

| 类型或方法 | 保持不变 |
|---|---|
| `IronClusterManager` | 对外非泛型 API 保持不变。 |
| `IronClusterHandler` | 对外非泛型 API 保持不变。 |
| `IronRaftRequest` | 继续作为当前 Raft 写入请求。 |
| `IronClusterWriteResponse` | 继续作为当前写入响应。 |
| `IronRaftTcpRpcRequest` | TCP 请求枚举暂不泛型化。 |
| `IronRaftTcpRpcResponse` | TCP 响应枚举暂不泛型化。 |
