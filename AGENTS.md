# IronMesh 协作指南

## 项目定位

IronMesh 是一个使用 Rust 构建的高性能分布式业务系统框架。

系统初期包含两个主要角色：

- 网关：接收外部请求，并将请求路由或转发到业务系统。
- 业务系统：承载具体业务能力。

网络通信、协议设计、运行时基础设施应该演进为可复用的框架能力，不应该和某一个具体业务实现强绑定。

## 顶层目录职责

- `iron-gateway/`：网关系统。
- `iron-business/`：业务系统父目录。
- `iron-business/iron-service-auth/`：登录注册服务。进入实现阶段后，它应该是一个独立二进制 crate。
- `iron-business/iron-service-ddz/`：斗地主服务。进入实现阶段后，它应该是一个独立二进制 crate。
- `iron-business/iron-service-pdk/`：跑得快服务。进入实现阶段后，它应该是一个独立二进制 crate。
- `iron-business/iron-service-registry/`：集群注册发现与选举种子服务。它只作为启动壳，具体逻辑必须放在 `iron-core/iron-core-cluster/`。
- `iron-core/`：核心基础设施父目录，不绑定任何具体业务系统。
- `iron-core/iron-core-business/`：业务通信核心模型和业务协议导出。
- `iron-core/iron-core-cluster/`：集群通信核心、TCP 服务注册发现、Raft 选举、验证查询接口和集群协议导出。
- `iron-protocol/`：协议设计、DSL、FlatBuffers schema、数据模型。
- `iron-protocol/iron-flat-dsl/cluster/`：集群之间的基础协议。
- `iron-protocol/iron-flat-dsl/business/`：集群业务协议。
- `iron-protocol/iron-scheme-libs/`：FlatBuffers 生成代码 crate 集合。
- `iron-protocol/iron-scheme-libs/build-support/`：FlatBuffers 生成脚本公共辅助代码。
- `iron-protocol/tools/`：协议专属工具目录，例如固定版本的 `flatc`。
- `iron-common/`：没有业务语义、没有网络语义的公共基础能力。

## 边界规则

- 具体业务逻辑只放在 `iron-business/*` 下。
- 网络连接、连接池、路由抽象、运行时辅助、编解码框架代码放在 `iron-core/*` 下。
- 集群内部服务注册、服务发现、健康感知和 Raft 通信必须优先使用 TCP；HTTP 只能作为人工验证查询入口。
- 协议结构、schema、数据模型、协议 DSL 定义放在 `iron-protocol/*` 下。
- 错误、配置辅助、日志初始化、时间工具、ID 工具、通用工具放在 `iron-common/*` 下。
- 不要把业务逻辑放进 `iron-core` 或 `iron-common`。
- 不要把具体协议 schema 放进 `iron-core`。
- 不要让 `iron-common` 依赖业务、网关、核心网络或具体协议概念。

## 拆分原则

- 优先使用父目录作为组织边界。
- 当某个能力的职责和依赖方向清晰后，再拆成独立 crate。
- crate 之间优先使用明确依赖，避免隐藏的跨目录耦合。
- crate 名称应该和目录名称保持一致。
- 只有当目录已经是有效 crate，并拥有自己的 `Cargo.toml` 时，才加入 workspace members。
- 框架代码应该能被网关和业务系统复用。
- 协议需要随着系统增长进行版本化和分类管理。

## Cargo Workspace 规范

- 根目录 `Cargo.toml` 统一管理 workspace 成员、公共 package 信息和公共依赖。
- 子 crate 的 `version` 和 `edition` 默认使用 `version.workspace = true`、`edition.workspace = true`。
- 第三方依赖版本必须优先声明在根目录 `[workspace.dependencies]` 中，子 crate 使用 `依赖名.workspace = true`。
- workspace 内部 crate 的路径必须优先声明在根目录 `[workspace.dependencies]` 中，子 crate 使用 `crate-name.workspace = true`。
- `[workspace.dependencies]` 必须按类别分组，并使用中文注释标明类别，例如第三方依赖、workspace 内部 crate。
- 只有当某个依赖确实需要在单个 crate 中使用不同版本或不同 feature 时，才允许在子 crate 单独声明，并在旁边写中文注释说明原因。

## 代码规格与注释规范

- 所有非自动生成代码和协议定义中的注释必须使用中文。
- Rust 代码统一使用 `//` 注释，不使用 `///` 或 `//!`。
- 自动生成代码不受本规范约束，例如 `src/scheme` 下由 FlatBuffers 生成的代码。
- 非自动生成的 Rust 代码中，每个数据结构上方、每个字段后方、每个方法上方都必须有中文注释。
- `iron-protocol/iron-flat-dsl` 下所有协议 schema 中，所有协议数据模型定义上方、所有字段后方、所有枚举值后方都必须有中文注释。
- 协议数据模型包括但不限于：`namespace`、`enum`、`table`、`struct`、`union`、`rpc_service`、`root_type`。
- 涉及数据结构或方法变更时，Codex/AI 必须先用表格说明变更了数据结构和方法，用户确认后再编码。
