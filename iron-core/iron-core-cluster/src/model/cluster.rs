// 集群注册发现数据模型。

use super::IronClusterError;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use super::IronRaft;
use super::IronRaftStore;

// 集群节点角色。
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum IronClusterNodeRole {
    Gateway,  // 网关节点。
    Business, // 业务节点。
    Control,  // 控制节点。
}

// 集群对象状态。
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum IronClusterState {
    Unknown,  // 状态未知。
    Starting, // 启动中。
    Healthy,  // 健康。
    Offline,  // 离线。
}

// 集群端点协议类型。
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum IronClusterEndpointProtocol {
    Tcp,  // TCP 协议。
    Http, // HTTP 协议。
}

// IronMesh 服务类型。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IronClusterServiceKind {
    Gateway, // 网关服务。
    Auth,    // 登录注册服务。
    Ddz,     // 斗地主服务。
    Pdk,     // 跑得快服务。
}

impl IronClusterServiceKind {
    // 返回服务名称。
    pub(crate) fn service_name(self) -> &'static str {
        match self {
            Self::Gateway => "iron-gateway",
            Self::Auth => "iron-service-auth",
            Self::Ddz => "iron-service-ddz",
            Self::Pdk => "iron-service-pdk",
        }
    }

    // 返回默认节点标识。
    pub(crate) fn default_node_id(self) -> &'static str {
        match self {
            Self::Gateway => "iron-gateway-1",
            Self::Auth => "iron-service-auth-1",
            Self::Ddz => "iron-service-ddz-1",
            Self::Pdk => "iron-service-pdk-1",
        }
    }

    // 返回默认 Raft 节点 ID。
    pub(crate) fn default_raft_node_id(self) -> u64 {
        match self {
            Self::Gateway => 1,
            Self::Auth => 2,
            Self::Ddz => 3,
            Self::Pdk => 4,
        }
    }

    // 返回默认节点角色。
    pub(crate) fn node_role(self) -> IronClusterNodeRole {
        match self {
            Self::Gateway => IronClusterNodeRole::Gateway,
            Self::Auth | Self::Ddz | Self::Pdk => IronClusterNodeRole::Business,
        }
    }
}

// 集群种子节点配置。
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IronClusterSeedConfig {
    pub peers: Vec<IronClusterPeer>, // TOML 中的 Raft 种子节点列表。
}

impl IronClusterSeedConfig {
    // 从 TOML 文件读取集群种子节点配置。
    pub(crate) fn from_toml_file(path: impl AsRef<Path>) -> Result<Self, IronClusterError> {
        let text = fs::read_to_string(path)?;
        Ok(toml::from_str(&text)?)
    }
}

// 集群种子节点。
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IronClusterPeer {
    pub raft_node_id: u64, // 对端 Raft 节点 ID。
    pub node_id: String,   // 对端 IronMesh 节点 ID。
    pub http_url: String,  // 对端控制面 HTTP 地址。
}

// 集群启动配置。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IronClusterConfig {
    pub cluster_id: String,             // 集群 ID。
    pub raft_node_id: u64,              // 当前 Raft 节点 ID。
    pub node_id: String,                // 当前 IronMesh 节点 ID。
    pub node_role: IronClusterNodeRole, // 当前节点角色。
    pub service_name: String,           // 当前服务名称。
    pub http_addr: String,              // 当前控制面监听地址。
    pub cluster_token: String,          // 集群内部共享密钥。
    pub peers: Vec<IronClusterPeer>,    // 从本地 TOML 读取的种子节点。
}

// 集群运行时。
#[derive(Clone)]
pub(crate) struct IronClusterRuntime {
    pub(crate) config: IronClusterConfig,    // 当前节点启动配置。
    pub(crate) raft: IronRaft,               // 当前节点 Raft 句柄。
    pub(crate) store: IronRaftStore,         // 当前节点 Raft 内存存储。
    pub(crate) http_client: reqwest::Client, // 集群控制面 HTTP 客户端。
}

// 集群 HTTP 共享状态。
#[derive(Clone)]
pub(crate) struct IronClusterHttpState {
    pub(crate) cluster_token: String, // 集群内部共享密钥。
    pub(crate) raft: IronRaft,        // 当前节点 Raft 句柄。
    pub(crate) store: IronRaftStore,  // 当前节点 Raft 内存存储。
}

// 集群服务端点记录。
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IronClusterEndpointRecord {
    pub name: String,                          // 连接名称。
    pub protocol: IronClusterEndpointProtocol, // 连接协议。
    pub host: String,                          // 连接地址。
    pub port: u16,                             // 连接端口。
}

// 集群服务注册记录。
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IronClusterServiceRecord {
    pub node_id: String,                           // 服务所在节点 ID。
    pub service_name: String,                      // 服务名称。
    pub state: IronClusterState,                   // 服务状态。
    pub endpoints: Vec<IronClusterEndpointRecord>, // 服务连接端点。
}

// 集群服务注册表。
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct IronClusterRegistry {
    pub metadata_version: u64, // 注册表元数据版本。
    pub services: BTreeMap<String, IronClusterServiceRecord>, // 当前服务注册记录。
}

impl IronClusterRegistry {
    // 应用一条集群注册表命令。
    pub(crate) fn apply_command(
        &mut self,
        command: IronClusterCommand,
    ) -> IronClusterCommandResult {
        match command {
            IronClusterCommand::RegisterService(record) => {
                let key = service_record_key(&record.node_id, &record.service_name);
                self.services.insert(key, record);
            }
            IronClusterCommand::UnregisterService {
                node_id,
                service_name,
            } => {
                let key = service_record_key(&node_id, &service_name);
                if let Some(record) = self.services.get_mut(&key) {
                    record.state = IronClusterState::Offline;
                }
            }
        }

        self.metadata_version += 1;

        IronClusterCommandResult {
            metadata_version: self.metadata_version,
        }
    }
}

// 集群注册表写命令。
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum IronClusterCommand {
    RegisterService(IronClusterServiceRecord), // 注册或更新服务。
    UnregisterService {
        node_id: String,      // 下线服务所在节点 ID。
        service_name: String, // 下线服务名称。
    },
}

// 集群注册表命令执行结果。
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct IronClusterCommandResult {
    pub metadata_version: u64, // 注册表元数据版本。
}

// 生成服务注册表键。
pub(crate) fn service_record_key(node_id: &str, service_name: &str) -> String {
    format!("{node_id}:{service_name}")
}
