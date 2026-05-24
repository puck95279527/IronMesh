// 集群配置数据结构。

use super::BizServiceKind;
use super::ClusterError;
use super::IronRaftStore;
use super::IronRaftTypeConfig;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

// 集群种子节点配置。
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterSeedConfig {
    pub registry_nodes: Vec<ClusterRegistryNodeConfig>, // TOML 中的 registry 种子节点列表。
    pub debug_http: ClusterDebugHttpConfig,             // registry 验证 HTTP 配置。
}

impl ClusterSeedConfig {
    // 从 TOML 文件读取集群种子节点配置。
    pub(crate) fn from_toml_file(path: impl AsRef<Path>) -> Result<Self, ClusterError> {
        let text = fs::read_to_string(path)?;
        Ok(toml::from_str(&text)?)
    }
}

// 集群注册中心种子节点配置。
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterRegistryNodeConfig {
    pub raft_node_id: u64, // registry Raft 节点 ID。
    pub tcp_addr: String,  // registry TCP 地址。
}

// 集群验证 HTTP 配置。
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterDebugHttpConfig {
    pub http_addr: String, // 验证查询 HTTP 地址。
}

// 注册中心启动配置。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterRegistryConfig {
    pub cluster_id: String,                             // 集群 ID。
    pub cluster_token: String,                          // 集群内部共享密钥。
    pub registry_nodes: Vec<ClusterRegistryNodeConfig>, // 注册中心 Raft 节点列表。
    pub debug_http_addr: String,                        // 验证查询 HTTP 地址。
}

// 工作节点启动配置。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterWorkerConfig {
    pub cluster_id: String,                             // 集群 ID。
    pub cluster_token: String,                          // 集群内部共享密钥。
    pub biz_kind: BizServiceKind,                       // 当前业务服务类型。
    pub biz_service_id: String,                         // 当前业务服务实例 ID。
    pub registry_nodes: Vec<ClusterRegistryNodeConfig>, // 注册中心种子节点列表。
}

// 注册中心运行节点。
#[derive(Clone)]
pub(crate) struct ClusterRegistryRuntimeNode {
    pub(crate) raft_node_id: u64, // 当前 Raft 节点 ID。
    pub(crate) tcp_addr: String,  // 当前 TCP 监听地址。
    pub(crate) raft: openraft::Raft<IronRaftTypeConfig>, // 当前节点 Raft 句柄。
    pub(crate) store: IronRaftStore, // 当前节点 Raft 存储。
}

// 注册中心验证 HTTP 共享状态。
#[derive(Clone)]
pub(crate) struct ClusterDebugHttpState {
    pub(crate) nodes: Vec<ClusterRegistryRuntimeNode>, // 注册中心运行节点列表。
}
