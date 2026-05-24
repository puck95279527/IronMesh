// 集群配置数据结构。

use super::ClusterError;
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
