// 集群 Raft 数据模型。

use super::IronClusterCommand;
use super::IronClusterCommandResult;
use super::IronClusterRegistry;
use openraft::LogId;
use openraft::Snapshot;
use openraft::StoredMembership;
use openraft::Vote;
use openraft::impls::BasicNode;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;

openraft::declare_raft_types!(
    // IronMesh 集群 Raft 类型配置。
    pub IronRaftTypeConfig:
        D = IronClusterCommand,
        R = IronClusterCommandResult,
        NodeId = u64,
        Node = BasicNode,
        Entry = openraft::Entry<IronRaftTypeConfig>,
        SnapshotData = Vec<u8>,
);

// IronMesh 集群 Raft 句柄。
pub type IronRaft = openraft::Raft<IronRaftTypeConfig>;

// IronMesh 集群 Raft 日志条目。
pub type IronRaftEntry = openraft::Entry<IronRaftTypeConfig>;

// IronMesh 集群 Raft 快照。
pub type IronRaftSnapshot = Snapshot<IronRaftTypeConfig>;

// IronMesh 集群 Raft 共享存储。
#[derive(Clone, Debug, Default)]
pub struct IronRaftStore {
    pub inner: Arc<RwLock<IronRaftStoreInner>>, // Raft 日志和状态机共享数据。
}

// IronMesh 集群 Raft 存储内部状态。
#[derive(Clone, Debug)]
pub struct IronRaftStoreInner {
    pub vote: Option<Vote<u64>>,                 // 当前节点保存的投票状态。
    pub committed: Option<LogId<u64>>,           // 当前节点保存的提交位置。
    pub logs: BTreeMap<u64, IronRaftEntry>,      // 当前节点内存 Raft 日志。
    pub last_purged_log_id: Option<LogId<u64>>,  // 已清理的最后日志 ID。
    pub last_applied_log_id: Option<LogId<u64>>, // 状态机已应用的最后日志 ID。
    pub last_membership: StoredMembership<u64, BasicNode>, // 状态机已应用的最后成员配置。
    pub registry: IronClusterRegistry,           // 状态机中的服务注册表。
    pub snapshot: Option<IronRaftSnapshot>,      // 当前状态机快照。
}

impl Default for IronRaftStoreInner {
    // 创建默认 Raft 存储内部状态。
    fn default() -> Self {
        Self {
            vote: None,
            committed: None,
            logs: BTreeMap::new(),
            last_purged_log_id: None,
            last_applied_log_id: None,
            last_membership: StoredMembership::default(),
            registry: IronClusterRegistry::default(),
            snapshot: None,
        }
    }
}

// IronMesh 集群 Raft 快照数据。
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IronRaftSnapshotData {
    pub last_applied_log_id: Option<LogId<u64>>, // 快照包含的最后应用日志 ID。
    pub last_membership: StoredMembership<u64, BasicNode>, // 快照包含的最后成员配置。
    pub registry: IronClusterRegistry,           // 快照包含的服务注册表。
}

// IronMesh 集群 Raft 网络工厂。
#[derive(Clone)]
pub struct IronRaftNetworkFactory {
    pub cluster_token: String,        // 集群内部共享密钥。
    pub http_client: reqwest::Client, // Raft RPC HTTP 客户端。
}

// IronMesh 集群 Raft 单节点网络客户端。
pub struct IronRaftNetwork {
    pub target: u64,                  // 目标 Raft 节点 ID。
    pub target_node: BasicNode,       // 目标 Raft 节点网络信息。
    pub cluster_token: String,        // 集群内部共享密钥。
    pub http_client: reqwest::Client, // Raft RPC HTTP 客户端。
}
