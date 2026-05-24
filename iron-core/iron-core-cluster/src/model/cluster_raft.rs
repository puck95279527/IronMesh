// 集群 Raft 数据模型。

use super::ClusterCommand;
use super::IronClusterService;
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
        D = ClusterCommand,
        R = (),
        NodeId = u64,
        Node = BasicNode,
        Entry = openraft::Entry<IronRaftTypeConfig>,
        SnapshotData = Vec<u8>,
);

// IronMesh 集群 Raft 共享存储。
#[derive(Clone, Debug, Default)]
pub struct IronRaftStore {
    pub inner: Arc<RwLock<IronRaftStoreInner>>, // Raft 日志和状态机共享数据。
}

// IronMesh 集群 Raft 存储内部状态。
#[derive(Clone, Debug)]
pub struct IronRaftStoreInner {
    pub vote: Option<Vote<u64>>,       // 当前节点保存的投票状态。
    pub committed: Option<LogId<u64>>, // 当前节点保存的提交位置。
    pub logs: BTreeMap<u64, openraft::Entry<IronRaftTypeConfig>>, // 当前节点内存 Raft 日志。
    pub last_purged_log_id: Option<LogId<u64>>, // 已清理的最后日志 ID。
    pub last_applied_log_id: Option<LogId<u64>>, // 状态机已应用的最后日志 ID。
    pub last_membership: StoredMembership<u64, BasicNode>, // 状态机已应用的最后成员配置。
    pub registry: BTreeMap<String, IronClusterService>, // 状态机中的服务注册表。
    pub snapshot: Option<Snapshot<IronRaftTypeConfig>>, // 当前状态机快照。
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
            registry: BTreeMap::default(),
            snapshot: None,
        }
    }
}

// IronMesh 集群 Raft 状态。
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IronClusterRaftState {
    pub last_applied_log_id: Option<LogId<u64>>, // 状态机已应用的最后日志 ID。
    pub last_membership: StoredMembership<u64, BasicNode>, // 状态机当前成员配置。
    pub registry: BTreeMap<String, IronClusterService>, // 状态机当前服务注册表。
}
