use std::collections::BTreeMap;

use crate::raft::model::iron_raft_type_config::IronRaftTypeConfig;

// IronMesh Raft 最小日志存储模型。
#[derive(Debug, Clone, Default)]
pub struct IronRaftLogStore {
    pub last_purged_log_id: Option<openraft::LogId<u64>>, // 已清理的最后一条日志标识。
    pub logs: BTreeMap<u64, openraft::Entry<IronRaftTypeConfig>>, // 按日志索引保存的 Raft 日志。
    pub committed: Option<openraft::LogId<u64>>, // 已提交的最后一条日志标识。
    pub vote: Option<openraft::Vote<u64>>, // 当前节点保存的投票状态。
}
