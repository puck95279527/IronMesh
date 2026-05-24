use crate::raft::model::iron_raft_stored_snapshot::IronRaftStoredSnapshot;
use crate::raft::model::iron_raft_state_machine_data::IronRaftStateMachineData;

// IronMesh Raft 最小状态机存储模型。
#[derive(Debug, Clone, Default)]
pub struct IronRaftStateMachineStore {
    pub last_applied_log: Option<openraft::LogId<u64>>, // 状态机已经应用的最后一条日志标识。
    pub last_membership: openraft::StoredMembership<u64, openraft::BasicNode>, // 状态机已经应用的最后一个成员关系。
    pub state_machine: IronRaftStateMachineData, // 当前节点持有的最小状态机数据。
    pub snapshot_idx: u64, // 用于生成快照标识的递增序号。
    pub current_snapshot: Option<IronRaftStoredSnapshot>, // 当前状态机保存的快照。
}
