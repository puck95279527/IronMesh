use std::collections::BTreeMap;

// IronMesh Raft 最小状态机数据模型。
#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct IronRaftStateMachineData {
    pub data: BTreeMap<String, String>, // 状态机中保存的最小键值数据。
}
