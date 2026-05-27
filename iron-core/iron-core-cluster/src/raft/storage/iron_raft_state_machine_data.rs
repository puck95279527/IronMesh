use serde::de::DeserializeOwned;

use crate::raft::model::command::iron_cluster_write_response::IronClusterWriteResponse;
use crate::raft::model::command::iron_raft_request::IronRaftRequest;

// IronMesh Raft 状态机数据范式，用于约束可被 Raft 存储层托管的数据模型。
pub(crate) trait IronRaftStateMachineData:
    Clone + Default + serde::Serialize + DeserializeOwned + Send + Sync + 'static
{
    // 应用一条 Raft 写入请求，并返回写入结果。
    fn apply_raft_request(&mut self, request: IronRaftRequest) -> IronClusterWriteResponse;
}
