use crate::data_plane::iron_cluster_data_command::IronClusterDataCommand;

// IronMesh Raft 最小请求模型。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub enum IronRaftRequest {
    // 集群业务数据写命令。
    ClusterData(IronClusterDataCommand),
}
