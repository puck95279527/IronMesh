use std::io::Cursor;

use openraft::BasicNode;

use crate::raft::model::command::iron_cluster_write_response::IronClusterWriteResponse;
use crate::raft::model::command::iron_raft_request::IronRaftRequest;

openraft::declare_raft_types!(
    // IronMesh 集群 Raft 类型配置。
    pub IronRaftTypeConfig:
        D = IronRaftRequest,
        R = IronClusterWriteResponse,
        NodeId = u64,
        Node = BasicNode,
        Entry = openraft::Entry<IronRaftTypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
);
