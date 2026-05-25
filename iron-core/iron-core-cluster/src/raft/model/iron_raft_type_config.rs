use std::io::Cursor;

use openraft::BasicNode;

use crate::raft::model::command::iron_raft_request::IronRaftRequest;
use crate::raft::model::command::iron_raft_response::IronRaftResponse;

openraft::declare_raft_types!(
    // IronMesh 集群 Raft 类型配置。
    pub IronRaftTypeConfig:
        D = IronRaftRequest,
        R = IronRaftResponse,
        NodeId = u64,
        Node = BasicNode,
        Entry = openraft::Entry<IronRaftTypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
);
