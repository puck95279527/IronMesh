use std::io::Cursor;

use openraft::BasicNode;

// IronMesh Raft 类型配置。
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct IronTypeConfig;

impl openraft::RaftTypeConfig for IronTypeConfig {
    type D = ();
    type R = ();
    type NodeId = u64;
    type Node = BasicNode;
    type Entry = openraft::Entry<Self>;
    type SnapshotData = Cursor<Vec<u8>>;
    type AsyncRuntime = openraft::TokioRuntime;
    type Responder = openraft::impls::OneshotResponder<Self>;
}
