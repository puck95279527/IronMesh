// IronMesh Raft 最小快照存储模型。
#[derive(Debug, Clone)]
pub struct IronRaftStoredSnapshot {
    pub meta: openraft::SnapshotMeta<u64, openraft::BasicNode>, // 快照对应的 Raft 元信息。
    pub data: Vec<u8>,                                          // 快照序列化后的字节数据。
}
