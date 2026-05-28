use std::io::Cursor;

use openraft::RaftSnapshotBuilder;
use openraft::Snapshot;
use openraft::SnapshotMeta;
use openraft::StorageError;
use openraft::storage::RaftStateMachine;

use crate::raft::IronTypeConfig;

// IronMesh Raft 状态机。
#[derive(Clone, Debug, Default)]
pub struct IronStateMachine;

impl RaftStateMachine<IronTypeConfig> for IronStateMachine {
    type SnapshotBuilder = Self;

    // 读取状态机已经应用的状态。
    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<openraft::LogId<u64>>,
            openraft::StoredMembership<u64, openraft::BasicNode>,
        ),
        StorageError<u64>,
    > {
        Ok((None, openraft::StoredMembership::default()))
    }

    // 应用已经提交的日志。
    async fn apply<I>(&mut self, _entries: I) -> Result<Vec<()>, StorageError<u64>>
    where
        I: IntoIterator<Item = openraft::Entry<IronTypeConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        Ok(Vec::new())
    }

    // 获取快照构建器。
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        Self
    }

    // 开始接收新的快照。
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    // 安装已经接收完成的快照。
    async fn install_snapshot(
        &mut self,
        _meta: &SnapshotMeta<u64, openraft::BasicNode>,
        _snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        Ok(())
    }

    // 读取当前快照。
    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<IronTypeConfig>>, StorageError<u64>> {
        Ok(None)
    }
}

impl RaftSnapshotBuilder<IronTypeConfig> for IronStateMachine {
    // 构建当前状态机快照。
    async fn build_snapshot(&mut self) -> Result<Snapshot<IronTypeConfig>, StorageError<u64>> {
        Ok(Snapshot {
            meta: SnapshotMeta {
                last_log_id: None,
                last_membership: openraft::StoredMembership::default(),
                snapshot_id: String::new(),
            },
            snapshot: Box::new(Cursor::new(Vec::new())),
        })
    }
}
