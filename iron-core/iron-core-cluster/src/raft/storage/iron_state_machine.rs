use std::io::Cursor;
use std::sync::Arc;

use openraft::RaftSnapshotBuilder;
use openraft::Snapshot;
use openraft::SnapshotMeta;
use openraft::StorageError;
use openraft::entry::RaftPayload;
use openraft::storage::RaftStateMachine;
use tokio::sync::Mutex;

use crate::raft::IronTypeConfig;

// IronMesh Raft 状态机。
#[derive(Clone, Debug, Default)]
pub struct IronStateMachine {
    pub last_applied_log: Arc<Mutex<Option<openraft::LogId<u64>>>>, // 状态机已经应用的最后一条日志标识。
    pub last_membership: Arc<Mutex<openraft::StoredMembership<u64, openraft::BasicNode>>>, // 状态机已经应用的最后一个成员关系。
}

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
        Ok((
            *self.last_applied_log.lock().await,
            self.last_membership.lock().await.clone(),
        ))
    }

    // 应用已经提交的日志。
    async fn apply<I>(&mut self, entries: I) -> Result<Vec<()>, StorageError<u64>>
    where
        I: IntoIterator<Item = openraft::Entry<IronTypeConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let mut responses = Vec::new();

        for entry in entries {
            *self.last_applied_log.lock().await = Some(entry.log_id);

            if let Some(membership) = entry.get_membership() {
                *self.last_membership.lock().await =
                    openraft::StoredMembership::new(Some(entry.log_id), membership.clone());
            }

            responses.push(());
        }

        Ok(responses)
    }

    // 获取快照构建器。
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
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
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<IronTypeConfig>>, StorageError<u64>> {
        let last_applied_log = *self.last_applied_log.lock().await;
        let last_membership = self.last_membership.lock().await.clone();

        Ok(Some(Snapshot {
            meta: SnapshotMeta {
                last_log_id: last_applied_log,
                last_membership,
                snapshot_id: String::new(),
            },
            snapshot: Box::new(Cursor::new(Vec::new())),
        }))
    }
}

impl RaftSnapshotBuilder<IronTypeConfig> for IronStateMachine {
    // 构建当前状态机快照。
    async fn build_snapshot(&mut self) -> Result<Snapshot<IronTypeConfig>, StorageError<u64>> {
        let last_applied_log = *self.last_applied_log.lock().await;
        let last_membership = self.last_membership.lock().await.clone();

        Ok(Snapshot {
            meta: SnapshotMeta {
                last_log_id: last_applied_log,
                last_membership,
                snapshot_id: String::new(),
            },
            snapshot: Box::new(Cursor::new(Vec::new())),
        })
    }
}
