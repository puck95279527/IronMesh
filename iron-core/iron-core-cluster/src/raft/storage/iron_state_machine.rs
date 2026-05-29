use std::io::Cursor;
use std::sync::Arc;

use openraft::BasicNode;
use openraft::RaftSnapshotBuilder;
use openraft::Snapshot;
use openraft::SnapshotMeta;
use openraft::StorageError;
use openraft::entry::RaftPayload;
use openraft::storage::RaftStateMachine;
use tokio::sync::Mutex;

use crate::raft::IronTypeConfig;

// IronMesh Raft 状态机内部状态。
#[derive(Debug, Default)]
struct IronStateMachineInner {
    last_applied_log: Option<openraft::LogId<u64>>, // 状态机已经应用的最后一条日志标识。
    last_membership: openraft::StoredMembership<u64, openraft::BasicNode>, // 状态机已经应用的最后一个成员关系。
    current_snapshot_meta: Option<SnapshotMeta<u64, BasicNode>>,           // 当前状态机快照元数据。
}

// IronMesh Raft 状态机。
#[derive(Clone, Debug, Default)]
pub struct IronStateMachine {
    inner: Arc<Mutex<IronStateMachineInner>>, // 状态机内部状态。
}

impl IronStateMachine {
    // 构建当前状态机快照元数据。
    async fn build_snapshot_meta(&self) -> SnapshotMeta<u64, BasicNode> {
        let inner = self.inner.lock().await;
        let last_applied_log = inner.last_applied_log;
        let last_membership = inner.last_membership.clone();
        let snapshot_id = format!(
            "iron-snapshot-last={last_applied_log:?}-membership={:?}",
            last_membership.log_id()
        );

        SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        }
    }
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
        let inner = self.inner.lock().await;
        Ok((inner.last_applied_log, inner.last_membership.clone()))
    }

    // 应用已经提交的日志。
    async fn apply<I>(&mut self, entries: I) -> Result<Vec<()>, StorageError<u64>>
    where
        I: IntoIterator<Item = openraft::Entry<IronTypeConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let mut responses = Vec::new();
        let mut inner = self.inner.lock().await;

        for entry in entries {
            inner.last_applied_log = Some(entry.log_id);

            if let Some(membership) = entry.get_membership() {
                inner.last_membership =
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
        meta: &SnapshotMeta<u64, openraft::BasicNode>,
        _snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.lock().await;
        inner.last_applied_log = meta.last_log_id;
        inner.last_membership = meta.last_membership.clone();
        inner.current_snapshot_meta = Some(meta.clone());
        Ok(())
    }

    // 读取当前快照。
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<IronTypeConfig>>, StorageError<u64>> {
        let inner = self.inner.lock().await;
        let meta = match inner.current_snapshot_meta.clone() {
            Some(meta) => meta,
            None => {
                let last_applied_log = inner.last_applied_log;
                let last_membership = inner.last_membership.clone();
                let snapshot_id = format!(
                    "iron-snapshot-last={last_applied_log:?}-membership={:?}",
                    last_membership.log_id()
                );

                SnapshotMeta {
                    last_log_id: last_applied_log,
                    last_membership,
                    snapshot_id,
                }
            }
        };

        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(Vec::new())),
        }))
    }
}

impl RaftSnapshotBuilder<IronTypeConfig> for IronStateMachine {
    // 构建当前状态机快照。
    async fn build_snapshot(&mut self) -> Result<Snapshot<IronTypeConfig>, StorageError<u64>> {
        let meta = self.build_snapshot_meta().await;
        self.inner.lock().await.current_snapshot_meta = Some(meta.clone());

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(Vec::new())),
        })
    }
}
