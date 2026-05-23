// 集群 Raft 内存存储实现。

use crate::model::IronClusterCommandResult;
use crate::model::IronRaftEntry;
use crate::model::IronRaftSnapshotData;
use crate::model::IronRaftStore;
use crate::model::IronRaftTypeConfig;
use openraft::LogId;
use openraft::LogState;
use openraft::RaftLogReader;
use openraft::Snapshot;
use openraft::SnapshotMeta;
use openraft::StorageError;
use openraft::StoredMembership;
use openraft::Vote;
use openraft::entry::EntryPayload;
use openraft::impls::BasicNode;
use openraft::storage::LogFlushed;
use openraft::storage::RaftLogStorage;
use openraft::storage::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use std::ops::{Bound, RangeBounds};

impl IronRaftStore {
    // 读取当前服务注册表快照。
    pub(crate) async fn registry_snapshot(&self) -> crate::model::IronClusterRegistry {
        self.inner.read().await.registry.clone()
    }
}

impl RaftLogReader<IronRaftTypeConfig> for IronRaftStore {
    // 按范围读取 Raft 日志条目。
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<IronRaftEntry>, StorageError<u64>> {
        let start = match range.start_bound() {
            Bound::Included(value) => *value,
            Bound::Excluded(value) => value.saturating_add(1),
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(value) => value.saturating_add(1),
            Bound::Excluded(value) => *value,
            Bound::Unbounded => u64::MAX,
        };
        let inner = self.inner.read().await;

        Ok(inner
            .logs
            .range(start..end)
            .map(|(_, entry)| entry.clone())
            .collect())
    }
}

impl RaftLogStorage<IronRaftTypeConfig> for IronRaftStore {
    type LogReader = IronRaftStore;

    // 读取当前日志状态。
    async fn get_log_state(&mut self) -> Result<LogState<IronRaftTypeConfig>, StorageError<u64>> {
        let inner = self.inner.read().await;
        let last_log_id = inner
            .logs
            .values()
            .next_back()
            .map(|entry| entry.log_id)
            .or(inner.last_purged_log_id);

        Ok(LogState {
            last_purged_log_id: inner.last_purged_log_id,
            last_log_id,
        })
    }

    // 获取日志读取器。
    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    // 保存当前投票状态。
    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        self.inner.write().await.vote = Some(vote.clone());
        Ok(())
    }

    // 读取当前投票状态。
    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.inner.read().await.vote.clone())
    }

    // 保存当前提交位置。
    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        self.inner.write().await.committed = committed;
        Ok(())
    }

    // 读取当前提交位置。
    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        Ok(self.inner.read().await.committed)
    }

    // 追加 Raft 日志条目。
    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<IronRaftTypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = IronRaftEntry> + Send,
        I::IntoIter: Send,
    {
        let mut inner = self.inner.write().await;
        for entry in entries {
            inner.logs.insert(entry.log_id.index, entry);
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    // 从指定日志 ID 开始截断日志。
    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.inner.write().await.logs.split_off(&log_id.index);
        Ok(())
    }

    // 清理指定日志 ID 之前的日志。
    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.write().await;
        inner.logs.retain(|index, _| *index > log_id.index);
        inner.last_purged_log_id = Some(log_id);
        Ok(())
    }
}

impl RaftStateMachine<IronRaftTypeConfig> for IronRaftStore {
    type SnapshotBuilder = IronRaftStore;

    // 读取状态机已应用状态。
    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        let inner = self.inner.read().await;
        Ok((inner.last_applied_log_id, inner.last_membership.clone()))
    }

    // 应用已提交的 Raft 日志到状态机。
    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<IronClusterCommandResult>, StorageError<u64>>
    where
        I: IntoIterator<Item = IronRaftEntry> + Send,
        I::IntoIter: Send,
    {
        let mut results = Vec::new();
        let mut inner = self.inner.write().await;

        for entry in entries {
            inner.last_applied_log_id = Some(entry.log_id);
            match entry.payload {
                EntryPayload::Blank => results.push(IronClusterCommandResult::default()),
                EntryPayload::Membership(membership) => {
                    inner.last_membership = StoredMembership::new(Some(entry.log_id), membership);
                    results.push(IronClusterCommandResult::default());
                }
                EntryPayload::Normal(command) => {
                    let result = inner.registry.apply_command(command);
                    results.push(result);
                }
            }
        }

        Ok(results)
    }

    // 获取快照构建器。
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    // 开始接收快照。
    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Vec<u8>>, StorageError<u64>> {
        Ok(Box::new(Vec::new()))
    }

    // 安装接收到的快照。
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Vec<u8>>,
    ) -> Result<(), StorageError<u64>> {
        let snapshot_data: IronRaftSnapshotData =
            serde_json::from_slice(&snapshot).map_err(|error| snapshot_storage_error(error))?;
        let mut inner = self.inner.write().await;

        inner.last_applied_log_id = snapshot_data.last_applied_log_id;
        inner.last_membership = snapshot_data.last_membership;
        inner.registry = snapshot_data.registry;
        inner.snapshot = Some(Snapshot {
            meta: meta.clone(),
            snapshot,
        });

        Ok(())
    }

    // 读取当前快照。
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<IronRaftTypeConfig>>, StorageError<u64>> {
        Ok(self.inner.read().await.snapshot.clone())
    }
}

impl RaftSnapshotBuilder<IronRaftTypeConfig> for IronRaftStore {
    // 构建当前状态机快照。
    async fn build_snapshot(&mut self) -> Result<Snapshot<IronRaftTypeConfig>, StorageError<u64>> {
        let inner = self.inner.read().await;
        let data = IronRaftSnapshotData {
            last_applied_log_id: inner.last_applied_log_id,
            last_membership: inner.last_membership.clone(),
            registry: inner.registry.clone(),
        };
        let snapshot = serde_json::to_vec(&data).map_err(|error| snapshot_storage_error(error))?;
        let snapshot_id = format!(
            "{}-{}",
            inner
                .last_applied_log_id
                .map(|log_id| log_id.index)
                .unwrap_or_default(),
            inner.registry.metadata_version
        );

        Ok(Snapshot {
            meta: SnapshotMeta {
                last_log_id: inner.last_applied_log_id,
                last_membership: inner.last_membership.clone(),
                snapshot_id,
            },
            snapshot: Box::new(snapshot),
        })
    }
}

// 构造快照存储错误。
fn snapshot_storage_error(error: serde_json::Error) -> StorageError<u64> {
    StorageError::from_io_error(
        openraft::ErrorSubject::Snapshot(None),
        openraft::ErrorVerb::Read,
        std::io::Error::new(std::io::ErrorKind::InvalidData, error),
    )
}
