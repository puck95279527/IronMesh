use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::LogState;
use openraft::RaftLogReader;
use openraft::StorageError;
use openraft::storage::LogFlushed;
use openraft::storage::RaftLogStorage;
use tokio::sync::Mutex;

use crate::raft::IronTypeConfig;

// IronMesh Raft 日志存储。
#[derive(Clone, Debug, Default)]
pub struct IronLogStore {
    pub last_purged_log_id: Arc<Mutex<Option<openraft::LogId<u64>>>>, // 已清理的最后一条日志标识。
    pub logs: Arc<Mutex<BTreeMap<u64, openraft::Entry<IronTypeConfig>>>>, // 按日志索引保存的 Raft 日志。
    pub committed: Arc<Mutex<Option<openraft::LogId<u64>>>>, // 已提交的最后一条日志标识。
    pub vote: Arc<Mutex<Option<openraft::Vote<u64>>>>,       // 当前节点保存的投票状态。
}

impl RaftLogReader<IronTypeConfig> for IronLogStore {
    // 读取指定范围内的日志。
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<openraft::Entry<IronTypeConfig>>, StorageError<u64>>
    where
        RB: RangeBounds<u64> + Clone + Debug + openraft::OptionalSend,
    {
        let logs = self.logs.lock().await;
        Ok(logs.range(range).map(|(_, entry)| entry.clone()).collect())
    }
}

impl RaftLogStorage<IronTypeConfig> for IronLogStore {
    type LogReader = Self;

    // 读取日志存储状态。
    async fn get_log_state(&mut self) -> Result<LogState<IronTypeConfig>, StorageError<u64>> {
        let logs = self.logs.lock().await;
        let last_purged_log_id = *self.last_purged_log_id.lock().await;
        let last_log_id = logs
            .iter()
            .next_back()
            .map(|(_, entry)| entry.log_id)
            .or(last_purged_log_id);

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    // 获取日志读取器。
    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    // 保存投票状态。
    async fn save_vote(&mut self, vote: &openraft::Vote<u64>) -> Result<(), StorageError<u64>> {
        *self.vote.lock().await = Some(*vote);
        Ok(())
    }

    // 读取投票状态。
    async fn read_vote(&mut self) -> Result<Option<openraft::Vote<u64>>, StorageError<u64>> {
        Ok(*self.vote.lock().await)
    }

    // 保存提交位置。
    async fn save_committed(
        &mut self,
        committed: Option<openraft::LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        *self.committed.lock().await = committed;
        Ok(())
    }

    // 读取提交位置。
    async fn read_committed(&mut self) -> Result<Option<openraft::LogId<u64>>, StorageError<u64>> {
        Ok(*self.committed.lock().await)
    }

    // 追加日志。
    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<IronTypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = openraft::Entry<IronTypeConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let mut logs = self.logs.lock().await;
        for entry in entries {
            logs.insert(entry.log_id.index, entry);
        }

        callback.log_io_completed(Ok(()));
        Ok(())
    }

    // 截断日志。
    async fn truncate(&mut self, log_id: openraft::LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut logs = self.logs.lock().await;
        let keys = logs
            .range(log_id.index..)
            .map(|(key, _)| *key)
            .collect::<Vec<_>>();
        for key in keys {
            logs.remove(&key);
        }

        Ok(())
    }

    // 清理日志。
    async fn purge(&mut self, log_id: openraft::LogId<u64>) -> Result<(), StorageError<u64>> {
        *self.last_purged_log_id.lock().await = Some(log_id);

        let mut logs = self.logs.lock().await;
        let keys = logs
            .range(..=log_id.index)
            .map(|(key, _)| *key)
            .collect::<Vec<_>>();
        for key in keys {
            logs.remove(&key);
        }

        Ok(())
    }
}
