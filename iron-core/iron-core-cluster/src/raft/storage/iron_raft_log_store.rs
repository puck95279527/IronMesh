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

use crate::raft::model::iron_raft_type_config::IronRaftTypeConfig;

// IronMesh Raft 最小日志存储模型。
#[derive(Debug, Clone)]
pub struct IronRaftLogStore {
    pub last_purged_log_id: Arc<Mutex<Option<openraft::LogId<u64>>>>, // 已清理的最后一条日志标识。
    pub logs: Arc<Mutex<BTreeMap<u64, openraft::Entry<IronRaftTypeConfig>>>>, // 按日志索引保存的 Raft 日志。
    pub committed: Arc<Mutex<Option<openraft::LogId<u64>>>>, // 已提交的最后一条日志标识。
    pub vote: Arc<Mutex<Option<openraft::Vote<u64>>>>,       // 当前节点保存的投票状态。
}

impl Default for IronRaftLogStore {
    // 创建空的最小日志存储。
    fn default() -> Self {
        Self {
            last_purged_log_id: Arc::new(Mutex::new(None)),
            logs: Arc::new(Mutex::new(BTreeMap::new())),
            committed: Arc::new(Mutex::new(None)),
            vote: Arc::new(Mutex::new(None)),
        }
    }
}

impl RaftLogReader<IronRaftTypeConfig> for IronRaftLogStore {
    // 读取指定范围内的日志。
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<openraft::Entry<IronRaftTypeConfig>>, StorageError<u64>>
    where
        RB: RangeBounds<u64> + Clone + Debug + openraft::OptionalSend,
    {
        let logs = self.logs.lock().await;
        Ok(logs.range(range).map(|(_, entry)| entry.clone()).collect())
    }
}

impl RaftLogStorage<IronRaftTypeConfig> for IronRaftLogStore {
    type LogReader = Self;

    // 读取日志存储的边界状态。
    async fn get_log_state(&mut self) -> Result<LogState<IronRaftTypeConfig>, StorageError<u64>> {
        let logs = self.logs.lock().await;
        let last_purged_log_id = self.last_purged_log_id.lock().await.clone();
        let last_log_id = logs
            .iter()
            .next_back()
            .map(|(_, entry)| entry.log_id.clone())
            .or_else(|| last_purged_log_id.clone());

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
        *self.vote.lock().await = Some(vote.clone());
        Ok(())
    }

    // 读取当前保存的投票状态。
    async fn read_vote(&mut self) -> Result<Option<openraft::Vote<u64>>, StorageError<u64>> {
        Ok(self.vote.lock().await.clone())
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
        Ok(self.committed.lock().await.clone())
    }

    // 追加日志。
    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<IronRaftTypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = openraft::Entry<IronRaftTypeConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let mut logs = self.logs.lock().await;
        for entry in entries {
            logs.insert(entry.log_id.index, entry);
        }

        callback.log_io_completed(Ok(()));
        Ok(())
    }

    // 截断指定日志及之后的日志。
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

    // 清理指定日志及之前的日志。
    async fn purge(&mut self, log_id: openraft::LogId<u64>) -> Result<(), StorageError<u64>> {
        *self.last_purged_log_id.lock().await = Some(log_id.clone());

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
