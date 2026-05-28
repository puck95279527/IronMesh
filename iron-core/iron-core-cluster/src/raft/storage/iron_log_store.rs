use std::fmt::Debug;
use std::ops::RangeBounds;

use openraft::LogState;
use openraft::RaftLogReader;
use openraft::StorageError;
use openraft::storage::LogFlushed;
use openraft::storage::RaftLogStorage;

use crate::raft::IronTypeConfig;

// IronMesh Raft 日志存储。
#[derive(Clone, Debug, Default)]
pub struct IronLogStore;

impl RaftLogReader<IronTypeConfig> for IronLogStore {
    // 读取指定范围内的日志。
    async fn try_get_log_entries<RB>(
        &mut self,
        _range: RB,
    ) -> Result<Vec<openraft::Entry<IronTypeConfig>>, StorageError<u64>>
    where
        RB: RangeBounds<u64> + Clone + Debug + openraft::OptionalSend,
    {
        Ok(Vec::new())
    }
}

impl RaftLogStorage<IronTypeConfig> for IronLogStore {
    type LogReader = Self;

    // 读取日志存储状态。
    async fn get_log_state(&mut self) -> Result<LogState<IronTypeConfig>, StorageError<u64>> {
        Ok(LogState {
            last_purged_log_id: None,
            last_log_id: None,
        })
    }

    // 获取日志读取器。
    async fn get_log_reader(&mut self) -> Self::LogReader {
        Self
    }

    // 保存投票状态。
    async fn save_vote(&mut self, _vote: &openraft::Vote<u64>) -> Result<(), StorageError<u64>> {
        Ok(())
    }

    // 读取投票状态。
    async fn read_vote(&mut self) -> Result<Option<openraft::Vote<u64>>, StorageError<u64>> {
        Ok(None)
    }

    // 保存提交位置。
    async fn save_committed(
        &mut self,
        _committed: Option<openraft::LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        Ok(())
    }

    // 读取提交位置。
    async fn read_committed(&mut self) -> Result<Option<openraft::LogId<u64>>, StorageError<u64>> {
        Ok(None)
    }

    // 追加日志。
    async fn append<I>(
        &mut self,
        _entries: I,
        _callback: LogFlushed<IronTypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = openraft::Entry<IronTypeConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        Ok(())
    }

    // 截断日志。
    async fn truncate(&mut self, _log_id: openraft::LogId<u64>) -> Result<(), StorageError<u64>> {
        Ok(())
    }

    // 清理日志。
    async fn purge(&mut self, _log_id: openraft::LogId<u64>) -> Result<(), StorageError<u64>> {
        Ok(())
    }
}
