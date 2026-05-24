use std::io::Cursor;
use std::sync::Arc;

use openraft::EntryPayload;
use openraft::RaftSnapshotBuilder;
use openraft::Snapshot;
use openraft::SnapshotMeta;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::entry::RaftPayload;
use tokio::sync::Mutex;
use openraft::storage::RaftStateMachine;

use crate::raft::model::iron_raft_request::IronRaftRequest;
use crate::raft::model::iron_raft_stored_snapshot::IronRaftStoredSnapshot;
use crate::raft::dto::iron_raft_state_machine_data::IronRaftStateMachineData;
use crate::raft::model::iron_raft_type_config::IronRaftTypeConfig;

// IronMesh Raft 最小状态机存储模型。
#[derive(Debug, Clone)]
pub struct IronRaftStateMachineStore {
    pub last_applied_log: Arc<Mutex<Option<openraft::LogId<u64>>>>, // 状态机已经应用的最后一条日志标识。
    pub last_membership: Arc<Mutex<openraft::StoredMembership<u64, openraft::BasicNode>>>, // 状态机已经应用的最后一个成员关系。
    pub state_machine: Arc<Mutex<IronRaftStateMachineData>>, // 当前节点持有的最小状态机数据。
    pub snapshot_idx: Arc<Mutex<u64>>, // 用于生成快照标识的递增序号。
    pub current_snapshot: Arc<Mutex<Option<IronRaftStoredSnapshot>>>, // 当前状态机保存的快照。
}

impl Default for IronRaftStateMachineStore {
    // 创建空的最小状态机存储。
    fn default() -> Self {
        Self {
            last_applied_log: Arc::new(Mutex::new(None)),
            last_membership: Arc::new(Mutex::new(openraft::StoredMembership::default())),
            state_machine: Arc::new(Mutex::new(IronRaftStateMachineData::default())),
            snapshot_idx: Arc::new(Mutex::new(0)),
            current_snapshot: Arc::new(Mutex::new(None)),
        }
    }
}

impl RaftStateMachine<IronRaftTypeConfig> for IronRaftStateMachineStore {
    type SnapshotBuilder = Self;

    // 读取状态机已经应用的状态。
    async fn applied_state(
        &mut self,
    ) -> Result<(Option<openraft::LogId<u64>>, openraft::StoredMembership<u64, openraft::BasicNode>), StorageError<u64>>
    {
        Ok((self.last_applied_log.lock().await.clone(), self.last_membership.lock().await.clone()))
    }

    // 应用已经提交的日志到状态机。
    async fn apply<I>(&mut self, entries: I) -> Result<Vec<crate::raft::model::iron_raft_response::IronRaftResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = openraft::Entry<IronRaftTypeConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let mut responses = Vec::new();

        for entry in entries {
            *self.last_applied_log.lock().await = Some(entry.log_id.clone());

            if let Some(membership) = entry.get_membership() {
                *self.last_membership.lock().await =
                    openraft::StoredMembership::new(Some(entry.log_id.clone()), membership.clone());
            }

            let response = match entry.payload {
                EntryPayload::Blank => crate::raft::model::iron_raft_response::IronRaftResponse::default(),
                EntryPayload::Normal(IronRaftRequest::Set { key, value }) => {
                    self.state_machine.lock().await.data.insert(key, value.clone());
                    crate::raft::model::iron_raft_response::IronRaftResponse { value: Some(value) }
                }
                EntryPayload::Membership(_) => crate::raft::model::iron_raft_response::IronRaftResponse::default(),
            };

            responses.push(response);
        }

        Ok(responses)
    }

    // 获取快照构建器。
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    // 开始接收新的快照。
    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    // 安装已经接收完成的快照。
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let data = snapshot.into_inner();
        let state_machine_data = serde_json::from_slice(&data).map_err(|error| StorageError::IO {
            source: StorageIOError::read_snapshot(Some(meta.signature()), openraft::AnyError::new(&error)),
        })?;

        *self.last_applied_log.lock().await = meta.last_log_id.clone();
        *self.last_membership.lock().await = meta.last_membership.clone();
        *self.state_machine.lock().await = IronRaftStateMachineData {
            data: state_machine_data,
        };
        *self.current_snapshot.lock().await = Some(IronRaftStoredSnapshot {
            meta: meta.clone(),
            data,
        });

        Ok(())
    }

    // 读取当前快照。
    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<IronRaftTypeConfig>>, StorageError<u64>> {
        Ok(self.current_snapshot.lock().await.as_ref().map(|snapshot| Snapshot {
            meta: snapshot.meta.clone(),
            snapshot: Box::new(Cursor::new(snapshot.data.clone())),
        }))
    }
}

impl RaftSnapshotBuilder<IronRaftTypeConfig> for IronRaftStateMachineStore {
    // 构建当前状态机快照。
    async fn build_snapshot(&mut self) -> Result<Snapshot<IronRaftTypeConfig>, StorageError<u64>> {
        let state_machine = self.state_machine.lock().await;
        let data = serde_json::to_vec(&state_machine.data).map_err(|error| StorageError::IO {
            source: StorageIOError::write_snapshot(None, openraft::AnyError::new(&error)),
        })?;
        drop(state_machine);

        let mut snapshot_idx = self.snapshot_idx.lock().await;
        *snapshot_idx += 1;

        let last_applied_log = self.last_applied_log.lock().await.clone();
        let last_membership = self.last_membership.lock().await.clone();

        let snapshot_id = match &last_applied_log {
            Some(log_id) => format!("{}-{}-{}", log_id.committed_leader_id(), log_id.index, *snapshot_idx),
            None => format!("--{}", *snapshot_idx),
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        *self.current_snapshot.lock().await = Some(IronRaftStoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        });

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}
