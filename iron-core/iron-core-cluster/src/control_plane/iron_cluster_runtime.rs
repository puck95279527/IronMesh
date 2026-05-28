use std::error::Error;
use std::io::Error as IoError;
use std::sync::Arc;

use openraft::Raft;
use tokio::task::JoinSet;

use crate::api::iron_cluster_write_error::IronClusterWriteError;
use crate::contract::iron_cluster_entity_model::IronClusterEntityModel;
use crate::contract::iron_cluster_entity_model_source_node_tagged::IronClusterEntityModelSourceNodeTagged;
use crate::control_plane::iron_cluster_write_router::IronClusterWriteRouter;
use crate::data_plane::iron_cluster_entity::IronClusterEntity;
use crate::data_plane::iron_cluster_state::IronClusterState;
use crate::raft::control::iron_cluster_node::IronClusterNode;
use crate::raft::model::command::iron_cluster_write_response::IronClusterWriteResponse;
use crate::raft::model::command::iron_raft_state_machine_write_request::IronRaftStateMachineWriteRequest;
use crate::raft::model::iron_raft_type_config::IronRaftTypeConfig;
use crate::raft::storage::iron_raft_state_machine_container::IronRaftStateMachineContainer;
use crate::raft::storage::iron_raft_state_machine_data::IronRaftStateMachineData;
use crate::raft::storage::iron_raft_state_machine_store::IronRaftStateMachineStore;

// IronMesh 集群运行时。
pub(crate) struct IronClusterRuntime<S>
where
    S: IronRaftStateMachineData,
{
    // 当前集群节点信息，用于对外 API 记录操作来源。
    current_node: IronClusterNode,
    // 当前节点本地状态机存储。
    state_machine_store: IronRaftStateMachineStore<S>,
    // 集群写入路由器。
    write_router: IronClusterWriteRouter<S>,
    // 当前集群节点托管的后台任务集合。
    tasks: Arc<tokio::sync::Mutex<JoinSet<()>>>,
}

impl IronClusterRuntime<IronRaftStateMachineContainer<IronClusterState>> {
    // 新增默认集群业务实体。
    pub(crate) async fn insert_cluster_data<T>(
        &self,
        value: T,
    ) -> Result<IronClusterWriteResponse<IronClusterEntity>, IronClusterWriteError>
    where
        T: IronClusterEntityModel
            + IronClusterEntityModelSourceNodeTagged
            + Into<IronClusterEntity>,
    {
        self.write_cluster_data(IronRaftStateMachineWriteRequest::cluster_insert(
            self.current_node_id(),
            value,
        ))
        .await
    }

    // 修改默认集群业务实体。
    pub(crate) async fn update_cluster_data<T>(
        &self,
        value: T,
    ) -> Result<IronClusterWriteResponse<IronClusterEntity>, IronClusterWriteError>
    where
        T: IronClusterEntityModel
            + IronClusterEntityModelSourceNodeTagged
            + Into<IronClusterEntity>,
    {
        self.write_cluster_data(IronRaftStateMachineWriteRequest::cluster_update(
            self.current_node_id(),
            value,
        ))
        .await
    }

    // 删除默认集群业务实体。
    pub(crate) async fn delete_cluster_data<T>(
        &self,
        value: T,
    ) -> Result<IronClusterWriteResponse<IronClusterEntity>, IronClusterWriteError>
    where
        T: IronClusterEntityModel
            + IronClusterEntityModelSourceNodeTagged
            + Into<IronClusterEntity>,
    {
        self.write_cluster_data(IronRaftStateMachineWriteRequest::cluster_delete(
            self.current_node_id(),
            value,
        ))
        .await
    }

    // 按实体键删除默认集群业务实体。
    pub(crate) async fn delete_cluster_data_key<T>(
        &self,
        key: T::Key,
    ) -> Result<IronClusterWriteResponse<IronClusterEntity>, IronClusterWriteError>
    where
        T: IronClusterEntityModel
            + IronClusterEntityModelSourceNodeTagged
            + Into<IronClusterEntity>,
    {
        self.write_cluster_data(IronRaftStateMachineWriteRequest::cluster_delete_key::<T>(
            self.current_node_id(),
            key,
        ))
        .await
    }
}

impl<S> IronClusterRuntime<S>
where
    S: IronRaftStateMachineData,
{
    // 创建集群运行时。
    pub(crate) fn new(
        current_node: IronClusterNode,
        raft: Raft<IronRaftTypeConfig<S>>,
        state_machine_store: IronRaftStateMachineStore<S>,
        tasks: JoinSet<()>,
    ) -> Self {
        Self {
            current_node: current_node.clone(),
            state_machine_store,
            write_router: IronClusterWriteRouter::new(current_node, raft),
            tasks: Arc::new(tokio::sync::Mutex::new(tasks)),
        }
    }

    // 读取当前节点本地已经 apply 的状态机数据。
    pub(crate) async fn local_state_machine_data(&self) -> S {
        self.state_machine_store.state_machine.lock().await.clone()
    }

    // 读取当前节点 ID。
    pub(crate) fn current_node_id(&self) -> u64 {
        self.current_node.node_id
    }

    // 读取当前节点已经解析完成的 TCP 地址。
    pub(crate) fn current_node_addr(&self) -> String {
        self.current_node.node_addr()
    }

    // 写入集群业务数据。
    pub(crate) async fn write_cluster_data(
        &self,
        request: S::WriteRequest,
    ) -> Result<S::WriteResponse, IronClusterWriteError> {
        self.write_router.write_cluster_data(request).await
    }

    // 等待集群后台任务结束或失败。
    pub(crate) async fn wait_shutdown(&self) -> Result<(), Box<dyn Error>> {
        let mut tasks = self.tasks.lock().await;
        match tasks.join_next().await {
            Some(Ok(())) => Err(IoError::other("Raft 后台任务已退出").into()),
            Some(Err(error)) => {
                Err(IoError::other(format!("Raft 后台任务执行失败: {error}")).into())
            }
            None => Err(IoError::other("Raft 后台任务集合为空").into()),
        }
    }
}
