use std::error::Error;

use crate::api::iron_cluster_write_error::IronClusterWriteError;
use crate::contract::iron_cluster_entity_model::IronClusterEntityModel;
use crate::contract::iron_cluster_entity_model_source_node_tagged::IronClusterEntityModelSourceNodeTagged;
use crate::control_plane::iron_cluster_runtime::IronClusterRuntime;
use crate::data_plane::iron_cluster_entity::IronClusterEntity;
use crate::data_plane::iron_cluster_state::IronClusterState;
use crate::raft::model::command::iron_cluster_write_response::IronClusterWriteResponse;
use crate::raft::storage::iron_raft_state_machine_container::IronRaftStateMachineContainer;
use crate::raft::storage::iron_raft_state_machine_data::IronRaftStateMachineData;

// IronMesh 集群运行处理器，是外部调用者操作已启动节点的公开入口。
pub struct IronClusterHandler<D = IronClusterState>
where
    D: IronRaftStateMachineData,
{
    // 集群内部运行时，真实状态机使用 Raft 总容器承载。
    pub(crate) inner: IronClusterRuntime<IronRaftStateMachineContainer<D>>,
}

impl<D> IronClusterHandler<D>
where
    D: IronRaftStateMachineData,
{
    // 读取当前节点本地已经 apply 的数据面状态。
    pub async fn local_state_machine_data(&self) -> D {
        self.inner.local_state_machine_data().await.cluster_state
    }

    // 读取当前节点 ID。
    pub fn current_node_id(&self) -> u64 {
        self.inner.current_node_id()
    }

    // 读取当前节点已经解析完成的 TCP 地址。
    pub fn current_node_addr(&self) -> String {
        self.inner.current_node_addr()
    }

    // 等待集群后台任务结束或失败，供实际服务进程显式阻塞使用。
    pub async fn wait_shutdown(&self) -> Result<(), Box<dyn Error>> {
        self.inner.wait_shutdown().await
    }
}

impl IronClusterHandler<IronClusterState> {
    // 新增集群业务实体，并为支持来源节点索引的实体打标记。
    pub async fn insert_cluster_data<T>(
        &self,
        value: T,
    ) -> Result<IronClusterWriteResponse<IronClusterEntity>, IronClusterWriteError>
    where
        T: IronClusterEntityModel
            + IronClusterEntityModelSourceNodeTagged
            + Into<IronClusterEntity>,
    {
        self.inner.insert_cluster_data(value).await
    }

    // 修改集群业务实体，并为支持来源节点索引的实体刷新标记。
    pub async fn update_cluster_data<T>(
        &self,
        value: T,
    ) -> Result<IronClusterWriteResponse<IronClusterEntity>, IronClusterWriteError>
    where
        T: IronClusterEntityModel
            + IronClusterEntityModelSourceNodeTagged
            + Into<IronClusterEntity>,
    {
        self.inner.update_cluster_data(value).await
    }

    // 删除集群业务实体，并移除对应来源节点索引。
    pub async fn delete_cluster_data<T>(
        &self,
        value: T,
    ) -> Result<IronClusterWriteResponse<IronClusterEntity>, IronClusterWriteError>
    where
        T: IronClusterEntityModel
            + IronClusterEntityModelSourceNodeTagged
            + Into<IronClusterEntity>,
    {
        self.inner.delete_cluster_data(value).await
    }

    // 按实体键删除集群业务实体，并移除对应来源节点索引。
    pub async fn delete_cluster_data_key<T>(
        &self,
        key: T::Key,
    ) -> Result<IronClusterWriteResponse<IronClusterEntity>, IronClusterWriteError>
    where
        T: IronClusterEntityModel
            + IronClusterEntityModelSourceNodeTagged
            + Into<IronClusterEntity>,
    {
        self.inner.delete_cluster_data_key::<T>(key).await
    }
}
