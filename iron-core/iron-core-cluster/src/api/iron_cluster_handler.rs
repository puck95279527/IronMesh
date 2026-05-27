use std::error::Error;

use crate::api::iron_cluster_write_error::IronClusterWriteError;
use crate::control_plane::iron_cluster_runtime::IronClusterRuntime;
use crate::data_plane::iron_cluster_data_command::IronClusterDataCommand;
use crate::data_plane::iron_cluster_state::IronClusterState;
use crate::raft::model::command::iron_cluster_write_response::IronClusterWriteResponse;

// 1. IronMesh 集群运行处理器，是外部调用者操作已启动节点的公开入口。
pub struct IronClusterHandler {
    // 2. 集群内部运行时，封装控制面和数据面运行期组件。
    pub(crate) inner: IronClusterRuntime,
}

impl IronClusterHandler {
    // 3. 读取当前节点本地已经 apply 的状态机数据。
    pub async fn local_state_machine_data(&self) -> IronClusterState {
        self.inner.local_state_machine_data().await
    }

    // 4. 读取当前节点 ID。
    pub fn current_node_id(&self) -> u64 {
        self.inner.current_node_id()
    }

    // 5. 读取当前节点已经解析完成的 TCP 地址。
    pub fn current_node_addr(&self) -> String {
        self.inner.current_node_addr()
    }

    // 6. 写入集群业务数据。
    pub async fn write_cluster_data(
        &self,
        command: IronClusterDataCommand,
    ) -> Result<IronClusterWriteResponse, IronClusterWriteError> {
        self.inner.write_cluster_data(command).await
    }

    // 7. 等待集群后台任务结束或失败，供实际服务进程显式阻塞使用。
    pub async fn wait_shutdown(&self) -> Result<(), Box<dyn Error>> {
        self.inner.wait_shutdown().await
    }
}
