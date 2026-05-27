use std::error::Error;
use std::io::{Error as IoError, ErrorKind};
use std::sync::Arc;

use openraft::Raft;
use tokio::task::JoinSet;

use crate::api::iron_cluster_write_error::IronClusterWriteError;
use crate::control_plane::iron_cluster_write_router::IronClusterWriteRouter;
use crate::data_plane::iron_cluster_data_command::IronClusterDataCommand;
use crate::data_plane::iron_cluster_state::IronClusterState;
use crate::data_plane::iron_cluster_state_reader::IronClusterStateReader;
use crate::raft::control::iron_cluster_node::IronClusterNode;
use crate::raft::model::command::iron_cluster_write_response::IronClusterWriteResponse;
use crate::raft::model::iron_raft_type_config::IronRaftTypeConfig;
use crate::raft::storage::iron_raft_state_machine_store::IronRaftStateMachineStore;

// IronMesh 集群运行时。
pub(crate) struct IronClusterRuntime {
    // 当前集群节点信息，用于对外 API 记录操作来源。
    current_node: IronClusterNode,
    // 集群状态读取器。
    state_reader: IronClusterStateReader,
    // 集群写入路由器。
    write_router: IronClusterWriteRouter,
    // 当前集群节点托管的后台任务集合。
    tasks: Arc<tokio::sync::Mutex<JoinSet<()>>>,
}

impl IronClusterRuntime {
    // 创建集群运行时。
    pub(crate) fn new(
        current_node: IronClusterNode,
        raft: Raft<IronRaftTypeConfig>,
        state_machine_store: IronRaftStateMachineStore,
        tasks: JoinSet<()>,
    ) -> Self {
        Self {
            current_node: current_node.clone(),
            state_reader: IronClusterStateReader::new(state_machine_store),
            write_router: IronClusterWriteRouter::new(current_node, raft),
            tasks: Arc::new(tokio::sync::Mutex::new(tasks)),
        }
    }

    // 读取当前节点本地已经 apply 的状态机数据。
    pub(crate) async fn local_state_machine_data(&self) -> IronClusterState {
        self.state_reader.local_state_machine_data().await
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
        command: IronClusterDataCommand,
    ) -> Result<IronClusterWriteResponse, IronClusterWriteError> {
        self.write_router.write_cluster_data(command).await
    }

    // 等待后台任务退出，供实际服务进程显式阻塞使用。
    pub(crate) async fn wait_forever(&self) -> Result<(), Box<dyn Error>> {
        let mut tasks = self.tasks.lock().await;
        match tasks.join_next().await {
            Some(Ok(())) => Err(IoError::new(ErrorKind::Other, "Raft 后台任务已退出").into()),
            Some(Err(error)) => Err(IoError::new(
                ErrorKind::Other,
                format!("Raft 后台任务执行失败: {error}"),
            )
            .into()),
            None => Err(IoError::new(ErrorKind::Other, "Raft 后台任务集合为空").into()),
        }
    }
}
