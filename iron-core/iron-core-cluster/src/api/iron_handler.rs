use crate::api::IronController;
use crate::control_plane::IronClusterManager;
use crate::control_plane::IronClusterNodeRole;
use crate::control_plane::IronClusterRuntime;

// IronMesh 集群处理器。
#[derive(Clone, Debug)]
pub struct IronHandler {
    pub(crate) controller: IronController,          // 集群控制器。
    pub(crate) runtime: Option<IronClusterRuntime>, // 集群运行时。
}

impl IronHandler {
    // 启动集群处理器。
    pub async fn start(mut self) -> anyhow::Result<Self> {
        let runtime = self.controller.cluster_manager.start().await?;
        self.runtime = Some(runtime);
        Ok(self)
    }

    // 等待进程关闭信号。
    pub async fn wait_shutdown(&self) -> anyhow::Result<()> {
        let Some(runtime) = &self.runtime else {
            tokio::signal::ctrl_c().await?;
            return Ok(());
        };

        if self.controller.cluster_manager.current_node.node_role != IronClusterNodeRole::Learner {
            return runtime.wait_shutdown().await;
        }

        let advertise_node_ip = self.controller.cluster_manager.current_node.node_ip.clone();
        let mut runtime = runtime.clone();

        loop {
            match runtime.wait_shutdown().await {
                Ok(()) => return Ok(()),
                Err(error) => {
                    tracing::warn!(
                        %error,
                        "[Iron] [cluster] learner 运行时已退出，准备生成新节点 ID 并重新加入集群"
                    );
                    runtime.abort_tasks().await;

                    let cluster_manager =
                        IronClusterManager::add_learner(advertise_node_ip.clone())?;
                    runtime = cluster_manager.start().await?;

                    tracing::info!(
                        node_id = runtime.current_node_id(),
                        node_addr = %runtime.current_node_addr(),
                        "[Iron] [cluster] learner 运行时已重建"
                    );
                }
            }
        }
    }
}
