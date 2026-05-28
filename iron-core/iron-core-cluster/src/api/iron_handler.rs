use crate::api::IronController;
use crate::control_plane::IronClusterRuntime;

// IronMesh 集群处理器。
#[derive(Clone, Debug)]
pub struct IronHandler {
    pub controller: IronController,          // 集群控制器。
    pub runtime: Option<IronClusterRuntime>, // 集群运行时。
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
        match &self.runtime {
            Some(runtime) => runtime.wait_shutdown().await,
            None => {
                tokio::signal::ctrl_c().await?;
                Ok(())
            }
        }
    }
}
