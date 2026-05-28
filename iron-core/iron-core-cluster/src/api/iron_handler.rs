use crate::api::IronController;

// IronMesh 集群处理器。
#[derive(Clone, Debug)]
pub struct IronHandler {
    pub controller: IronController, // 集群控制器。
}

impl IronHandler {
    // 等待进程关闭信号。
    pub async fn wait_shutdown(&self) -> anyhow::Result<()> {
        tokio::signal::ctrl_c().await?;
        Ok(())
    }
}
