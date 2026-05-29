use std::fmt;
use std::io;
use std::sync::Arc;

use openraft::Raft;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

use crate::control_plane::IronClusterNode;
use crate::raft::IronTypeConfig;

// IronMesh 集群运行时。
#[derive(Clone)]
pub struct IronClusterRuntime {
    pub(crate) current_node: IronClusterNode, // 当前集群节点。
    pub(crate) raft: Raft<IronTypeConfig>,    // 当前节点 Raft 句柄。
    tasks: Arc<Mutex<JoinSet<()>>>,           // 当前节点后台任务集合。
}

impl IronClusterRuntime {
    // 创建集群运行时。
    pub fn new(
        current_node: IronClusterNode,
        raft: Raft<IronTypeConfig>,
        tasks: JoinSet<()>,
    ) -> Self {
        Self {
            current_node,
            raft,
            tasks: Arc::new(Mutex::new(tasks)),
        }
    }

    // 读取当前节点 ID。
    pub fn current_node_id(&self) -> u64 {
        self.current_node.node_id
    }

    // 读取当前节点已经解析完成的 TCP 地址。
    pub fn current_node_addr(&self) -> String {
        self.current_node.node_addr()
    }

    // 读取当前节点 Raft 句柄。
    pub fn raft(&self) -> &Raft<IronTypeConfig> {
        &self.raft
    }

    // 主动停止当前运行时中尚未退出的后台任务。
    pub async fn abort_tasks(&self) {
        let mut tasks = self.tasks.lock().await;
        tasks.abort_all();
    }

    // 等待关闭信号或后台任务退出。
    pub async fn wait_shutdown(&self) -> anyhow::Result<()> {
        let ctrl_c = tokio::signal::ctrl_c();
        tokio::pin!(ctrl_c);

        let mut tasks = self.tasks.lock().await;
        tokio::select! {
            result = &mut ctrl_c => {
                result?;
                Ok(())
            }
            task = tasks.join_next() => {
                match task {
                    Some(Ok(())) => Err(io::Error::other("Raft 后台任务已退出").into()),
                    Some(Err(error)) => Err(io::Error::other(format!("Raft 后台任务执行失败: {error}")).into()),
                    None => Err(io::Error::other("Raft 后台任务集合为空").into()),
                }
            }
        }
    }
}

impl fmt::Debug for IronClusterRuntime {
    // 格式化集群运行时调试信息。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IronClusterRuntime")
            .field("current_node", &self.current_node)
            .finish_non_exhaustive()
    }
}
