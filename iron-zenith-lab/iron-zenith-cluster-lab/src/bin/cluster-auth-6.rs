// 启动认证 Raft 验证节点。
mod support;

use iron_core_cluster::{IronCat, IronClusterManager, IronClusterState, IronClusterWriteRequest};
use support::cluster_logging::init_cluster_process_logging;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_cluster_process_logging()?;
    let cluster_manager = IronClusterManager::<IronClusterState>::add_learner("127.0.0.1")?;

    let cluster_handler = cluster_manager.start().await?;
    cluster_handler
        .write_cluster_data(IronClusterWriteRequest::<u64, IronCat>::Insert(IronCat {
            id: cluster_handler.current_node_id(),
            name: cluster_handler.current_node_addr(),
            age: "normal".to_string(),
        }))
        .await?;
    cluster_handler.wait_shutdown().await
}
