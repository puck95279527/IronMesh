// 启动注册 Raft 验证节点。
mod support;

use iron_core_cluster::{IronCat, IronClusterEntity, IronClusterManager, IronClusterWriteRequest};
use support::cluster_logging::init_cluster_process_logging;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_cluster_process_logging()?;
    let cluster_manager = IronClusterManager::add_voter(2)?;

    let cluster_handler = cluster_manager.start().await?;
    cluster_handler
        .write_cluster_data(IronClusterWriteRequest::Insert(IronClusterEntity::Cat(
            IronCat {
                id: cluster_handler.current_node_id(),
                name: cluster_handler.current_node_addr(),
                age: "boot".to_string(),
            },
        )))
        .await?;
    cluster_handler
        .write_cluster_data(IronClusterWriteRequest::Delete(IronClusterEntity::Cat(
            IronCat {
                id: cluster_handler.current_node_id(),
                name: cluster_handler.current_node_addr(),
                age: "delete".to_string(),
            },
        )))
        .await?;
    cluster_handler.wait_shutdown().await
}
