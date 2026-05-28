// 启动注册 Raft 验证节点。
mod support;

use iron_core_cluster::{IronCat, IronClusterManager, IronClusterWriteRequest};
use support::cluster_logging::init_cluster_process_logging;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_cluster_process_logging()?;
    let cluster_manager = IronClusterManager::add_voter(3)?;

    let cluster_handler = cluster_manager.start().await?;
    cluster_handler
        .write_cluster_data(IronClusterWriteRequest::insert(IronCat {
            id: cluster_handler.current_node_id() as u32,
            name: cluster_handler.current_node_addr(),
            age: "boot".to_string(),
        }))
        .await?;
    cluster_handler
        .write_cluster_data(IronClusterWriteRequest::delete_key::<IronCat>(
            cluster_handler.current_node_id() as u32,
        ))
        .await?;
    cluster_handler.wait_shutdown().await
}
