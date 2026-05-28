// 启动网关 Raft 验证节点。
mod support;

use iron_core_cluster::{IronCat, IronClusterManager, IronDog};
use support::cluster_logging::init_cluster_process_logging;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_cluster_process_logging()?;
    let cluster_manager = IronClusterManager::add_learner("127.0.0.1")?;

    let cluster_handler = cluster_manager.start().await?;
    cluster_handler
        .insert_cluster_data(IronCat {
            id: 201,
            name: "gateway-cat-edge".to_string(),
            age: "edge-gateway".to_string(),
        })
        .await?;
    cluster_handler
        .insert_cluster_data(IronDog {
            id: 201,
            name: "gateway-dog-edge".to_string(),
            color: "blue".to_string(),
        })
        .await?;
    cluster_handler.wait_shutdown().await
}
