// 启动注册 Raft 验证节点。
mod support;

use iron_core_cluster::{IronCat, IronClusterManager, IronDog};
use support::cluster_logging::init_cluster_process_logging;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_cluster_process_logging()?;
    let cluster_manager = IronClusterManager::add_voter(2)?;

    let cluster_handler = cluster_manager.start().await?;
    cluster_handler
        .insert_cluster_data(IronCat {
            id: 102,
            name: "registry-cat-beta".to_string(),
            age: "peer-registry".to_string(),
        })
        .await?;
    cluster_handler
        .insert_cluster_data(IronDog {
            id: 102,
            name: "registry-dog-beta".to_string(),
            color: "gray".to_string(),
        })
        .await?;
    cluster_handler.wait_shutdown().await
}
