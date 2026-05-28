// 启动斗地主 Raft 验证节点。
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
            id: 401,
            name: "game-cat-table".to_string(),
            age: "table-game".to_string(),
        })
        .await?;
    cluster_handler
        .insert_cluster_data(IronDog {
            id: 401,
            name: "game-dog-table".to_string(),
            color: "red".to_string(),
        })
        .await?;
    cluster_handler.wait_shutdown().await
}
