// 启动注册 Raft 验证节点。
mod support;

use iron_core_cluster::{IronCat, IronClusterManager, IronClusterWriteRequest, IronDog};
use support::cluster_logging::init_cluster_process_logging;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_cluster_process_logging()?;
    let cluster_manager = IronClusterManager::add_voter(1)?;

    let cluster_handler = cluster_manager.start().await?;
    cluster_handler
        .write_cluster_data(IronClusterWriteRequest::insert(IronCat {
            id: 101,
            name: "registry-cat-alpha".to_string(),
            age: "seed-registry".to_string(),
        }))
        .await?;
    cluster_handler
        .write_cluster_data(IronClusterWriteRequest::insert(IronDog {
            id: 101,
            name: "registry-dog-alpha".to_string(),
            color: "silver".to_string(),
        }))
        .await?;
    cluster_handler.wait_shutdown().await
}
