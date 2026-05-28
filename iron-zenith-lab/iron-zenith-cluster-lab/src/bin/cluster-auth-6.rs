// 启动认证 Raft 验证节点。
mod support;

use iron_core_cluster::{IronCat, IronClusterManager, IronClusterWriteRequest, IronDog};
use support::cluster_logging::init_cluster_process_logging;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_cluster_process_logging()?;
    let cluster_manager = IronClusterManager::add_learner("127.0.0.1")?;

    let cluster_handler = cluster_manager.start().await?;
    cluster_handler
        .write_cluster_data(IronClusterWriteRequest::insert(IronCat {
            id: 301,
            name: "auth-cat-login".to_string(),
            age: "login-auth".to_string(),
        }))
        .await?;
    cluster_handler
        .write_cluster_data(IronClusterWriteRequest::insert(IronDog {
            id: 301,
            name: "auth-dog-login".to_string(),
            color: "green".to_string(),
        }))
        .await?;
    cluster_handler.wait_shutdown().await
}
