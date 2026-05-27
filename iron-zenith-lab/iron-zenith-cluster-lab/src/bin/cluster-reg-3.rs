// 启动注册 Raft 验证节点。
mod support;

use iron_core_cluster::{IronClusterDataCommand, IronClusterManager};
use support::cluster_logging::init_cluster_process_logging;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_cluster_process_logging()?;
    let cluster_manager = IronClusterManager::add_voter(3)?;

    let cluster_handler = cluster_manager.start().await?;
    cluster_handler
        .write_cluster_data(IronClusterDataCommand::Set {
            key: format!("cluster/node/{}", cluster_handler.current_node_id()),
            value: format!(
                "{}|{}|boot",
                cluster_handler.current_node_id(),
                cluster_handler.current_node_addr()
            ),
        })
        .await?;
    cluster_handler.wait_shutdown().await
}
