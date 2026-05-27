// 启动注册 Raft 验证节点。
mod support;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use iron_core_cluster::IronClusterManager;
    use support::cluster_data_writer::write_current_node_cluster_data;
    use support::cluster_logging::init_cluster_process_logging;

    init_cluster_process_logging()?;
    let cluster_manager = IronClusterManager::add_voter(2)?;

    let cluster_handle = cluster_manager.start().await?;
    write_current_node_cluster_data(&cluster_handle, 2, "127.0.0.1:5002", "boot").await;
    cluster_handle.wait_forever().await
}
