// 启动斗地主 Raft 验证节点。
mod support;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use iron_core_cluster::IronClusterManager;
    use support::cluster_data_writer::write_current_node_cluster_data;
    use support::cluster_logging::init_cluster_process_logging;

    init_cluster_process_logging()?;
    let cluster_manager = IronClusterManager::add_learner("127.0.0.1")?;

    let cluster_handler = cluster_manager.start().await?;
    let node_id = cluster_handler.current_node_id();
    let node_addr = cluster_handler.current_node_addr();
    write_current_node_cluster_data(&cluster_handler, node_id, &node_addr, "normal").await;
    cluster_handler.wait_forever().await
}
