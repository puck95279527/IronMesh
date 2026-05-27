// 启动认证 Raft 验证节点。
mod support;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use iron_core_cluster::control_plane::iron_raft_cluster_manager::IronRaftClusterManager;
    use support::cluster_data_writer::write_current_node_cluster_data;
    use support::cluster_logging::init_cluster_process_logging;

    init_cluster_process_logging()?;
    let cluster_manager = IronRaftClusterManager::add_learner("127.0.0.1")?;

    let cluster_handle = cluster_manager.start().await?;
    let node_id = cluster_handle.current_node_id();
    let node_addr = cluster_handle.current_node_addr();
    write_current_node_cluster_data(&cluster_handle, node_id, &node_addr, "normal").await;
    cluster_handle.wait_forever().await
}
