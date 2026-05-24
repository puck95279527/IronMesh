// 启动第一个注册 Raft 节点。
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    iron_core_cluster_v2::raft::start_iron_raft_node(1, "127.0.0.1:5001".to_string(), 6001).await
}
