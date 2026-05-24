// 启动第二个注册 Raft 节点。
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    iron_core_cluster_v2::raft::start_iron_raft_node(2, "127.0.0.1:5002".to_string()).await
}
