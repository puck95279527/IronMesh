// 启动第三个注册 Raft 节点。
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    iron_core_cluster_v2::raft::start_iron_raft_node(3, "127.0.0.1:5003".to_string(), 6003).await
}
