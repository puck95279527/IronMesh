use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::prelude::*;

// 初始化集群日志，只保留本库目标并统一显示中文前缀。
pub fn init_cluster_logging() {
    let filter = Targets::new()
        .with_target("iron_core_cluster_v2", LevelFilter::INFO)
        .with_default(LevelFilter::OFF);

    let _ = tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .compact()
                .with_target(false),
        )
        .with(filter)
        .try_init();
}

// 生成节点标签。
pub fn node_tag(role: &str, node_id: u64, node_name: &str) -> String {
    format!("[{role}={node_id},{node_name}]")
}

// 生成当前节点标签。
pub fn self_tag(node_id: u64, node_name: &str) -> String {
    node_tag("self", node_id, node_name)
}

// 生成对方节点标签。
pub fn peer_tag(node_id: u64, node_name: &str) -> String {
    node_tag("peer", node_id, node_name)
}

// 生成多个节点标签。
pub fn many_tag<I, S>(nodes: I) -> String
where
    I: IntoIterator<Item = (u64, S)>,
    S: AsRef<str>,
{
    let items = nodes
        .into_iter()
        .map(|(node_id, node_name)| format!("{node_id},{}", node_name.as_ref()))
        .collect::<Vec<_>>()
        .join(";");

    format!("[many={items}]")
}
