// 生成 Raft 节点日志标签。
pub(crate) fn node_tag(role: &str, node_id: u64, node_name: &str) -> String {
    format!("[{role}={node_id},{node_name}]")
}

// 生成当前 Raft 节点日志标签。
pub(crate) fn self_tag(node_id: u64, node_name: &str) -> String {
    node_tag("self", node_id, node_name)
}

// 生成对方 Raft 节点日志标签。
pub(crate) fn peer_tag(node_id: u64, node_name: &str) -> String {
    node_tag("peer", node_id, node_name)
}

// 生成多个 Raft 节点日志标签。
pub(crate) fn many_tag<I, S>(nodes: I) -> String
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
