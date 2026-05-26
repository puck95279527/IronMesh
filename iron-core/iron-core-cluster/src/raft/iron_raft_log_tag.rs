// 生成 Raft 节点日志标签。
pub(crate) fn node_tag(role: &str, node_id: u64) -> String {
    format!("[{role}={node_id}]")
}

// 生成当前 Raft 节点日志标签。
pub(crate) fn self_tag(node_id: u64) -> String {
    node_tag("self", node_id)
}

// 生成对方 Raft 节点日志标签。
pub(crate) fn peer_tag(node_id: u64) -> String {
    node_tag("peer", node_id)
}

// 生成多个 Raft 节点日志标签。
pub(crate) fn many_tag<I>(nodes: I) -> String
where
    I: IntoIterator<Item = u64>,
{
    let items = nodes
        .into_iter()
        .map(|node_id| node_id.to_string())
        .collect::<Vec<_>>()
        .join(";");

    format!("[many={items}]")
}

// 生成排除当前节点后的多个 Raft 节点日志标签。
pub(crate) fn other_tag<I>(self_node_id: u64, nodes: I) -> String
where
    I: IntoIterator<Item = u64>,
{
    many_tag(nodes.into_iter().filter(|node_id| *node_id != self_node_id))
}
