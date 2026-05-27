use iron_core_cluster::IronClusterDataCommand;
use iron_core_cluster::IronClusterHandle;
use std::time::Duration;

// 写入当前验证节点自己的集群业务数据。
pub(crate) async fn write_current_node_cluster_data(
    cluster_handle: &IronClusterHandle,
    node_id: u64,
    node_addr: &str,
    node_role: &str,
) {
    for write_index in 1..=3 {
        let key = format!("cluster/node/{node_id}/write/{write_index}");
        let value = format!("{node_id}|{node_addr}|{node_role}|write={write_index}");
        let result = cluster_handle
            .write_cluster_data(IronClusterDataCommand::Set { key, value })
            .await;

        if let Err(error) = result {
            eprintln!(
                "[Iron] [cluster-lab] 集群业务数据写入失败 node_id={node_id} write_index={write_index} error={error}"
            );
        }

        if write_index < 3 {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}
