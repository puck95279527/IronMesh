// 启动 IronMesh 网关服务。
fn main() -> Result<(), iron_core_cluster::IronClusterError> {
    iron_core_cluster::run_cluster_service_from_local_toml(
        iron_core_cluster::IronClusterServiceKind::Gateway,
    )
}
