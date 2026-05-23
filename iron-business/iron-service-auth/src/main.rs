// 启动 IronMesh 登录注册服务。
fn main() -> Result<(), iron_core_cluster::IronClusterError> {
    iron_core_cluster::run_cluster_service_from_local_toml(
        iron_core_cluster::IronClusterServiceKind::Auth,
    )
}
