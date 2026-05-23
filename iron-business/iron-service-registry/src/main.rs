// 启动 IronMesh 注册中心服务。
fn main() -> Result<(), iron_core_cluster::IronClusterError> {
    iron_core_cluster::run_registry_cluster_from_local_toml()
}
