// 复制集群种子配置到斗地主服务运行目录。
fn main() {
    iron_core_cluster::copy_cluster_seed_config_to_runtime_dir().expect("复制集群种子配置失败");
}
