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
