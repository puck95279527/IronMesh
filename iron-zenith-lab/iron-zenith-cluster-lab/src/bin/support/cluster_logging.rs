use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::prelude::*;

// 初始化集群验证进程日志。
pub(crate) fn init_cluster_process_logging() -> Result<(), Box<dyn std::error::Error>> {
    let filter = Targets::new()
        .with_target("iron_core_cluster", LevelFilter::DEBUG)
        .with_target("iron_zenith_cluster_lab", LevelFilter::DEBUG)
        .with_default(LevelFilter::OFF);

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .compact()
                .with_target(false),
        )
        .with(filter)
        .try_init()?;

    Ok(())
}
