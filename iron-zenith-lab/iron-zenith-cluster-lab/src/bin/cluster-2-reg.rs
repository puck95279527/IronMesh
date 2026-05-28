mod support;

use iron_core_cluster::api::IronController;
use support::cluster_logging::init_cluster_process_logging;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_cluster_process_logging();
    let handler = IronController::add_voter(2)?.start().await?;
    handler.wait_shutdown().await?;
    Ok(())
}
