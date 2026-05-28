mod support;

use iron_core_cluster::api::IronController;
use support::cluster_logging::init_cluster_process_logging;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_cluster_process_logging();
    let _handler = IronController::add_voter(3)?;
    Ok(())
}
