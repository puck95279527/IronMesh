use crate::api::IronHandler;

// IronMesh 集群控制器。
#[derive(Clone, Debug, Default)]
pub struct IronController;

impl IronController {
    // 添加投票节点。
    pub fn add_voter(_node_id: u64) -> anyhow::Result<IronHandler> {
        Ok(IronHandler::default())
    }

    // 添加学习节点。
    pub fn add_learner(_advertise_node_ip: impl Into<String>) -> anyhow::Result<IronHandler> {
        Ok(IronHandler::default())
    }
}
