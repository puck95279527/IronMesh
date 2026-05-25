use std::collections::BTreeMap;

use crate::cluster_data::iron_cluster_data_command::IronClusterDataCommand;
use crate::raft::model::command::iron_raft_response::IronRaftResponse;

// IronMesh 集群业务数据模型。
#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct IronClusterData {
    pub(crate) values: BTreeMap<String, String>, // 集群业务数据的最小键值存储。
}

impl IronClusterData {
    // 应用集群数据写命令。
    pub(crate) fn apply_command(&mut self, command: IronClusterDataCommand) -> IronRaftResponse {
        match command {
            IronClusterDataCommand::Set { key, value } => {
                self.values.insert(key, value.clone());
                IronRaftResponse { value: Some(value) }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // 验证集群数据写命令可以写入数据。
    #[test]
    fn apply_command_sets_value() {
        let mut cluster_data = IronClusterData::default();

        let response = cluster_data.apply_command(IronClusterDataCommand::Set {
            key: "service/auth".to_string(),
            value: "127.0.0.1:9001".to_string(),
        });

        assert_eq!(response.value, Some("127.0.0.1:9001".to_string()));
        assert_eq!(
            cluster_data.values.get("service/auth"),
            Some(&"127.0.0.1:9001".to_string())
        );
    }

    // 验证同一个键再次写入时会覆盖旧值。
    #[test]
    fn apply_command_overwrites_value() {
        let mut cluster_data = IronClusterData::default();

        cluster_data.apply_command(IronClusterDataCommand::Set {
            key: "service/auth".to_string(),
            value: "127.0.0.1:9001".to_string(),
        });
        let response = cluster_data.apply_command(IronClusterDataCommand::Set {
            key: "service/auth".to_string(),
            value: "127.0.0.1:9002".to_string(),
        });

        assert_eq!(response.value, Some("127.0.0.1:9002".to_string()));
        assert_eq!(
            cluster_data.values.get("service/auth"),
            Some(&"127.0.0.1:9002".to_string())
        );
    }
}
