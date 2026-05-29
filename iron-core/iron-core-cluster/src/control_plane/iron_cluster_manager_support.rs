use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io;

use toml::Value;

use crate::control_plane::IronClusterNode;
use crate::control_plane::IronClusterNodeRole;

// IronMesh 集群管理器辅助能力。
#[derive(Clone, Debug, Default)]
pub struct IronClusterManagerSupport;

impl IronClusterManagerSupport {
    // 从 cluster-boot.toml 读取集群启动节点配置。
    pub fn load_cluster_boot() -> anyhow::Result<BTreeMap<u64, IronClusterNode>> {
        let config_path = env::current_exe()?
            .parent()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "无法找到当前可执行文件目录"))?
            .join("cluster-boot.toml");
        let content = fs::read_to_string(&config_path)?;
        let value: Value = toml::from_str(&content)?;
        let boot_nodes_value = value
            .get("IronClusterNode")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{} 缺少 IronClusterNode 数组", config_path.display()),
                )
            })?;

        let mut boot_nodes = BTreeMap::new();
        for item in boot_nodes_value {
            let table = item.as_table().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "{} 中的 IronClusterNode 条目必须是表",
                        config_path.display()
                    ),
                )
            })?;

            let node_id = table
                .get("node_id")
                .and_then(Value::as_integer)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "{} 中的 IronClusterNode 条目缺少 node_id",
                            config_path.display()
                        ),
                    )
                })?;
            if node_id < 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "集群节点 node_id 不能为负数",
                )
                .into());
            }

            let node_ip = table
                .get("advertise_node_ip")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "{} 中的 IronClusterNode 条目缺少 advertise_node_ip",
                            config_path.display()
                        ),
                    )
                })?
                .to_string();

            let node_port = table
                .get("node_port")
                .and_then(Value::as_integer)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "{} 中的 IronClusterNode 条目缺少 node_port",
                            config_path.display()
                        ),
                    )
                })?;
            if !(0..=u16::MAX as i64).contains(&node_port) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "集群节点 node_port 超出 u16 范围",
                )
                .into());
            }

            let http_debug_addr = table
                .get("http_debug_addr")
                .and_then(Value::as_str)
                .map(|value| value.to_string());
            let is_boot_node = table
                .get("is_boot_node")
                .and_then(Value::as_bool)
                .unwrap_or(false);

            let node = IronClusterNode {
                node_id: node_id as u64,
                node_ip,
                node_port: Some(node_port as u16),
                http_debug_addr,
                is_boot_node,
                node_role: IronClusterNodeRole::Voter,
            };

            if boot_nodes.insert(node.node_id, node).is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "cluster-boot.toml 中存在重复的 node_id",
                )
                .into());
            }
        }

        if boot_nodes.is_empty() {
            return Err(
                io::Error::new(io::ErrorKind::InvalidData, "cluster-boot.toml 不能为空").into(),
            );
        }

        Ok(boot_nodes)
    }
}
