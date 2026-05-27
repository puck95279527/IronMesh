use std::error::Error;
use std::fmt;

// IronMesh 集群写入错误。
#[derive(Debug)]
pub enum IronClusterWriteError {
    // 当前集群暂时没有 leader。
    NoLeader,
    // 当前节点知道 leader 标识，但成员关系中缺少 leader 节点地址。
    LeaderNodeMissing {
        leader_id: u64, // 缺少地址的 leader 节点标识。
    },
    // leader 本地写入失败。
    LocalWrite(
        Box<
            openraft::error::RaftError<
                u64,
                openraft::error::ClientWriteError<u64, openraft::BasicNode>,
            >,
        >,
    ),
    // 非 leader 向 leader 转发写入超时。
    ForwardWriteTimeout {
        leader_id: u64,      // 转发目标 leader 节点标识。
        leader_addr: String, // 转发目标 leader TCP 地址。
        message: String,     // 底层超时错误信息。
    },
    // 非 leader 向 leader 转发写入时网络失败。
    ForwardWriteNetwork {
        leader_id: u64,      // 转发目标 leader 节点标识。
        leader_addr: String, // 转发目标 leader TCP 地址。
        message: String,     // 底层网络错误信息。
    },
    // leader 收到转发写入后拒绝执行。
    ForwardWriteRejected {
        leader_id: u64,      // 转发目标 leader 节点标识。
        leader_addr: String, // 转发目标 leader TCP 地址。
        message: String,     // 远端 leader 返回的拒绝原因。
    },
    // 非 leader 收到的转发写入响应协议不符合预期。
    ForwardWriteProtocol {
        leader_id: u64,      // 转发目标 leader 节点标识。
        leader_addr: String, // 转发目标 leader TCP 地址。
        message: String,     // 协议或数据格式错误信息。
    },
}

impl fmt::Display for IronClusterWriteError {
    // 格式化集群写入错误。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoLeader => write!(formatter, "当前集群暂时没有 leader"),
            Self::LeaderNodeMissing { leader_id } => {
                write!(
                    formatter,
                    "当前集群缺少 leader 节点地址 leader_id={leader_id}"
                )
            }
            Self::LocalWrite(error) => write!(formatter, "leader 本地写入失败: {error}"),
            Self::ForwardWriteTimeout {
                leader_id,
                leader_addr,
                message,
            } => write!(
                formatter,
                "转发 leader 写入超时 leader_id={leader_id}, leader_addr={leader_addr}: {message}"
            ),
            Self::ForwardWriteNetwork {
                leader_id,
                leader_addr,
                message,
            } => write!(
                formatter,
                "转发 leader 写入网络失败 leader_id={leader_id}, leader_addr={leader_addr}: {message}"
            ),
            Self::ForwardWriteRejected {
                leader_id,
                leader_addr,
                message,
            } => write!(
                formatter,
                "leader 拒绝转发写入 leader_id={leader_id}, leader_addr={leader_addr}: {message}"
            ),
            Self::ForwardWriteProtocol {
                leader_id,
                leader_addr,
                message,
            } => write!(
                formatter,
                "转发 leader 写入响应协议异常 leader_id={leader_id}, leader_addr={leader_addr}: {message}"
            ),
        }
    }
}

impl Error for IronClusterWriteError {
    // 返回底层错误来源。
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::LocalWrite(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}
