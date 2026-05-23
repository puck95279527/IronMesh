// 集群 Raft HTTP 网络实现。

use crate::http::IRON_CLUSTER_TOKEN_HEADER;
use crate::model::IronRaftNetwork;
use crate::model::IronRaftNetworkFactory;
use crate::model::IronRaftSnapshot;
use crate::model::IronRaftTypeConfig;
use openraft::RaftNetwork;
use openraft::RaftNetworkFactory;
use openraft::Vote;
use openraft::error::Fatal;
use openraft::error::RPCError;
use openraft::error::RaftError;
use openraft::error::ReplicationClosed;
use openraft::error::StreamingError;
use openraft::error::Unreachable;
use openraft::impls::BasicNode;
use openraft::network::RPCOption;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::SnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use std::future::Future;

impl RaftNetworkFactory<IronRaftTypeConfig> for IronRaftNetworkFactory {
    type Network = IronRaftNetwork;

    // 创建指向目标节点的 Raft 网络客户端。
    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        IronRaftNetwork {
            target,
            target_node: node.clone(),
            cluster_token: self.cluster_token.clone(),
            http_client: self.http_client.clone(),
        }
    }
}

impl RaftNetwork<IronRaftTypeConfig> for IronRaftNetwork {
    // 发送 Raft AppendEntries RPC。
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<IronRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        self.post_json("append", &rpc).await
    }

    // 发送 Raft RequestVote RPC。
    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        self.post_json("vote", &rpc).await
    }

    // 发送 Raft 完整快照 RPC。
    async fn full_snapshot(
        &mut self,
        vote: Vote<u64>,
        snapshot: IronRaftSnapshot,
        _cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<u64>, StreamingError<IronRaftTypeConfig, Fatal<u64>>> {
        let _ = (vote, snapshot);
        let error = std::io::Error::new(std::io::ErrorKind::Unsupported, "暂不传输 Raft 快照");
        Err(StreamingError::Unreachable(Unreachable::new(&error)))
    }
}

impl IronRaftNetwork {
    // 向目标节点发送 JSON RPC。
    async fn post_json<TReq, TResp>(
        &self,
        action: &str,
        request: &TReq,
    ) -> Result<TResp, RPCError<u64, BasicNode, RaftError<u64>>>
    where
        TReq: serde::Serialize + ?Sized,
        TResp: for<'de> serde::Deserialize<'de>,
    {
        let url = format!(
            "{}/iron/cluster/raft/{action}",
            self.target_node.addr.trim_end_matches('/')
        );
        let response = self
            .http_client
            .post(url)
            .header(IRON_CLUSTER_TOKEN_HEADER, &self.cluster_token)
            .json(request)
            .send()
            .await
            .map_err(|error| RPCError::Unreachable(Unreachable::new(&error)))?
            .error_for_status()
            .map_err(|error| RPCError::Unreachable(Unreachable::new(&error)))?
            .json::<TResp>()
            .await
            .map_err(|error| RPCError::Unreachable(Unreachable::new(&error)))?;

        Ok(response)
    }
}
