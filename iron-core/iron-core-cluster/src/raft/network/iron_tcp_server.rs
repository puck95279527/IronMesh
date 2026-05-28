use std::io;

use futures_util::SinkExt;
use futures_util::StreamExt;
use openraft::Raft;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::raft::IronTypeConfig;
use crate::raft::network::protocol::IronTcpFrameCodec;
use crate::raft::network::protocol::IronTcpRequest;
use crate::raft::network::protocol::IronTcpResponse;

// IronMesh Raft TCP 服务端。
#[derive(Clone)]
pub struct IronTcpServer {
    pub raft: Raft<IronTypeConfig>, // Raft 节点句柄。
}

impl IronTcpServer {
    // 创建 TCP 服务端。
    pub fn new(raft: Raft<IronTypeConfig>) -> Self {
        Self { raft }
    }

    // 启动 TCP 服务端并持续处理连接。
    pub async fn serve(self, listener: TcpListener) -> Result<(), io::Error> {
        loop {
            let (stream, _) = listener.accept().await?;
            let raft = self.raft.clone();

            tokio::spawn(async move {
                let _ = Self::handle_connection(raft, stream).await;
            });
        }
    }

    // 在单个连接上循环处理多个请求。
    async fn handle_connection(
        raft: Raft<IronTypeConfig>,
        stream: TcpStream,
    ) -> Result<(), io::Error> {
        let mut framed = Framed::new(stream, IronTcpFrameCodec::default());

        while let Some(frame) = framed.next().await {
            let request = IronTcpFrameCodec::decode_request(frame?)?;
            let response = Self::handle_request(raft.clone(), request).await?;
            let response = IronTcpFrameCodec::encode_response(&response)?;
            framed.send(response).await?;
        }

        Ok(())
    }

    // 处理单个 TCP 请求。
    async fn handle_request(
        raft: Raft<IronTypeConfig>,
        request: IronTcpRequest,
    ) -> Result<IronTcpResponse, io::Error> {
        match request {
            IronTcpRequest::AppendEntries(rpc) => Ok(IronTcpResponse::AppendEntries(
                raft.append_entries(rpc).await,
            )),
            IronTcpRequest::Vote(rpc) => Ok(IronTcpResponse::Vote(raft.vote(rpc).await)),
            IronTcpRequest::FullSnapshot { .. } => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "full snapshot tcp request is not implemented",
            )),
        }
    }
}
