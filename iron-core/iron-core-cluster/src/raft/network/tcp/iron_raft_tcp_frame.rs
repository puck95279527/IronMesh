use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

use crate::raft::iron_raft_constants::MAX_FRAME_BODY_LEN;
use crate::raft::iron_raft_constants::TCP_FRAME_READ_TIMEOUT;
use crate::raft::iron_raft_constants::TCP_FRAME_WRITE_TIMEOUT;

// IronMesh Raft TCP 帧编解码器。
pub struct IronRaftTcpFrame;

impl IronRaftTcpFrame {
    // 帧头长度，使用 4 字节长度前缀。
    const HEADER_LEN: usize = 4;

    // 从 TCP 连接读取一个 JSON 帧。
    pub async fn read_json<T>(stream: &mut tokio::net::TcpStream) -> Result<T, std::io::Error>
    where
        T: DeserializeOwned,
    {
        let mut header = [0_u8; Self::HEADER_LEN];
        // 等待下一帧头时不设置空闲超时，让集群内部长连接可以持续复用。
        stream.read_exact(&mut header).await?;

        let body_len = u32::from_be_bytes(header) as usize;
        if body_len > MAX_FRAME_BODY_LEN {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("raft tcp frame body too large: {body_len}"),
            ));
        }

        let mut body = vec![0_u8; body_len];
        Self::read_exact_with_timeout(stream, &mut body).await?;

        serde_json::from_slice::<T>(&body)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
    }

    // 向 TCP 连接写入一个 JSON 帧。
    pub async fn write_json<T>(
        stream: &mut tokio::net::TcpStream,
        value: &T,
    ) -> Result<(), std::io::Error>
    where
        T: Serialize,
    {
        let body = serde_json::to_vec(value)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        if body.len() > MAX_FRAME_BODY_LEN {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("raft tcp frame body too large: {}", body.len()),
            ));
        }

        let header = (body.len() as u32).to_be_bytes();
        Self::write_all_with_timeout(stream, &header).await?;
        Self::write_all_with_timeout(stream, &body).await?;
        Self::flush_with_timeout(stream).await?;
        Ok(())
    }

    // 判断连接是否已经关闭。
    pub fn is_connection_closed(error: &std::io::Error) -> bool {
        matches!(
            error.kind(),
            std::io::ErrorKind::UnexpectedEof
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::BrokenPipe
        )
    }

    // 在超时时间内读取指定长度的数据。
    async fn read_exact_with_timeout(
        stream: &mut tokio::net::TcpStream,
        buffer: &mut [u8],
    ) -> Result<(), std::io::Error> {
        match tokio::time::timeout(TCP_FRAME_READ_TIMEOUT, stream.read_exact(buffer)).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "raft tcp frame read timeout",
            )),
        }
    }

    // 在超时时间内写入指定数据。
    async fn write_all_with_timeout(
        stream: &mut tokio::net::TcpStream,
        buffer: &[u8],
    ) -> Result<(), std::io::Error> {
        match tokio::time::timeout(TCP_FRAME_WRITE_TIMEOUT, stream.write_all(buffer)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "raft tcp frame write timeout",
            )),
        }
    }

    // 在超时时间内刷新 TCP 写缓冲。
    async fn flush_with_timeout(stream: &mut tokio::net::TcpStream) -> Result<(), std::io::Error> {
        match tokio::time::timeout(TCP_FRAME_WRITE_TIMEOUT, stream.flush()).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "raft tcp frame flush timeout",
            )),
        }
    }
}
