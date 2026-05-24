use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

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
        stream.read_exact(&mut header).await?;

        let body_len = u32::from_be_bytes(header) as usize;
        let mut body = vec![0_u8; body_len];
        stream.read_exact(&mut body).await?;

        serde_json::from_slice::<T>(&body).map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
    }

    // 向 TCP 连接写入一个 JSON 帧。
    pub async fn write_json<T>(stream: &mut tokio::net::TcpStream, value: &T) -> Result<(), std::io::Error>
    where
        T: Serialize,
    {
        let body =
            serde_json::to_vec(value).map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        let header = (body.len() as u32).to_be_bytes();

        stream.write_all(&header).await?;
        stream.write_all(&body).await?;
        stream.flush().await?;
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
}
