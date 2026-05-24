// 集群 TCP 控制面模块。

use crate::model::ClusterError;
use crate::model::ClusterFrameHeader;
use crate::model::ClusterFrameKind;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

// TCP 帧头字节长度。
const IRON_CLUSTER_FRAME_HEADER_LEN: usize = 6;

// 读取一个 JSON TCP 帧。
pub(crate) async fn read_json_frame<R, T>(
    reader: &mut R,
) -> Result<(ClusterFrameKind, T), ClusterError>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let (kind, body) = read_frame(reader).await?;
    let value = serde_json::from_slice(&body)?;
    Ok((kind, value))
}

// 写入一个 JSON TCP 帧。
pub(crate) async fn write_json_frame<W, T>(
    writer: &mut W,
    kind: ClusterFrameKind,
    value: &T,
) -> Result<(), ClusterError>
where
    W: AsyncWrite + Unpin,
    T: Serialize + ?Sized,
{
    let body = serde_json::to_vec(value)?;
    write_frame(writer, kind, &body).await
}

// 读取一个原始 TCP 帧。
pub(crate) async fn read_frame<R>(
    reader: &mut R,
) -> Result<(ClusterFrameKind, Vec<u8>), ClusterError>
where
    R: AsyncRead + Unpin,
{
    let mut header_bytes = [0_u8; IRON_CLUSTER_FRAME_HEADER_LEN];
    reader.read_exact(&mut header_bytes).await?;

    let kind_code = u16::from_be_bytes([header_bytes[0], header_bytes[1]]);
    let Some(kind) = ClusterFrameKind::from_code(kind_code) else {
        return Err(ClusterError::InvalidFrameKind { kind: kind_code });
    };
    let body_len = u32::from_be_bytes([
        header_bytes[2],
        header_bytes[3],
        header_bytes[4],
        header_bytes[5],
    ]);
    let header = ClusterFrameHeader { kind, body_len };
    let mut body = vec![0_u8; header.body_len as usize];

    reader.read_exact(&mut body).await?;
    Ok((header.kind, body))
}

// 写入一个原始 TCP 帧。
pub(crate) async fn write_frame<W>(
    writer: &mut W,
    kind: ClusterFrameKind,
    body: &[u8],
) -> Result<(), ClusterError>
where
    W: AsyncWrite + Unpin,
{
    let header = ClusterFrameHeader {
        kind,
        body_len: body.len() as u32,
    };
    let mut header_bytes = [0_u8; IRON_CLUSTER_FRAME_HEADER_LEN];

    header_bytes[0..2].copy_from_slice(&header.kind.code().to_be_bytes());
    header_bytes[2..6].copy_from_slice(&header.body_len.to_be_bytes());
    writer.write_all(&header_bytes).await?;
    writer.write_all(body).await?;
    writer.flush().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // 验证 TCP JSON 帧可以完整写入和读取。
    #[tokio::test]
    async fn json_frame_can_roundtrip() {
        let (mut client, mut server) = tokio::io::duplex(1024);
        let payload = "ok".to_string();

        write_json_frame(&mut client, ClusterFrameKind::Heartbeat, &payload)
            .await
            .expect("写入 TCP JSON 帧失败");
        let (kind, actual): (ClusterFrameKind, String) = read_json_frame(&mut server)
            .await
            .expect("读取 TCP JSON 帧失败");

        assert_eq!(kind, ClusterFrameKind::Heartbeat);
        assert_eq!(actual, payload);
    }
}
