use std::io;

use bytes::BufMut;
use bytes::BytesMut;
use iron_core_cluster::constant::IRON_TCP_MAX_FRAME_BYTES;
use iron_core_cluster::raft::network::IronTcpFrameCodec;
use iron_core_cluster::raft::network::IronTcpRequest;
use tokio_util::codec::Decoder;
use tokio_util::codec::Encoder;

#[test]
fn small_tcp_frame_can_encode_and_decode() {
    let request = IronTcpRequest::JoinCluster {
        node_id: 42,
        node_addr: "127.0.0.1:5001".to_string(),
    };
    let request_bytes = IronTcpFrameCodec::encode_request(&request).expect("请求编码应该成功");

    let mut codec = IronTcpFrameCodec;
    let mut buffer = BytesMut::new();
    codec
        .encode(request_bytes, &mut buffer)
        .expect("小帧编码应该成功");

    let frame = codec
        .decode(&mut buffer)
        .expect("小帧解码不应该报错")
        .expect("小帧应该已经完整");
    let decoded_request = IronTcpFrameCodec::decode_request(frame).expect("请求内容解码应该成功");

    match decoded_request {
        IronTcpRequest::JoinCluster { node_id, node_addr } => {
            assert_eq!(42, node_id);
            assert_eq!("127.0.0.1:5001", node_addr);
        }
        _ => panic!("解码后应该仍然是加入集群请求"),
    }
}

#[test]
fn oversized_tcp_frame_header_fails_without_waiting_body() {
    let oversized_len = IRON_TCP_MAX_FRAME_BYTES + 1;
    let mut buffer = BytesMut::new();
    buffer.put_u32(oversized_len as u32);

    let mut codec = IronTcpFrameCodec;
    let error = codec.decode(&mut buffer).expect_err("超限帧头应该立即失败");

    assert_eq!(io::ErrorKind::InvalidData, error.kind());
}
