use std::fmt;
use std::io::Cursor;
use std::marker::PhantomData;

use openraft::BasicNode;

use crate::raft::storage::iron_raft_state_machine_data::IronRaftStateMachineData;

// IronMesh 集群 Raft 类型配置。
pub struct IronRaftTypeConfig<S>
where
    S: IronRaftStateMachineData,
{
    // 状态机类型标记，避免运行期存储泛型数据。
    marker: PhantomData<fn() -> S>,
}

impl<S> Clone for IronRaftTypeConfig<S>
where
    S: IronRaftStateMachineData,
{
    // 复制 Raft 类型配置标记。
    fn clone(&self) -> Self {
        *self
    }
}

impl<S> Copy for IronRaftTypeConfig<S> where S: IronRaftStateMachineData {}

impl<S> Default for IronRaftTypeConfig<S>
where
    S: IronRaftStateMachineData,
{
    // 创建默认 Raft 类型配置标记。
    fn default() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<S> fmt::Debug for IronRaftTypeConfig<S>
where
    S: IronRaftStateMachineData,
{
    // 格式化 Raft 类型配置。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("IronRaftTypeConfig")
    }
}

impl<S> Eq for IronRaftTypeConfig<S> where S: IronRaftStateMachineData {}

impl<S> PartialEq for IronRaftTypeConfig<S>
where
    S: IronRaftStateMachineData,
{
    // 比较 Raft 类型配置标记。
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl<S> Ord for IronRaftTypeConfig<S>
where
    S: IronRaftStateMachineData,
{
    // 比较 Raft 类型配置标记顺序。
    fn cmp(&self, _other: &Self) -> std::cmp::Ordering {
        std::cmp::Ordering::Equal
    }
}

impl<S> PartialOrd for IronRaftTypeConfig<S>
where
    S: IronRaftStateMachineData,
{
    // 比较 Raft 类型配置标记偏序。
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<S> openraft::RaftTypeConfig for IronRaftTypeConfig<S>
where
    S: IronRaftStateMachineData,
{
    type D = S::WriteRequest;
    type R = S::WriteResponse;
    type NodeId = u64;
    type Node = BasicNode;
    type Entry = openraft::Entry<Self>;
    type SnapshotData = Cursor<Vec<u8>>;
    type AsyncRuntime = openraft::TokioRuntime;
    type Responder = openraft::impls::OneshotResponder<Self>;
}
