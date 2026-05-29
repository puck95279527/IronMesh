# 数据面结构设计草案

## 当前范围

本文档只记录当前已经确认的数据结构方向，用于后续继续讨论和实现。

当前阶段只讨论数据结构，不讨论代码实现、Raft 写入、HTTP 查询接口或 README 内容。

## 已确认方向

- 数据面根容器命名为 `StateDataContainer`。
- 容器保存一组业务记录，当前业务记录示例为 `BusinessPerson`。
- `BusinessPerson` 的字段可以在编译期随时修改。
- 主键字段名和主键类型也可以修改。
- 使用 trait 让业务结构自己声明 key，容器不需要知道具体字段名。
- 当前选择编译期强类型方案，不使用运行时动态字段 `Map`。

## 核心草案

```rust
pub trait StateDataRecord {
    type Key: Clone + Eq;

    fn state_key(&self) -> Self::Key;
}

pub struct StateDataContainer<T: StateDataRecord> {
    pub records: Vec<T>,
}

pub struct BusinessPerson {
    pub id: u64,
    pub name: String,
    pub age: u32,
}

impl StateDataRecord for BusinessPerson {
    type Key = u64;

    fn state_key(&self) -> Self::Key {
        self.id
    }
}
```

## 主键改名和类型变化示例

未来如果主键字段名和主键类型发生变化，只需要修改业务结构和 trait 实现。

```rust
pub struct BusinessPerson {
    pub person_code: String,
    pub nickname: String,
    pub level: u32,
}

impl StateDataRecord for BusinessPerson {
    type Key = String;

    fn state_key(&self) -> Self::Key {
        self.person_code.clone()
    }
}
```

## 设计意图

`StateDataContainer` 只负责保存记录集合，不绑定业务字段名，也不假设主键一定叫 `id`。

`BusinessPerson` 自己通过 `StateDataRecord` 声明主键是什么，因此后续字段增加、删除、改名，或者主键类型从 `u64` 改为 `String`，都不要求容器结构跟着改变。

这个方向适合当前需求：业务结构会频繁调整，但仍希望保持 Rust 编译期强类型约束。

## 下次继续讨论的问题

- `StateDataContainer` 是否继续保持 `Vec<T>`，还是后续增加索引能力。
- `StateDataRecord::Key` 是否需要增加 `Ord`、`Hash`、`Debug` 等约束。
- 是否需要支持多个集合，例如 `business_persons`、`rooms`、`services`。
- 是否要把这个结构接入 Raft state machine。
- 是否需要为数据面写入定义命令模型。
