use std::error::Error;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// IronMesh 雪花 ID 的项目纪元，使用 2024-01-01 00:00:00 UTC。
pub const IRON_SNOWFLAKE_EPOCH_MS: u64 = 1_704_067_200_000;

// IronMesh 雪花 ID 生成器。
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IronSnowflakeIdGenerator {
    worker_id: u16,         // 当前生成器使用的 worker 标识。
    sequence: u16,          // 当前生成器使用的同毫秒序列值。
    last_timestamp_ms: u64, // 当前生成器最近一次生成 ID 时使用的绝对毫秒时间。
}

// IronMesh 雪花 ID 解析结果。
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct IronSnowflakeIdParts {
    pub timestamp_ms: u64, // ID 中携带的绝对毫秒时间。
    pub worker_id: u16,    // ID 中携带的 worker 标识。
    pub sequence: u16,     // ID 中携带的同毫秒序列值。
}

// IronMesh 雪花 ID 生成错误。
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum IronSnowflakeIdError {
    // 系统时间早于 Unix epoch，无法得到正向毫秒时间。
    SystemTimeBeforeUnixEpoch,
    // 当前系统时间早于 IronMesh 项目纪元。
    TimestampBeforeEpoch {
        current_timestamp_ms: u64, // 当前系统绝对毫秒时间。
        epoch_ms: u64,             // IronMesh 雪花 ID 项目纪元毫秒时间。
    },
    // 当前时间超过 41 位时间戳能够表达的范围。
    TimestampOverflow {
        timestamp_part: u64,     // 当前时间相对项目纪元的毫秒偏移量。
        max_timestamp_part: u64, // 41 位时间戳能够表达的最大毫秒偏移量。
    },
    // 当前系统时间小于上一次生成 ID 的时间，说明发生了时钟回拨。
    ClockMovedBackwards {
        current_timestamp_ms: u64, // 当前系统绝对毫秒时间。
        last_timestamp_ms: u64,    // 上一次生成 ID 使用的绝对毫秒时间。
    },
}

// IronMesh 无锁全局雪花 ID 生成器。
struct IronSnowflakeAtomicGenerator {
    worker_id: u16,   // 当前无锁生成器使用的 worker 标识。
    state: AtomicU64, // 当前无锁生成器保存的时间戳偏移和同毫秒序列。
}

impl IronSnowflakeIdGenerator {
    // 使用全局默认生成器生成下一个 u64 雪花 ID。
    pub fn next_u64() -> u64 {
        Self::next_global_u64()
    }

    // 使用指定 seed 创建雪花 ID 生成器。
    //
    // 使用示例：
    // ```rust
    // use iron_core_cluster::utils::iron_snowflake_id::IronSnowflakeIdGenerator;
    //
    // fn main() -> Result<(), Box<dyn std::error::Error>> {
    //     let node_addr = "127.0.0.1:5004";
    //     let mut generator = IronSnowflakeIdGenerator::new(node_addr);
    //     let node_id = generator.next_id()?;
    //     let parts = IronSnowflakeIdGenerator::parse(node_id);
    //
    //     println!("node_id={node_id}");
    //     println!("timestamp_ms={}", parts.timestamp_ms);
    //     println!("worker_id={}", parts.worker_id);
    //     println!("sequence={}", parts.sequence);
    //
    //     Ok(())
    // }
    // ```
    pub fn new(seed: impl AsRef<str>) -> Self {
        Self::build(seed.as_ref())
    }

    // 生成下一个 u64 雪花 ID。
    pub fn next_id(&mut self) -> Result<u64, IronSnowflakeIdError> {
        self.generate_next_id()
    }

    // 解析 u64 雪花 ID 中携带的时间、worker 和序列。
    pub fn parse(id: u64) -> IronSnowflakeIdParts {
        Self::parse_id(id)
    }

    // 读取 u64 雪花 ID 中携带的绝对毫秒时间。
    pub fn timestamp_ms(id: u64) -> u64 {
        Self::parse(id).timestamp_ms
    }
}

// ==================== 以下是实现细节，调用方不需要关心 ====================

// 雪花 ID 中时间戳部分占用的位数。
const TIMESTAMP_BITS: u8 = 41;

// 雪花 ID 中 worker 部分占用的位数。
const WORKER_BITS: u8 = 10;

// 雪花 ID 中同毫秒序列部分占用的位数。
const SEQUENCE_BITS: u8 = 12;

// 雪花 ID 中 worker 部分的最大可表示值。
const WORKER_MASK: u64 = (1_u64 << WORKER_BITS) - 1;

// 雪花 ID 中同毫秒序列部分的最大可表示值。
const SEQUENCE_MASK: u64 = (1_u64 << SEQUENCE_BITS) - 1;

// 雪花 ID 中时间戳部分的最大可表示值。
const TIMESTAMP_MASK: u64 = (1_u64 << TIMESTAMP_BITS) - 1;

// 雪花 ID 中 worker 部分左移的位数。
const WORKER_SHIFT: u8 = SEQUENCE_BITS;

// 雪花 ID 中时间戳部分左移的位数。
const TIMESTAMP_SHIFT: u8 = WORKER_BITS + SEQUENCE_BITS;

impl IronSnowflakeIdGenerator {
    // 构建雪花 ID 生成器的内部状态。
    fn build(seed: &str) -> Self {
        let now_nanos = Self::current_unix_nanos().unwrap_or(0);
        let mixed = Self::mix_seed(seed, now_nanos);

        Self {
            worker_id: (mixed & WORKER_MASK) as u16,
            sequence: ((mixed >> WORKER_BITS) & SEQUENCE_MASK) as u16,
            last_timestamp_ms: 0,
        }
    }

    // 生成下一个雪花 ID 的内部实现。
    fn generate_next_id(&mut self) -> Result<u64, IronSnowflakeIdError> {
        let mut timestamp_ms = Self::current_timestamp_ms()?;
        if timestamp_ms < self.last_timestamp_ms {
            return Err(IronSnowflakeIdError::ClockMovedBackwards {
                current_timestamp_ms: timestamp_ms,
                last_timestamp_ms: self.last_timestamp_ms,
            });
        }

        if timestamp_ms == self.last_timestamp_ms {
            self.sequence = ((self.sequence as u64 + 1) & SEQUENCE_MASK) as u16;
            if self.sequence == 0 {
                timestamp_ms = Self::wait_next_millisecond(self.last_timestamp_ms)?;
            }
        } else {
            self.sequence = ((self.sequence as u64 + 1) & SEQUENCE_MASK) as u16;
        }

        self.last_timestamp_ms = timestamp_ms;
        let timestamp_part = timestamp_ms - IRON_SNOWFLAKE_EPOCH_MS;
        if timestamp_part > TIMESTAMP_MASK {
            return Err(IronSnowflakeIdError::TimestampOverflow {
                timestamp_part,
                max_timestamp_part: TIMESTAMP_MASK,
            });
        }

        Ok((timestamp_part << TIMESTAMP_SHIFT)
            | ((self.worker_id as u64) << WORKER_SHIFT)
            | self.sequence as u64)
    }

    // 解析雪花 ID 的内部实现。
    fn parse_id(id: u64) -> IronSnowflakeIdParts {
        let timestamp_part = (id >> TIMESTAMP_SHIFT) & TIMESTAMP_MASK;
        let worker_id = ((id >> WORKER_SHIFT) & WORKER_MASK) as u16;
        let sequence = (id & SEQUENCE_MASK) as u16;

        IronSnowflakeIdParts {
            timestamp_ms: timestamp_part + IRON_SNOWFLAKE_EPOCH_MS,
            worker_id,
            sequence,
        }
    }

    // 等待系统时间进入指定毫秒之后的下一毫秒。
    fn wait_next_millisecond(last_timestamp_ms: u64) -> Result<u64, IronSnowflakeIdError> {
        loop {
            let timestamp_ms = Self::current_timestamp_ms()?;
            if timestamp_ms > last_timestamp_ms {
                return Ok(timestamp_ms);
            }

            std::thread::sleep(Duration::from_micros(100));
        }
    }

    // 读取当前系统绝对毫秒时间。
    fn current_timestamp_ms() -> Result<u64, IronSnowflakeIdError> {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| IronSnowflakeIdError::SystemTimeBeforeUnixEpoch)?;
        let current_timestamp_ms = duration.as_millis() as u64;

        if current_timestamp_ms < IRON_SNOWFLAKE_EPOCH_MS {
            return Err(IronSnowflakeIdError::TimestampBeforeEpoch {
                current_timestamp_ms,
                epoch_ms: IRON_SNOWFLAKE_EPOCH_MS,
            });
        }

        Ok(current_timestamp_ms)
    }

    // 读取当前系统绝对纳秒时间。
    fn current_unix_nanos() -> Result<u128, IronSnowflakeIdError> {
        Ok(SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| IronSnowflakeIdError::SystemTimeBeforeUnixEpoch)?
            .as_nanos())
    }

    // 混合 seed、进程号和当前纳秒时间，降低多个 learner 同时启动时的 worker 碰撞概率。
    fn mix_seed(seed: &str, now_nanos: u128) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        seed.hash(&mut hasher);
        std::process::id().hash(&mut hasher);
        now_nanos.hash(&mut hasher);
        hasher.finish()
    }

    // 使用全局默认生成器生成下一个 u64 雪花 ID 的内部实现。
    fn next_global_u64() -> u64 {
        static GLOBAL_GENERATOR: OnceLock<IronSnowflakeAtomicGenerator> = OnceLock::new();

        let generator = GLOBAL_GENERATOR
            .get_or_init(|| IronSnowflakeAtomicGenerator::new("ironmesh-global-snowflake"));

        loop {
            match generator.next_id() {
                Ok(id) => return id,
                Err(IronSnowflakeIdError::ClockMovedBackwards { .. }) => {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(error) => panic!("IronSnowflakeIdGenerator 全局 ID 生成失败: {error}"),
            }
        }
    }
}

impl IronSnowflakeAtomicGenerator {
    // 创建无锁全局雪花 ID 生成器。
    fn new(seed: &str) -> Self {
        let now_nanos = IronSnowflakeIdGenerator::current_unix_nanos().unwrap_or(0);
        let mixed = IronSnowflakeIdGenerator::mix_seed(seed, now_nanos);
        let worker_id = (mixed & WORKER_MASK) as u16;
        let initial_sequence = (mixed >> WORKER_BITS) & SEQUENCE_MASK;

        Self {
            worker_id,
            state: AtomicU64::new(initial_sequence),
        }
    }

    // 通过 CAS 生成下一个全局 u64 雪花 ID。
    fn next_id(&self) -> Result<u64, IronSnowflakeIdError> {
        loop {
            let timestamp_ms = IronSnowflakeIdGenerator::current_timestamp_ms()?;
            let timestamp_part = timestamp_ms - IRON_SNOWFLAKE_EPOCH_MS;
            if timestamp_part > TIMESTAMP_MASK {
                return Err(IronSnowflakeIdError::TimestampOverflow {
                    timestamp_part,
                    max_timestamp_part: TIMESTAMP_MASK,
                });
            }

            let old_state = self.state.load(Ordering::Relaxed);
            let old_timestamp_part = old_state >> SEQUENCE_BITS;
            let old_sequence = old_state & SEQUENCE_MASK;

            if timestamp_part < old_timestamp_part {
                return Err(IronSnowflakeIdError::ClockMovedBackwards {
                    current_timestamp_ms: timestamp_ms,
                    last_timestamp_ms: old_timestamp_part + IRON_SNOWFLAKE_EPOCH_MS,
                });
            }

            let (next_timestamp_part, next_sequence) = if timestamp_part == old_timestamp_part {
                let next_sequence = (old_sequence + 1) & SEQUENCE_MASK;
                if next_sequence == 0 {
                    let next_timestamp_ms = IronSnowflakeIdGenerator::wait_next_millisecond(
                        old_timestamp_part + IRON_SNOWFLAKE_EPOCH_MS,
                    )?;
                    (next_timestamp_ms - IRON_SNOWFLAKE_EPOCH_MS, next_sequence)
                } else {
                    (timestamp_part, next_sequence)
                }
            } else {
                (timestamp_part, 0)
            };

            let next_state = (next_timestamp_part << SEQUENCE_BITS) | next_sequence;
            if self
                .state
                .compare_exchange_weak(old_state, next_state, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return Ok((next_timestamp_part << TIMESTAMP_SHIFT)
                    | ((self.worker_id as u64) << WORKER_SHIFT)
                    | next_sequence);
            }

            std::hint::spin_loop();
        }
    }
}

impl Display for IronSnowflakeIdError {
    // 格式化雪花 ID 生成错误。
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SystemTimeBeforeUnixEpoch => {
                write!(formatter, "系统时间早于 Unix epoch，无法生成雪花 ID")
            }
            Self::TimestampBeforeEpoch {
                current_timestamp_ms,
                epoch_ms,
            } => write!(
                formatter,
                "系统时间早于 IronMesh 雪花 ID 纪元: current_timestamp_ms={current_timestamp_ms}, epoch_ms={epoch_ms}"
            ),
            Self::TimestampOverflow {
                timestamp_part,
                max_timestamp_part,
            } => write!(
                formatter,
                "系统时间超过雪花 ID 时间戳范围: timestamp_part={timestamp_part}, max_timestamp_part={max_timestamp_part}"
            ),
            Self::ClockMovedBackwards {
                current_timestamp_ms,
                last_timestamp_ms,
            } => write!(
                formatter,
                "系统时间发生回拨: current_timestamp_ms={current_timestamp_ms}, last_timestamp_ms={last_timestamp_ms}"
            ),
        }
    }
}

impl Error for IronSnowflakeIdError {}
