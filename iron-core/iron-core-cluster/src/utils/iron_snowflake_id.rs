use std::hash::{Hash, Hasher};
use std::io;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

// IronMesh 雪花 ID 的项目纪元，使用 2024-01-01 00:00:00 UTC。
const IRON_SNOWFLAKE_EPOCH_MS: u64 = 1_704_067_200_000;

// 雪花 ID 中时间戳部分占用的位数。
const SNOWFLAKE_TIMESTAMP_BITS: u8 = 41;

// 雪花 ID 中 worker 部分占用的位数。
const SNOWFLAKE_WORKER_BITS: u8 = 10;

// 雪花 ID 中同毫秒序列部分占用的位数。
const SNOWFLAKE_SEQUENCE_BITS: u8 = 12;

// 雪花 ID 中 worker 部分的最大可表示值。
const SNOWFLAKE_WORKER_MASK: u64 = (1_u64 << SNOWFLAKE_WORKER_BITS) - 1;

// 雪花 ID 中同毫秒序列部分的最大可表示值。
const SNOWFLAKE_SEQUENCE_MASK: u64 = (1_u64 << SNOWFLAKE_SEQUENCE_BITS) - 1;

// 雪花 ID 中时间戳部分的最大可表示值。
const SNOWFLAKE_TIMESTAMP_MASK: u64 = (1_u64 << SNOWFLAKE_TIMESTAMP_BITS) - 1;

// 雪花 ID 中 worker 部分左移的位数。
const SNOWFLAKE_WORKER_SHIFT: u8 = SNOWFLAKE_SEQUENCE_BITS;

// 雪花 ID 中时间戳部分左移的位数。
const SNOWFLAKE_TIMESTAMP_SHIFT: u8 = SNOWFLAKE_WORKER_BITS + SNOWFLAKE_SEQUENCE_BITS;

// IronMesh 雪花 ID 生成器。
pub(crate) struct IronSnowflakeIdGenerator;

// IronMesh 无锁全局雪花 ID 生成器。
struct IronSnowflakeAtomicGenerator {
    worker_id: u16,   // 当前无锁生成器使用的 worker 标识。
    state: AtomicU64, // 当前无锁生成器保存的时间戳偏移和同毫秒序列。
}

impl IronSnowflakeIdGenerator {
    // 使用全局默认生成器生成下一个 u64 雪花 ID。
    pub(crate) fn next_u64() -> anyhow::Result<u64> {
        static GLOBAL_GENERATOR: OnceLock<IronSnowflakeAtomicGenerator> = OnceLock::new();

        let generator = GLOBAL_GENERATOR
            .get_or_init(|| IronSnowflakeAtomicGenerator::new("ironmesh-global-snowflake"));
        generator.next_id()
    }

    // 选择指定逻辑毫秒之后的下一毫秒。
    fn next_timestamp_after(last_timestamp_part: u64) -> Option<u64> {
        match Self::current_timestamp_part() {
            Some(timestamp_part) if timestamp_part > last_timestamp_part => Some(timestamp_part),
            _ if last_timestamp_part < SNOWFLAKE_TIMESTAMP_MASK => Some(last_timestamp_part + 1),
            _ => None,
        }
    }

    // 读取当前系统毫秒相对项目纪元的偏移。
    fn current_timestamp_part() -> Option<u64> {
        let current_timestamp_ms: u64 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()?
            .as_millis()
            .try_into()
            .ok()?;

        if current_timestamp_ms < IRON_SNOWFLAKE_EPOCH_MS {
            return None;
        }

        Some((current_timestamp_ms - IRON_SNOWFLAKE_EPOCH_MS).min(SNOWFLAKE_TIMESTAMP_MASK))
    }

    // 读取当前系统绝对纳秒时间。
    fn current_unix_nanos() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0)
    }

    // 混合 seed、进程号和当前纳秒时间，降低多个 learner 同时启动时的 worker 碰撞概率。
    fn mix_seed(seed: &str, now_nanos: u128) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        seed.hash(&mut hasher);
        std::process::id().hash(&mut hasher);
        now_nanos.hash(&mut hasher);
        hasher.finish()
    }
}

impl IronSnowflakeAtomicGenerator {
    // 创建无锁全局雪花 ID 生成器。
    fn new(seed: &str) -> Self {
        let now_nanos = IronSnowflakeIdGenerator::current_unix_nanos();
        let mixed = IronSnowflakeIdGenerator::mix_seed(seed, now_nanos);
        let worker_id = (mixed & SNOWFLAKE_WORKER_MASK) as u16;
        let initial_sequence = (mixed >> SNOWFLAKE_WORKER_BITS) & SNOWFLAKE_SEQUENCE_MASK;

        Self {
            worker_id,
            state: AtomicU64::new(initial_sequence),
        }
    }

    // 通过 CAS 生成下一个全局 u64 雪花 ID。
    fn next_id(&self) -> anyhow::Result<u64> {
        loop {
            let old_state = self.state.load(Ordering::Relaxed);
            let old_timestamp_part = old_state >> SNOWFLAKE_SEQUENCE_BITS;
            let old_sequence = old_state & SNOWFLAKE_SEQUENCE_MASK;
            let timestamp_part = IronSnowflakeIdGenerator::current_timestamp_part()
                .unwrap_or(old_timestamp_part)
                .max(old_timestamp_part);

            let (next_timestamp_part, next_sequence) = if timestamp_part == old_timestamp_part {
                let next_sequence = (old_sequence + 1) & SNOWFLAKE_SEQUENCE_MASK;
                if next_sequence == 0 {
                    let Some(next_timestamp_part) =
                        IronSnowflakeIdGenerator::next_timestamp_after(old_timestamp_part)
                    else {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            "雪花 ID 时间戳范围已经耗尽，无法继续生成节点 ID",
                        )
                        .into());
                    };
                    (next_timestamp_part, next_sequence)
                } else {
                    (timestamp_part, next_sequence)
                }
            } else {
                (timestamp_part, 0)
            };

            let next_state = (next_timestamp_part << SNOWFLAKE_SEQUENCE_BITS) | next_sequence;
            if self
                .state
                .compare_exchange_weak(old_state, next_state, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return Ok((next_timestamp_part << SNOWFLAKE_TIMESTAMP_SHIFT)
                    | ((self.worker_id as u64) << SNOWFLAKE_WORKER_SHIFT)
                    | next_sequence);
            }

            std::hint::spin_loop();
        }
    }
}
