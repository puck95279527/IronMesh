use std::collections::HashSet;
use std::time::Instant;

use iron_core_cluster::utils::iron_snowflake_id::IronSnowflakeIdGenerator;

// 纯生成性能验证的 ID 数量。
const GENERATE_COUNT: usize = 1_000_000;

// 带重复检测性能验证的 ID 数量。
const UNIQUE_CHECK_COUNT: usize = 200_000;

// 启动雪花 ID 性能验证程序。
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut generator = IronSnowflakeIdGenerator::new("snowflake-id-perf");
    let generate_started = Instant::now();
    let mut last_id = 0_u64;

    for _ in 0..GENERATE_COUNT {
        last_id = generator.next_id()?;
    }

    let generate_elapsed = generate_started.elapsed();
    let generate_per_second = GENERATE_COUNT as f64 / generate_elapsed.as_secs_f64();
    println!(
        "[Iron] [snowflake-perf] generate_count={GENERATE_COUNT} elapsed_ms={} ids_per_second={:.0} last_id={last_id}",
        generate_elapsed.as_millis(),
        generate_per_second
    );

    let mut unique_generator = IronSnowflakeIdGenerator::new("snowflake-id-unique-check");
    let mut ids = HashSet::with_capacity(UNIQUE_CHECK_COUNT);
    let unique_started = Instant::now();

    for _ in 0..UNIQUE_CHECK_COUNT {
        let id = unique_generator.next_id()?;
        ids.insert(id);
    }

    let unique_elapsed = unique_started.elapsed();
    let duplicate_count = UNIQUE_CHECK_COUNT - ids.len();
    let unique_per_second = UNIQUE_CHECK_COUNT as f64 / unique_elapsed.as_secs_f64();
    println!(
        "[Iron] [snowflake-perf] unique_check_count={UNIQUE_CHECK_COUNT} elapsed_ms={} ids_per_second_with_hashset={:.0} duplicate_count={duplicate_count}",
        unique_elapsed.as_millis(),
        unique_per_second
    );

    let global_started = Instant::now();
    let mut global_last_id = 0_u64;

    for _ in 0..GENERATE_COUNT {
        global_last_id = IronSnowflakeIdGenerator::next_u64();
    }

    let global_elapsed = global_started.elapsed();
    let global_per_second = GENERATE_COUNT as f64 / global_elapsed.as_secs_f64();
    println!(
        "[Iron] [snowflake-perf] global_next_u64_count={GENERATE_COUNT} elapsed_ms={} ids_per_second={:.0} last_id={global_last_id}",
        global_elapsed.as_millis(),
        global_per_second
    );

    Ok(())
}
