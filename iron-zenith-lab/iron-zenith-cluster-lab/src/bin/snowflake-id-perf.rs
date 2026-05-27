// 雪花 ID 生成器已经收回到 iron-core-cluster 内部，不再作为外部验证入口。
fn main() {
    println!(
        "[Iron] [snowflake-perf] 雪花 ID 生成器是 iron-core-cluster 内部工具，外部验证入口已关闭"
    );
}
