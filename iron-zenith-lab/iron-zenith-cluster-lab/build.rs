use std::env;
use std::fs;
use std::path::PathBuf;

// 构建时把集群验证配置复制到可执行文件所在目录。
fn main() {
    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("缺少 CARGO_MANIFEST_DIR"));
    let source_path = manifest_dir.join("config").join("cluster-boot.toml");

    let target_root = env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| manifest_dir.join("../../target"));
    let profile = env::var("PROFILE").unwrap_or_else(|_| "debug".to_string());
    let target_dir = target_root.join(profile);
    let target_path = target_dir.join("cluster-boot.toml");

    fs::create_dir_all(&target_dir).expect("创建发布目录失败");
    fs::copy(&source_path, &target_path).expect("复制 cluster-boot.toml 失败");

    println!("cargo:rerun-if-changed={}", source_path.display());
}
