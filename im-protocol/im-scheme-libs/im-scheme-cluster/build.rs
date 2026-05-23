use std::env;
use std::path::PathBuf;
use std::process::Command;

// 构建集群协议的 FlatBuffers 生成代码。
fn main() {
    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is set by Cargo"));
    let schema = manifest_dir.join("../../im-flat-dsl/cluster/cluster.fbs");
    let out_dir = manifest_dir.join("src/scheme");
    let flatc = manifest_dir.join("../../im-flat-dsl/tools/flatc");

    println!("cargo:rerun-if-changed={}", schema.display());
    println!("cargo:rerun-if-changed={}", flatc.display());

    let output = Command::new(flatc.as_os_str())
        .arg("--rust")
        .arg("-o")
        .arg(&out_dir)
        .arg(&schema)
        .output()
        .unwrap_or_else(|error| {
            panic!(
                "failed to execute schema-local flatc `{}`: {error}. Restore im-protocol/im-flat-dsl/tools/flatc version 25.12.19",
                flatc.display()
            )
        });

    if !output.status.success() {
        panic!(
            "`{}` failed for schema {}:\nstdout:\n{}\nstderr:\n{}",
            flatc.display(),
            schema.display(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let generated = out_dir.join("cluster_generated.rs");
    let output = Command::new("rustfmt")
        .arg(&generated)
        .output()
        .unwrap_or_else(|error| {
            panic!(
                "failed to execute `rustfmt` for generated schema `{}`: {error}",
                generated.display()
            )
        });

    if !output.status.success() {
        panic!(
            "`rustfmt` failed for generated schema {}:\nstdout:\n{}\nstderr:\n{}",
            generated.display(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
