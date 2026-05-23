use std::env;
use std::path::PathBuf;
use std::process::Command;

// 构建业务协议的 FlatBuffers 生成代码。
fn main() {
    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is set by Cargo"));
    let schema_dir = manifest_dir.join("../../iron-flat-dsl/business");
    let schemas = [
        schema_dir.join("auth.fbs"),
        schema_dir.join("ddz.fbs"),
        schema_dir.join("pdk.fbs"),
        schema_dir.join("business.fbs"),
    ];
    let out_dir = manifest_dir.join("src/scheme");
    let flatc = manifest_dir.join("../../tools/flatc");

    for schema in &schemas {
        println!("cargo:rerun-if-changed={}", schema.display());
    }
    println!("cargo:rerun-if-changed={}", flatc.display());

    let output = Command::new(flatc.as_os_str())
        .arg("--rust")
        .arg("--rust-module-root-file")
        .arg("-o")
        .arg(&out_dir)
        .args(&schemas)
        .output()
        .unwrap_or_else(|error| {
            panic!(
                "failed to execute protocol-local flatc `{}`: {error}. Restore iron-protocol/tools/flatc version 25.12.19",
                flatc.display()
            )
        });

    if !output.status.success() {
        panic!(
            "`{}` failed for schemas in {}:\nstdout:\n{}\nstderr:\n{}",
            flatc.display(),
            schema_dir.display(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let generated_root = out_dir.join("mod.rs");
    let output = Command::new("rustfmt")
        .arg(&generated_root)
        .output()
        .unwrap_or_else(|error| {
            panic!(
                "failed to execute `rustfmt` for generated schema root `{}`: {error}",
                generated_root.display()
            )
        });

    if !output.status.success() {
        panic!(
            "`rustfmt` failed for generated schema root {}:\nstdout:\n{}\nstderr:\n{}",
            generated_root.display(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    for generated in generated_files(&out_dir) {
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
}

fn generated_files(out_dir: &std::path::Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_generated_files(out_dir, &mut files);
    files.sort();
    files
}

fn collect_generated_files(dir: &std::path::Path, files: &mut Vec<PathBuf>) {
    let entries = std::fs::read_dir(dir).unwrap_or_else(|error| {
        panic!(
            "failed to read generated schema dir `{}`: {error}",
            dir.display()
        )
    });

    for entry in entries {
        let entry = entry.unwrap_or_else(|error| {
            panic!(
                "failed to read generated schema entry in `{}`: {error}",
                dir.display()
            )
        });
        let path = entry.path();

        if path.is_dir() {
            collect_generated_files(&path, files);
        } else if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with("_generated.rs"))
        {
            files.push(path);
        }
    }
}
