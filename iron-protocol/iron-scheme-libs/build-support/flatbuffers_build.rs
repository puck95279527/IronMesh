use std::path::{Path, PathBuf};
use std::process::Command;

// 根据协议目录生成 FlatBuffers Rust 代码。
pub fn generate_flatbuffers(schema_dir: &Path, out_dir: &Path) {
    let schemas = schema_files(schema_dir);
    if schemas.is_empty() {
        panic!("schema dir `{}` does not contain any .fbs file", schema_dir.display());
    }

    let flatc = protocol_flatc(schema_dir);

    println!("cargo:rerun-if-changed={}", schema_dir.display());
    println!("cargo:rerun-if-changed={}", flatc.display());
    for schema in &schemas {
        println!("cargo:rerun-if-changed={}", schema.display());
    }

    std::fs::create_dir_all(out_dir).unwrap_or_else(|error| {
        panic!(
            "failed to create generated schema dir `{}`: {error}",
            out_dir.display()
        )
    });

    clean_generated_outputs(out_dir);

    let output = Command::new(flatc.as_os_str())
        .arg("--rust")
        .arg("--rust-module-root-file")
        .arg("-o")
        .arg(out_dir)
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

    format_generated_outputs(out_dir);
}

// 从协议目录推导项目固定的 flatc 工具路径。
fn protocol_flatc(schema_dir: &Path) -> PathBuf {
    schema_dir
        .parent()
        .and_then(Path::parent)
        .unwrap_or_else(|| {
            panic!(
                "failed to resolve protocol root from schema dir `{}`",
                schema_dir.display()
            )
        })
        .join("tools/flatc")
}

// 收集协议目录下的 FlatBuffers schema 文件。
fn schema_files(schema_dir: &Path) -> Vec<PathBuf> {
    let mut schemas = direct_schema_files(schema_dir);
    let root_schema_name = schema_dir
        .file_name()
        .and_then(|name| name.to_str())
        .map(|name| format!("{name}.fbs"));

    schemas.sort_by(|left, right| {
        let left_name = file_name(left);
        let right_name = file_name(right);
        let left_is_root = root_schema_name.as_deref() == Some(left_name);
        let right_is_root = root_schema_name.as_deref() == Some(right_name);

        left_is_root
            .cmp(&right_is_root)
            .then_with(|| left_name.cmp(right_name))
    });

    schemas
}

// 收集协议目录直属层级的 .fbs 文件。
fn direct_schema_files(schema_dir: &Path) -> Vec<PathBuf> {
    let entries = std::fs::read_dir(schema_dir).unwrap_or_else(|error| {
        panic!("failed to read schema dir `{}`: {error}", schema_dir.display())
    });
    let mut schemas = Vec::new();

    for entry in entries {
        let entry = entry.unwrap_or_else(|error| {
            panic!(
                "failed to read schema entry in `{}`: {error}",
                schema_dir.display()
            )
        });
        let path = entry.path();

        if path.is_file()
            && path
                .extension()
                .and_then(|extension| extension.to_str())
                .is_some_and(|extension| extension == "fbs")
        {
            schemas.push(path);
        }
    }

    schemas
}

// 清理上一次生成留下的 FlatBuffers Rust 文件。
fn clean_generated_outputs(out_dir: &Path) {
    let root_module = out_dir.join("mod.rs");
    if root_module.exists() {
        std::fs::remove_file(&root_module).unwrap_or_else(|error| {
            panic!(
                "failed to remove generated schema root `{}`: {error}",
                root_module.display()
            )
        });
    }

    for generated in generated_files(out_dir) {
        std::fs::remove_file(&generated).unwrap_or_else(|error| {
            panic!(
                "failed to remove generated schema `{}`: {error}",
                generated.display()
            )
        });
    }
}

// 格式化 FlatBuffers 生成的 Rust 文件。
fn format_generated_outputs(out_dir: &Path) {
    let root_module = out_dir.join("mod.rs");
    if root_module.exists() {
        rustfmt(&root_module);
    }

    for generated in generated_files(out_dir) {
        rustfmt(&generated);
    }
}

// 调用 rustfmt 格式化单个生成文件。
fn rustfmt(path: &Path) {
    let output = Command::new("rustfmt")
        .arg(path)
        .output()
        .unwrap_or_else(|error| {
            panic!(
                "failed to execute `rustfmt` for generated schema `{}`: {error}",
                path.display()
            )
        });

    if !output.status.success() {
        panic!(
            "`rustfmt` failed for generated schema {}:\nstdout:\n{}\nstderr:\n{}",
            path.display(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

// 收集输出目录下所有 FlatBuffers 生成文件。
fn generated_files(out_dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    if out_dir.exists() {
        collect_generated_files(out_dir, &mut files);
    }
    files.sort();
    files
}

// 递归收集输出目录中的 *_generated.rs 文件。
fn collect_generated_files(dir: &Path, files: &mut Vec<PathBuf>) {
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
        } else if file_name(&path).ends_with("_generated.rs") {
            files.push(path);
        }
    }
}

// 读取路径最后一段文件名。
fn file_name(path: &Path) -> &str {
    path.file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_else(|| panic!("failed to read file name from `{}`", path.display()))
}
