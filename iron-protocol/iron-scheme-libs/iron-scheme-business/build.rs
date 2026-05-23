use std::env;
use std::path::PathBuf;

mod flatbuffers_build {
    include!("../build-support/flatbuffers_build.rs");
}

// 构建业务协议的 FlatBuffers 生成代码。
fn main() {
    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is set by Cargo"));
    let schema_dir = manifest_dir.join("../../iron-flat-dsl/business");
    let out_dir = manifest_dir.join("src/scheme");

    flatbuffers_build::generate_flatbuffers(&schema_dir, &out_dir);
}
