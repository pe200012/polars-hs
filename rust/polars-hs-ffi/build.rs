use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=cbindgen.toml");

    let crate_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is set by Cargo");
    let header_path = PathBuf::from(&crate_dir)
        .join("..")
        .join("..")
        .join("include")
        .join("polars_hs.h");

    cbindgen::Builder::new()
        .with_crate(crate_dir)
        .with_config(cbindgen::Config::from_file("cbindgen.toml").expect("valid cbindgen.toml"))
        .generate()
        .expect("generate C header")
        .write_to_file(header_path);
}
