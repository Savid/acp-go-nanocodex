use sha2::{Digest, Sha256};
use std::{env, fs, path::PathBuf};

fn main() {
    let native =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("Cargo manifest directory"));
    let root = native.parent().expect("repository root");
    let mut paths = [
        "Cargo.toml",
        "Cargo.lock",
        "rust-toolchain.toml",
        "build.rs",
    ]
    .map(|name| format!("native/{name}"))
    .to_vec();
    paths.extend([
        "internal/nanocodex/protocol.go".to_owned(),
        "internal/nanocodex/client.go".to_owned(),
    ]);
    println!("cargo:rerun-if-changed={}", native.join("src").display());
    for entry in fs::read_dir(native.join("src")).expect("native source directory") {
        let entry = entry.expect("native source entry");
        if entry
            .path()
            .extension()
            .is_some_and(|extension| extension == "rs")
        {
            paths.push(format!(
                "native/src/{}",
                entry.file_name().to_str().expect("UTF-8 source name")
            ));
        }
    }
    paths.sort();
    let mut hash = Sha256::new();
    for path in paths {
        let absolute = root.join(&path);
        println!("cargo:rerun-if-changed={}", absolute.display());
        hash.update(path.as_bytes());
        hash.update([0]);
        hash.update(fs::read(absolute).expect("fingerprinted native source"));
        hash.update([0]);
    }
    println!(
        "cargo:rustc-env=NANOCODEX_HELPER_FINGERPRINT={:x}",
        hash.finalize()
    );
}
