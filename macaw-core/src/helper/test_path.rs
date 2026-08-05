use once_cell::sync::OnceCell;
use std::sync::Mutex;

use crate::lib::*;

/// Returns the [PathBuf] of the current test folder
#[macro_export]
macro_rules! test_path {
    () => {{
        use macaw_core::prelude::test_path::get_cargo_workspace;
        get_cargo_workspace(env!("CARGO_MANIFEST_DIR"))
            .join(file!())
            .parent()
            .unwrap()
            .to_path_buf()
    }};
}

/// See https://github.com/rust-lang/cargo/issues/3946
/// https://github.com/mitsuhiko/insta/blob/b113499249584cb650150d2d01ed96ee66db6b30/src/runtime.rs#L67-L88
pub fn get_cargo_workspace(manifest_dir: &str) -> &Path {
    static WORKSPACES: OnceCell<Mutex<BTreeMap<String, &'static Path>>> = OnceCell::new();
    let mut workspaces = WORKSPACES
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .unwrap();

    if let Some(rv) = workspaces.get(manifest_dir) {
        rv
    } else {
        #[derive(Deserialize)]
        struct Manifest {
            workspace_root: String,
        }
        let output = std::process::Command::new(env!("CARGO"))
            .arg("metadata")
            .arg("--format-version=1")
            .current_dir(manifest_dir)
            .output()
            .unwrap();
        let manifest: Manifest = serde_json::from_slice(&output.stdout).unwrap();
        let path = Box::leak(Box::new(PathBuf::from(manifest.workspace_root)));
        workspaces.insert(manifest_dir.to_string(), path.as_path());
        workspaces.get(manifest_dir).unwrap()
    }
}
