use crab_hmds::server;
use dotenvy::from_path;
use std::path::Path;

#[tokio::main]
pub async fn main() {
    // 1️⃣ 加载本 crate 根目录下的 .env（开发环境用）
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let env_path = Path::new(manifest_dir).join(".env");
    if env_path.exists() {
        let _ = from_path(&env_path);
    }
    server::start().await;
}
