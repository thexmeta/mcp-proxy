use mcp_proxy::Proxy;
use mcp_proxy::config::ProxyConfig;
use std::path::Path;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config.toml".to_string());
    let mut config = ProxyConfig::load(Path::new(&path))?;
    config.resolve_env_vars();

    println!("Config loaded, building proxy...");
    let proxy = Proxy::from_config(config).await?;
    println!("Proxy built, starting serve...");
    proxy.serve().await
}
