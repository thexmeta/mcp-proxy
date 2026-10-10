use mcp_proxy::Proxy;
use mcp_proxy::config::ProxyConfig;
use std::path::Path;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut config = ProxyConfig::load(Path::new("/home/mxadm/.mcp-proxy/config.toml"))?;
    config.resolve_env_vars();

    println!("Config loaded, building proxy...");
    let proxy = Proxy::from_config(config).await?;
    println!("Proxy built, starting serve...");
    proxy.serve().await
}
