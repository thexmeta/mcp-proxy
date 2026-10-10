use mcp_proxy::Proxy;
use mcp_proxy::config::ProxyConfig;
use std::path::Path;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing subscriber
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("mcp_proxy=debug"));
    FmtSubscriber::builder()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();

    let mut config = ProxyConfig::load(Path::new("/home/mxadm/.mcp-proxy/config.toml"))?;
    config.resolve_env_vars();

    println!("Config loaded, building proxy...");
    let proxy = match Proxy::from_config(config).await {
        Ok(p) => {
            println!("Proxy built successfully!");
            p
        }
        Err(e) => {
            eprintln!("Proxy::from_config failed: {:?}", e);
            return Err(e);
        }
    };
    println!("Starting serve...");
    match proxy.serve().await {
        Ok(_) => println!("Serve completed"),
        Err(e) => {
            eprintln!("Serve failed: {:?}", e);
            return Err(e);
        }
    }
    Ok(())
}
