use mcp_proxy::config::ProxyConfig;
use std::path::Path;

fn main() {
    let config = ProxyConfig::load(Path::new("examples/configs/endpoint_groups.toml")).unwrap();
    println!(
        "Backends: {:?}",
        config
            .backends
            .iter()
            .map(|b| (&b.name, &b.endpoint_groups))
            .collect::<Vec<_>>()
    );
    println!(
        "Endpoint groups: {:?}",
        config
            .proxy
            .endpoint_groups
            .iter()
            .map(|g| (&g.name, &g.backends))
            .collect::<Vec<_>>()
    );
}
