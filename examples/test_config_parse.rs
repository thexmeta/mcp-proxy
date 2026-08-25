use mcp_proxy::config::ProxyConfig;
use std::path::Path;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let default_path = "examples/configs/test_reverse_refs.toml".to_string();
    let config_path = args.get(1).unwrap_or(&default_path);
    let config = ProxyConfig::load(Path::new(config_path)).unwrap();
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

    let grouped_backend_names: std::collections::HashSet<String> = config
        .proxy
        .endpoint_groups
        .iter()
        .flat_map(|g| {
            let explicit = g.backends.iter().cloned();
            let reverse = config
                .backends
                .iter()
                .filter(|b| b.endpoint_groups.contains(&g.name))
                .map(|b| b.name.clone());
            explicit.chain(reverse)
        })
        .collect();
    println!("Grouped backend names: {:?}", grouped_backend_names);
}
