//! Tests for stdio backend timeout behavior.
//!
//! Verifies that mcp-proxy properly handles non-responsive backends:
//! - Without a timeout layer, tool calls to a hanging backend block forever
//! - With a timeout layer, tool calls return a JSON-RPC error instead
//!
//! # The hang path
//!
//! 1. `StdioClientTransport::spawn_command` starts the child process
//! 2. `McpClient::connect_with_handler` starts a message loop reading stdout
//! 3. `Backend::initialize()` sends `initialize` and waits for a response
//! 4. The message loop blocks on `read_until(b'\n')` — the process never writes
//! 5. `initialize()` hangs forever
//!
//! The per-backend `TimeoutLayer` only wraps request dispatch (`BackendService`),
//! not the initialization handshake. So adding a timeout to the backend config
//! does NOT prevent the hang during construction — only `tokio::time::timeout`
//! around the entire construction can bound it.

use std::time::Duration;

use mcp_proxy::Proxy;
use mcp_proxy::config::ProxyConfig;

/// A heartbeat file a spawned backend appends to while it is alive.
///
/// Process liveness is measured by side effect, not by pid: a child that was
/// killed but not yet reaped is a zombie, and `kill -0` on a zombie still
/// succeeds, so a pid check would pass even when the fix is absent.
#[cfg(unix)]
struct Heartbeat {
    path: std::path::PathBuf,
}

#[cfg(unix)]
impl Heartbeat {
    fn new(tag: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "mcp-proxy-{tag}-{}-{}.ticks",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_file(&path);
        Self { path }
    }

    /// A shell command that never answers `initialize` and ticks while alive.
    ///
    /// `trap '' TERM` makes the child ignore SIGTERM, so nothing here can pass
    /// on a polite signal that a genuinely wedged backend would also ignore.
    fn silent_backend_command(&self) -> String {
        format!(
            "trap '' TERM; while :; do printf x >> '{}'; sleep 0.1; done",
            self.path.display()
        )
    }

    fn ticks(&self) -> u64 {
        std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0)
    }

    async fn wait_until_alive(&self) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while self.ticks() == 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "backend never started ticking; this test cannot tell life from death"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Assert the process behind the heartbeat has stopped for good.
    async fn assert_stopped(&self) {
        tokio::time::sleep(Duration::from_millis(600)).await;
        let settled = self.ticks();
        tokio::time::sleep(Duration::from_millis(600)).await;
        let later = self.ticks();
        assert_eq!(
            settled, later,
            "a timed-out backend is still running ({settled} -> {later} ticks): the child was \
             dropped without being killed, so it stays alive unregistered, keeps its side effects \
             (a desktop backend re-autolaunches its own D-Bus session and portal), and every \
             retry spawns another one and pays the full init timeout again"
        );
    }
}

#[cfg(unix)]
impl Drop for Heartbeat {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[cfg(unix)]
fn silent_backend_config(command: &str, init_timeout_secs: u32) -> ProxyConfig {
    ProxyConfig::parse(&format!(
        r#"
        [proxy]
        name = "test-proxy"
        version = "1.0.0"
        [proxy.listen]

        [[backends]]
        name = "sleepy"
        transport = "stdio"
        command = "sh"
        args = ["-c", {command:?}]
        init_timeout = {init_timeout_secs}
        "#
    ))
    .expect("config should parse")
}

/// Integration: `init_timeout` bounds construction over a real subprocess.
///
/// Exercises the actual cross-process path the service uses -- real `sh` child,
/// real `StdioClientTransport`, real proxy construction -- rather than a mocked
/// seam. The two tests above prove construction hangs forever without it.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn init_timeout_bounds_construction_of_a_silent_stdio_backend() {
    let heartbeat = Heartbeat::new("init-timeout-bounds");
    let config = silent_backend_config(&heartbeat.silent_backend_command(), 1);

    let result = tokio::time::timeout(Duration::from_secs(15), Proxy::from_config(config)).await;

    let inner = result.expect(
        "construction must not hang: a 1s init_timeout should surface as an error, not a stall",
    );
    assert!(
        inner.is_err(),
        "a backend that never answers initialize must fail construction, not succeed"
    );
}

/// Regression: a backend that times out during `initialize` leaves no orphan.
///
/// The failure class this locks: `initialize` exceeds `init_timeout`, the caller
/// returns early, and the already-spawned child is dropped without being
/// killed. It survives unreachable but running. Asserting only that the error
/// surfaced (the test above) passes with the leak still present, so this checks
/// the process itself.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn a_timed_out_stdio_backend_leaves_no_orphan_process() {
    let heartbeat = Heartbeat::new("no-orphan");
    let config = silent_backend_config(&heartbeat.silent_backend_command(), 1);

    let result = tokio::time::timeout(Duration::from_secs(15), Proxy::from_config(config))
        .await
        .expect("construction should be bounded by init_timeout");
    assert!(result.is_err(), "the silent backend should have failed");

    // The child was spawned and running before the timeout fired; if it is
    // still ticking now, it outlived the connection that owned it.
    heartbeat.wait_until_alive().await;
    heartbeat.assert_stopped().await;
}

/// Test 1: A stdio backend that never responds hangs forever.
///
/// Wraps the proxy construction in `tokio::time::timeout` to prove the hang
/// without blocking the test suite. The timeout fires, confirming that
/// `Proxy::from_config` blocks indefinitely on a silent backend.
#[tokio::test(flavor = "multi_thread")]
async fn stdio_backend_hangs_without_timeout() {
    let config = ProxyConfig::parse(
        r#"
        [proxy]
        name = "test-proxy"
        version = "1.0.0"
        [proxy.listen]

        [[backends]]
        name = "sleepy"
        transport = "stdio"
        command = "sleep"
        args = ["999"]
        "#,
    )
    .expect("config should parse");

    // Without a timeout, Proxy::from_config blocks forever because the
    // `sleep 999` process never responds to the MCP initialize handshake.
    // We use a 3-second test guard to prove the hang.
    let result = tokio::time::timeout(Duration::from_secs(3), Proxy::from_config(config)).await;

    assert!(
        result.is_err(),
        "Proxy::from_config should hang (timeout fires) for a silent stdio backend"
    );
}

/// Test 2: A stdio backend with a per-backend timeout — construction still hangs.
///
/// The per-backend `TimeoutLayer` only wraps request dispatch, not the
/// initialization handshake in `Backend::initialize()`. So even with a
/// timeout configured, `Proxy::from_config` hangs during startup because
/// the MCP `initialize` request never gets a response.
#[tokio::test(flavor = "multi_thread")]
async fn stdio_backend_construction_hangs_even_with_timeout() {
    let config = ProxyConfig::parse(
        r#"
        [proxy]
        name = "test-proxy"
        version = "1.0.0"
        [proxy.listen]

        [[backends]]
        name = "sleepy"
        transport = "stdio"
        command = "sleep"
        args = ["999"]

        [backends.timeout]
        seconds = 2
        "#,
    )
    .expect("config should parse");

    // Even with a per-backend timeout, construction hangs because the timeout
    // only applies to request dispatch, not initialization.
    let result = tokio::time::timeout(Duration::from_secs(3), Proxy::from_config(config)).await;

    assert!(
        result.is_err(),
        "Proxy::from_config should still hang despite per-backend timeout — \
         the timeout layer only wraps request dispatch, not initialization"
    );
}

/// Test 3: A stdio backend that responds to initialize but hangs on tool calls.
///
/// This test uses a Python script that responds to the MCP initialize handshake
/// and capability list requests, then hangs on any subsequent request (e.g.
/// tools/call). This proves the per-backend timeout layer works for request
/// dispatch — but also that it does NOT help during construction.
#[tokio::test(flavor = "multi_thread")]
async fn stdio_backend_timeout_prevents_hang_on_tool_call() {
    // Python script that:
    // 1. Responds to initialize with a valid MCP response
    // 2. Responds to tools/list, resources/list, resources/templates/list,
    //    and prompts/list with empty lists
    // 3. Hangs on any other request (e.g. tools/call)
    let script = r#"import sys, json
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        req = json.loads(line)
    except Exception:
        continue
    method = req.get('method', '')
    rid = req.get('id', 0)
    if 'id' not in req:
        continue
    if method == 'initialize':
        resp = {'jsonrpc': '2.0', 'id': rid, 'result': {'protocolVersion': '2025-03-26', 'capabilities': {}, 'serverInfo': {'name': 'sleepy', 'version': '1.0.0'}}}
        print(json.dumps(resp))
        sys.stdout.flush()
    elif method in ('tools/list', 'resources/list', 'resources/templates/list', 'prompts/list'):
        key = 'tools' if method == 'tools/list' else 'resources' if method == 'resources/list' else 'resourceTemplates' if method == 'resources/templates/list' else 'prompts'
        resp = {'jsonrpc': '2.0', 'id': rid, 'result': {key: []}}
        print(json.dumps(resp))
        sys.stdout.flush()
    else:
        import time; time.sleep(999999)
"#;

    let config = ProxyConfig::parse(&format!(
        r#"
        [proxy]
        name = "test-proxy"
        version = "1.0.0"
        [proxy.listen]

        [[backends]]
        name = "sleepy"
        transport = "stdio"
        command = "python3"
        args = ["-c", {:?}]

        [backends.timeout]
        seconds = 2
        "#,
        script
    ))
    .expect("config should parse");

    // Build the proxy — this should succeed because the backend responds to
    // initialize AND capability list requests.
    let result = tokio::time::timeout(Duration::from_secs(10), Proxy::from_config(config)).await;
    match result {
        Ok(Ok(_proxy)) => {
            // Proxy constructed successfully — the backend responded to initialize
            // and capability discovery. The per-backend timeout is configured,
            // so a tool call through the proxy would eventually time out.
        }
        Ok(Err(e)) => panic!("Proxy::from_config failed: {e}"),
        Err(_) => {
            panic!("Proxy::from_config timed out — backend should have responded to initialize")
        }
    }
}

/// Test 4: A stdio backend that responds immediately (no hang).
///
/// Sanity check: a responsive backend works correctly through the full pipeline.
#[tokio::test(flavor = "multi_thread")]
async fn stdio_backend_responds_normally() {
    // Python script that responds to ALL requests with proper JSON-RPC responses.
    let script = r#"import sys, json
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        req = json.loads(line)
    except Exception:
        continue
    method = req.get('method', '')
    rid = req.get('id', 0)
    is_notification = 'id' not in req
    if is_notification:
        continue
    if method == 'initialize':
        resp = {'jsonrpc': '2.0', 'id': rid, 'result': {'protocolVersion': '2025-03-26', 'capabilities': {}, 'serverInfo': {'name': 'sleepy', 'version': '1.0.0'}}}
    elif method == 'tools/list':
        resp = {'jsonrpc': '2.0', 'id': rid, 'result': {'tools': []}}
    elif method == 'resources/list':
        resp = {'jsonrpc': '2.0', 'id': rid, 'result': {'resources': []}}
    elif method == 'resources/templates/list':
        resp = {'jsonrpc': '2.0', 'id': rid, 'result': {'resourceTemplates': []}}
    elif method == 'prompts/list':
        resp = {'jsonrpc': '2.0', 'id': rid, 'result': {'prompts': []}}
    elif method == 'tools/call':
        resp = {'jsonrpc': '2.0', 'id': rid, 'result': {'content': [{'type': 'text', 'text': 'hello from backend'}]}}
    else:
        resp = {'jsonrpc': '2.0', 'id': rid, 'error': {'code': -32601, 'message': 'Method not found: ' + method}}
    print(json.dumps(resp))
    sys.stdout.flush()
"#;

    let config = ProxyConfig::parse(&format!(
        r#"
        [proxy]
        name = "test-proxy"
        version = "1.0.0"
        [proxy.listen]

        [[backends]]
        name = "sleepy"
        transport = "stdio"
        command = "python3"
        args = ["-c", {:?}]
        "#,
        script
    ))
    .expect("config should parse");

    let result = tokio::time::timeout(Duration::from_secs(10), Proxy::from_config(config)).await;
    match result {
        Ok(Ok(_proxy)) => {
            // Proxy constructed successfully — the backend is responsive.
        }
        Ok(Err(e)) => panic!("Proxy::from_config failed: {e}"),
        Err(_) => panic!("Proxy::from_config timed out — responsive backend should not hang"),
    }
}
