//! Self-healing for HTTP backends whose health checks keep failing.
//!
//! The health checker ([`crate::admin::spawn_health_checker`]) pings every
//! backend and reports each result to a [`RecoveryManager`]. Once a backend has
//! failed [`RecoveryConfig::consecutive_failures`] checks in a row, the manager
//! starts a recovery task for it:
//!
//! 1. **Probe.** Connect to the backend with a throwaway client, run
//!    `initialize` and `ping`. If that fails the server really is down (or its
//!    ping is broken), and rebuilding the proxy's client could not help, so the
//!    existing backend is left in place.
//! 2. **Rebuild.** If the probe works, the proxy's client is stale: remove the
//!    backend and add it again from its config, which re-runs `initialize`
//!    and `tools/list` with the usual per-backend middleware.
//!
//! Attempts back off exponentially per backend, so a permanently broken
//! backend costs one probe every `max_backoff_ms` and nothing else. A backend
//! that was rebuilt [`MAX_INEFFECTIVE_REBUILDS`] times and is still unhealthy
//! is not rebuilt again until a health check passes. Backends are independent
//! of each other, and only HTTP backends are supervised.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tower_mcp::client::McpClient;
use tower_mcp::proxy::McpProxy;

use crate::config::{BackendConfig, ProxyConfig, RecoveryConfig};

/// Rebuilds after which a backend that is still unhealthy is left alone: the
/// client was not the problem.
const MAX_INEFFECTIVE_REBUILDS: u32 = 3;

/// Bound on a probe when the backend sets no `init_timeout`.
const DEFAULT_PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// What the proxy exposes about a backend's recovery.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecoveryStatus {
    /// Recovery attempts since the backend was last healthy.
    pub attempts: u32,
    /// Why the last attempt failed, until the next one succeeds.
    pub last_error: Option<String>,
}

/// Supervises HTTP backends and rebuilds the ones that stay unhealthy.
///
/// Cheap to clone; clones share state.
#[derive(Clone)]
pub struct RecoveryManager {
    inner: Arc<Inner>,
}

struct Inner {
    proxy: McpProxy,
    runtime: tokio::runtime::Handle,
    plans: Mutex<HashMap<String, Arc<Plan>>>,
    states: Mutex<HashMap<String, State>>,
}

/// How to recover one backend.
struct Plan {
    backend: BackendConfig,
    recovery: RecoveryConfig,
    kill_timeout_secs: u64,
    force_kill: bool,
    /// Serialized `backend`, to tell whether a reload changed it.
    fingerprint: String,
}

#[derive(Default)]
struct State {
    attempts: u32,
    /// Rebuilds since the backend was last healthy.
    rebuilds: u32,
    last_error: Option<String>,
    next_attempt_at: Option<Instant>,
    /// A recovery task is running for this backend.
    in_flight: bool,
    gave_up: bool,
    /// Bumped whenever the backend turns out healthy; a task from an earlier
    /// epoch stops.
    epoch: u64,
}

impl State {
    /// Note a successful rebuild. Returns `true` when the backend has now been
    /// rebuilt too often without getting healthy and recovery should stop.
    fn record_rebuild(&mut self) -> bool {
        self.last_error = None;
        self.rebuilds += 1;
        self.gave_up = self.rebuilds >= MAX_INEFFECTIVE_REBUILDS;
        self.gave_up
    }
}

impl RecoveryManager {
    /// Create a manager for the HTTP backends in `config`.
    ///
    /// Must be called from within a Tokio runtime: recovery tasks run on it.
    pub fn new(proxy: McpProxy, config: &ProxyConfig) -> Self {
        let manager = Self {
            inner: Arc::new(Inner {
                proxy,
                runtime: tokio::runtime::Handle::current(),
                plans: Mutex::new(HashMap::new()),
                states: Mutex::new(HashMap::new()),
            }),
        };
        manager.sync(config);
        manager
    }

    /// Adopt the backends of a (re)loaded config. Backends that are gone or
    /// no longer supervised lose their recovery state.
    pub fn sync(&self, config: &ProxyConfig) {
        let mut plans = self.inner.plans.lock().expect("recovery plans poisoned");
        let mut next = HashMap::new();
        for backend in config.backends.iter().filter(|b| b.enabled) {
            let Some(recovery) = backend.effective_recovery() else {
                continue;
            };
            let fingerprint = toml::to_string(backend).unwrap_or_default();
            let plan = match plans.remove(&backend.name) {
                Some(old) if old.fingerprint == fingerprint && old.recovery == recovery => old,
                _ => Arc::new(Plan {
                    backend: backend.clone(),
                    recovery,
                    kill_timeout_secs: config.proxy.shutdown_kill_timeout_secs,
                    force_kill: config.proxy.force_kill,
                    fingerprint,
                }),
            };
            next.insert(backend.name.clone(), plan);
        }
        *plans = next;
        self.inner
            .states
            .lock()
            .expect("recovery states poisoned")
            .retain(|name, _| plans.contains_key(name));
    }

    /// Recovery progress of `namespace` (all zero for an unsupervised or
    /// healthy backend).
    pub fn status(&self, namespace: &str) -> RecoveryStatus {
        let states = self.inner.states.lock().expect("recovery states poisoned");
        states
            .get(namespace)
            .map(|s| RecoveryStatus {
                attempts: s.attempts,
                last_error: s.last_error.clone(),
            })
            .unwrap_or_default()
    }

    /// Report one health check result. `consecutive_failures` counts failed
    /// checks in a row, including this one.
    pub fn observe(&self, namespace: &str, healthy: bool, consecutive_failures: u32) {
        let Some(plan) = self
            .inner
            .plans
            .lock()
            .expect("recovery plans poisoned")
            .get(namespace)
            .cloned()
        else {
            return;
        };

        let mut states = self.inner.states.lock().expect("recovery states poisoned");
        let state = states.entry(namespace.to_string()).or_default();

        if healthy {
            if state.attempts > 0 {
                tracing::info!(
                    backend = namespace,
                    attempts = state.attempts,
                    "Backend is healthy again"
                );
            }
            *state = State {
                epoch: state.epoch + 1,
                in_flight: state.in_flight,
                ..State::default()
            };
            return;
        }

        if consecutive_failures < plan.recovery.consecutive_failures
            || state.in_flight
            || state.gave_up
        {
            return;
        }
        state.in_flight = true;
        let epoch = state.epoch;
        drop(states);

        let inner = Arc::clone(&self.inner);
        let namespace = namespace.to_string();
        self.inner.runtime.spawn(async move {
            inner.recover(&namespace, epoch).await;
        });
    }
}

/// Wait before the attempt after number `attempt` (1-based): `initial`,
/// doubling each time, capped at `max`.
fn backoff_for(attempt: u32, initial_ms: u64, max_ms: u64) -> Duration {
    let doublings = attempt.saturating_sub(1).min(32);
    let ms = initial_ms.saturating_mul(1u64 << doublings).min(max_ms);
    Duration::from_millis(ms)
}

impl Inner {
    /// Retry recovery of `namespace` until it succeeds, the backend turns
    /// healthy by itself (`epoch` changes) or it is no longer supervised.
    async fn recover(&self, namespace: &str, epoch: u64) {
        loop {
            let wait = {
                let states = self.states.lock().expect("recovery states poisoned");
                states
                    .get(namespace)
                    .and_then(|s| s.next_attempt_at)
                    .map(|at| at.saturating_duration_since(Instant::now()))
            };
            if let Some(wait) = wait.filter(|w| !w.is_zero()) {
                tokio::time::sleep(wait).await;
            }

            // No plan means a reload dropped the backend, and its state with it.
            let Some(plan) = plan_of(self, namespace) else {
                return;
            };
            let attempt = {
                let mut states = self.states.lock().expect("recovery states poisoned");
                let Some(state) = states.get_mut(namespace) else {
                    return;
                };
                if state.epoch != epoch {
                    state.in_flight = false;
                    return;
                }
                state.attempts += 1;
                state.attempts
            };

            tracing::info!(
                backend = namespace,
                attempt,
                "Recovery attempt: health checks keep failing"
            );
            let outcome = self.attempt(&plan).await;

            let backoff = backoff_for(
                attempt,
                plan.recovery.initial_backoff_ms,
                plan.recovery.max_backoff_ms,
            );
            let mut states = self.states.lock().expect("recovery states poisoned");
            let Some(state) = states.get_mut(namespace) else {
                return;
            };
            if state.epoch != epoch {
                // Healthy again while the attempt ran; its outcome is moot.
                state.in_flight = false;
                return;
            }
            state.next_attempt_at = Some(Instant::now() + backoff);
            match outcome {
                Ok(()) => {
                    tracing::info!(backend = namespace, attempt, "Recovery succeeded");
                    if state.record_rebuild() {
                        tracing::warn!(
                            backend = namespace,
                            rebuilds = state.rebuilds,
                            "Backend still fails health checks after being rebuilt; \
                             not rebuilding it again until a health check passes"
                        );
                    }
                    state.in_flight = false;
                    return;
                }
                Err(error) => {
                    tracing::info!(
                        backend = namespace,
                        attempt,
                        %error,
                        retry_in_ms = backoff.as_millis() as u64,
                        "Recovery failed"
                    );
                    state.last_error = Some(error);
                }
            }
        }
    }

    /// One recovery attempt: prove a fresh client works, then swap it in.
    async fn attempt(&self, plan: &Arc<Plan>) -> Result<(), String> {
        probe(&plan.backend)
            .await
            .map_err(|e| format!("fresh connection failed: {e}"))?;

        // A reload while probing may have replaced or removed this backend.
        let current = self
            .plans
            .lock()
            .expect("recovery plans poisoned")
            .get(&plan.backend.name)
            .is_some_and(|p| Arc::ptr_eq(p, plan));
        if !current {
            return Err("backend config changed during recovery".to_string());
        }

        self.proxy.remove_backend(&plan.backend.name).await;
        crate::reload::add_backend(
            &self.proxy,
            &plan.backend,
            plan.kill_timeout_secs,
            plan.force_kill,
        )
        .await
        .map_err(|e| format!("re-adding backend failed: {e}"))
    }
}

fn plan_of(inner: &Inner, namespace: &str) -> Option<Arc<Plan>> {
    inner
        .plans
        .lock()
        .expect("recovery plans poisoned")
        .get(namespace)
        .cloned()
}

/// Connect to the backend with a throwaway client, initialize it and ping it.
async fn probe(backend: &BackendConfig) -> Result<(), String> {
    let url = backend
        .url
        .as_deref()
        .ok_or_else(|| "http backend requires 'url'".to_string())?;
    let transport = crate::proxy::build_http_transport(url, backend).map_err(|e| e.to_string())?;
    let limit = backend
        .init_timeout
        .map_or(DEFAULT_PROBE_TIMEOUT, Duration::from_secs);

    let client = McpClient::connect(transport)
        .await
        .map_err(|e| e.to_string())?;
    let result = tokio::time::timeout(limit, async {
        client
            .initialize("mcp-proxy-recovery", env!("CARGO_PKG_VERSION"))
            .await
            .map_err(|e| format!("initialize: {e}"))?;
        client.ping().await.map_err(|e| format!("ping: {e}"))
    })
    .await
    .unwrap_or_else(|_| Err(format!("timed out after {}s", limit.as_secs())));
    let _ = client.shutdown().await;
    result.map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use axum::Router;
    use axum::extract::State as AxumState;
    use axum::http::{StatusCode, header};
    use axum::response::{IntoResponse, Response};
    use axum::routing::post;
    use serde_json::{Value, json};
    use tower_mcp::client::{ClientTransport, HttpClientTransport};

    use crate::admin::{BackendMeta, BackendStatus, spawn_health_checker_with};

    #[test]
    fn backoff_doubles_and_is_capped() {
        let ms = |attempt| backoff_for(attempt, 100, 1_000).as_millis();
        assert_eq!(ms(1), 100);
        assert_eq!(ms(2), 200);
        assert_eq!(ms(3), 400);
        assert_eq!(ms(4), 800);
        assert_eq!(ms(5), 1_000);
        assert_eq!(ms(500), 1_000, "must not overflow");
    }

    #[test]
    fn rebuilds_that_do_not_help_stop_after_a_few() {
        let mut state = State::default();
        for _ in 1..MAX_INEFFECTIVE_REBUILDS {
            assert!(!state.record_rebuild());
        }
        assert!(state.record_rebuild());
        assert!(state.gave_up);
    }

    // ---- a stateless HTTP backend that can go down and come back ----

    #[derive(Clone, Default)]
    struct Counters {
        initializes: Arc<AtomicUsize>,
        tools_lists: Arc<AtomicUsize>,
        ping_404: Arc<AtomicBool>,
    }

    async fn handle(AxumState(counters): AxumState<Counters>, body: String) -> Response {
        let request: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
        let id = request.get("id").cloned();
        let method = request.get("method").and_then(Value::as_str).unwrap_or("");
        let result = match method {
            "initialize" => {
                counters.initializes.fetch_add(1, Ordering::SeqCst);
                json!({
                    "protocolVersion": request["params"]["protocolVersion"].clone(),
                    "capabilities": { "tools": {} },
                    "serverInfo": { "name": "mock", "version": "1.0.0" },
                })
            }
            "ping" if counters.ping_404.load(Ordering::SeqCst) => {
                // What a misconfigured Java backend answers.
                return (
                    StatusCode::NOT_FOUND,
                    "java.lang.IllegalStateException: no handler\n\tat com.example.Servlet",
                )
                    .into_response();
            }
            "ping" => json!({}),
            "tools/list" => {
                counters.tools_lists.fetch_add(1, Ordering::SeqCst);
                json!({ "tools": [] })
            }
            _ if id.is_none() => {
                return (StatusCode::ACCEPTED, [(header::CONNECTION, "close")]).into_response();
            }
            _ => json!({}),
        };
        (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "application/json"),
                (header::CONNECTION, "close"),
            ],
            json!({ "jsonrpc": "2.0", "id": id, "result": result }).to_string(),
        )
            .into_response()
    }

    struct MockBackend {
        addr: SocketAddr,
        counters: Counters,
        task: Option<tokio::task::JoinHandle<()>>,
    }

    impl MockBackend {
        async fn started() -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            drop(listener);
            let mut backend = Self {
                addr,
                counters: Counters::default(),
                task: None,
            };
            backend.start().await;
            backend
        }

        async fn start(&mut self) {
            let listener = tokio::net::TcpListener::bind(self.addr).await.unwrap();
            let app = Router::new()
                .route("/mcp", post(handle))
                .with_state(self.counters.clone());
            self.task = Some(tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            }));
        }

        async fn stop(&mut self) {
            if let Some(task) = self.task.take() {
                task.abort();
                let _ = task.await;
            }
        }

        fn initializes(&self) -> usize {
            self.counters.initializes.load(Ordering::SeqCst)
        }

        fn tools_lists(&self) -> usize {
            self.counters.tools_lists.load(Ordering::SeqCst)
        }
    }

    impl Drop for MockBackend {
        fn drop(&mut self) {
            if let Some(task) = self.task.take() {
                task.abort();
            }
        }
    }

    /// Stands in for a client that died for good (what an HTTP client used to
    /// do after one transient send error): once killed, every send fails and
    /// the transport reports itself disconnected, so the client's message
    /// loop ends and later requests fail with "Connection closed".
    struct KillableTransport {
        inner: HttpClientTransport,
        killed: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl ClientTransport for KillableTransport {
        async fn send(&mut self, message: &str) -> tower_mcp::error::Result<()> {
            if self.killed.load(Ordering::SeqCst) {
                return Err(tower_mcp::error::Error::Transport("killed".to_string()));
            }
            self.inner.send(message).await
        }

        async fn recv(&mut self) -> tower_mcp::error::Result<Option<String>> {
            self.inner.recv().await
        }

        fn is_connected(&self) -> bool {
            !self.killed.load(Ordering::SeqCst) && self.inner.is_connected()
        }

        async fn close(&mut self) -> tower_mcp::error::Result<()> {
            self.inner.close().await
        }
    }

    fn config_for(backends: &[(&str, &MockBackend)]) -> ProxyConfig {
        let mut toml = String::from(
            "[proxy]\nname = \"test\"\n\n[proxy.listen]\nhost = \"127.0.0.1\"\nport = 0\n",
        );
        for (name, backend) in backends {
            toml.push_str(&format!(
                "[[backends]]\nname = \"{name}\"\ntransport = \"http\"\n\
                 url = \"http://{}/mcp\"\n\
                 [backends.recovery]\nconsecutive_failures = 2\n\
                 initial_backoff_ms = 100\nmax_backoff_ms = 200\n",
                backend.addr
            ));
        }
        let mut config = ProxyConfig::parse(&toml).unwrap();
        config.apply_global_defaults();
        config
    }

    /// Add the backend to the proxy with a client that can be killed.
    async fn add_killable(proxy: &McpProxy, config: &ProxyConfig, name: &str) -> Arc<AtomicBool> {
        let backend = config.backends.iter().find(|b| b.name == name).unwrap();
        let killed = Arc::new(AtomicBool::new(false));
        let transport = KillableTransport {
            inner: crate::proxy::build_http_transport(backend.url.as_deref().unwrap(), backend)
                .unwrap(),
            killed: killed.clone(),
        };
        proxy.add_backend(name, transport).await.unwrap();
        killed
    }

    async fn add_plain(proxy: &McpProxy, config: &ProxyConfig, name: &str) {
        let backend = config.backends.iter().find(|b| b.name == name).unwrap();
        crate::reload::add_backend(proxy, backend, 0, false)
            .await
            .unwrap();
    }

    async fn empty_proxy() -> McpProxy {
        McpProxy::builder("test", "1.0.0")
            .build_strict()
            .await
            .unwrap()
    }

    /// Run the health checker with a short interval, as in production.
    fn supervise(proxy: &McpProxy, config: &ProxyConfig) -> crate::admin::AdminState {
        let meta = config
            .backends
            .iter()
            .map(|b| {
                (
                    b.name.clone(),
                    BackendMeta {
                        transport: "http".into(),
                    },
                )
            })
            .collect();
        spawn_health_checker_with(
            proxy.clone(),
            "test".into(),
            "1.0.0".into(),
            config.backends.len(),
            meta,
            Some(RecoveryManager::new(proxy.clone(), config)),
            Duration::from_millis(20),
        )
    }

    async fn status_of(admin: &crate::admin::AdminState, name: &str) -> Option<BackendStatus> {
        admin
            .health()
            .await
            .into_iter()
            .find(|b| b.namespace == name)
    }

    /// The scenario from production, with the client dead and the server fine:
    /// the backend is rebuilt and becomes healthy again, and a neighbour is
    /// never touched.
    #[tokio::test]
    async fn dead_client_is_rebuilt_when_the_server_is_healthy() {
        let hive = MockBackend::started().await;
        let other = MockBackend::started().await;
        let config = config_for(&[("hive", &hive), ("other", &other)]);
        let proxy = empty_proxy().await;
        let killed = add_killable(&proxy, &config, "hive").await;
        add_plain(&proxy, &config, "other").await;
        assert_eq!((hive.initializes(), hive.tools_lists()), (1, 1));

        killed.store(true, Ordering::SeqCst);
        let admin = supervise(&proxy, &config);

        let mut healed = false;
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline && !healed {
            // The rebuild lists the tools again; healthy means pings work.
            healed = hive.tools_lists() == 2
                && status_of(&admin, "hive").await.is_some_and(|s| s.healthy);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(healed, "hive was not rebuilt");
        // Healthy again, so the recovery bookkeeping starts over.
        let hive_status = status_of(&admin, "hive").await.unwrap();
        assert_eq!(hive_status.recovery_attempts, 0);
        assert_eq!(hive_status.recovery_last_error, None);

        // Probe (initialize) plus rebuild (initialize, tools/list).
        assert_eq!(hive.tools_lists(), 2, "hive must be re-added exactly once");
        assert_eq!(
            (other.initializes(), other.tools_lists()),
            (1, 1),
            "a healthy neighbour must not be touched"
        );
        let other_status = status_of(&admin, "other").await.unwrap();
        assert!(other_status.healthy);
        assert_eq!(other_status.recovery_attempts, 0);
    }

    /// With the fixed HTTP client a restart of a stateless backend needs no
    /// help at all: the same client works again as soon as the server does, and
    /// recovery has nothing to rebuild.
    #[tokio::test]
    async fn restarted_server_is_picked_up_by_the_existing_client() {
        let mut hive = MockBackend::started().await;
        let config = config_for(&[("hive", &hive)]);
        let proxy = empty_proxy().await;
        add_plain(&proxy, &config, "hive").await;
        let admin = supervise(&proxy, &config);

        hive.stop().await;
        let mut down = false;
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline && !down {
            down = status_of(&admin, "hive")
                .await
                .is_some_and(|s| s.consecutive_failures >= 5);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(down, "outage was not noticed");

        hive.start().await;
        let mut healed = false;
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline && !healed {
            healed = status_of(&admin, "hive").await.is_some_and(|s| s.healthy);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(healed, "hive did not come back");
        assert_eq!(
            hive.tools_lists(),
            1,
            "the client must not have been rebuilt"
        );
    }

    /// While the server is down nothing can be rebuilt: the old backend stays
    /// registered, attempts and the last error are reported, and once the
    /// server returns the backend heals.
    #[tokio::test]
    async fn server_down_leaves_backend_in_place_and_reports_attempts() {
        let mut hive = MockBackend::started().await;
        let config = config_for(&[("hive", &hive)]);
        let proxy = empty_proxy().await;
        let killed = add_killable(&proxy, &config, "hive").await;

        killed.store(true, Ordering::SeqCst);
        hive.stop().await;
        let admin = supervise(&proxy, &config);

        let mut last = None;
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            let status = status_of(&admin, "hive").await;
            if status.as_ref().is_some_and(|s| s.recovery_attempts >= 2) {
                last = status;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let status = last.expect("recovery attempts never reported");
        assert!(!status.healthy);
        assert!(
            status
                .recovery_last_error
                .as_deref()
                .is_some_and(|e| e.contains("fresh connection failed")),
            "unexpected error: {:?}",
            status.recovery_last_error
        );
        assert!(proxy.backend_namespaces().contains(&"hive".to_string()));
        assert_eq!(hive.tools_lists(), 1, "nothing may be rebuilt while down");

        hive.start().await;
        let mut healed = false;
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline && !healed {
            healed = status_of(&admin, "hive").await.is_some_and(|s| s.healthy);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(healed, "hive did not come back after the server returned");
    }

    /// A backend whose ping is permanently broken (404 and a stack trace) while
    /// everything else works must not be torn down over and over, and must be
    /// retried at a backed-off pace.
    #[tokio::test]
    async fn permanently_broken_ping_is_never_rebuilt_and_backs_off() {
        let hive = MockBackend::started().await;
        let config = config_for(&[("hive", &hive)]);
        let proxy = empty_proxy().await;
        add_plain(&proxy, &config, "hive").await;

        hive.counters.ping_404.store(true, Ordering::SeqCst);
        let admin = supervise(&proxy, &config);

        tokio::time::sleep(Duration::from_millis(1_200)).await;
        let status = status_of(&admin, "hive").await.unwrap();
        assert!(!status.healthy);
        assert!(
            status.recovery_attempts >= 2,
            "{}",
            status.recovery_attempts
        );
        // Backoff 100ms, 200ms, 200ms, ...: about 8 probes fit in 1.2s.
        // Retrying on every 20ms health check would be ~60.
        assert!(
            status.recovery_attempts <= 12,
            "{}",
            status.recovery_attempts
        );
        assert!(
            status
                .recovery_last_error
                .as_deref()
                .is_some_and(|e| e.contains("ping")),
            "{:?}",
            status.recovery_last_error
        );
        // Only the initial add listed tools: no rebuild ever happened.
        assert_eq!(hive.tools_lists(), 1);
        assert!(proxy.backend_namespaces().contains(&"hive".to_string()));
    }

    #[tokio::test]
    async fn recovery_is_off_for_stdio_and_when_disabled() {
        let toml = r#"
            [proxy]
            name = "test"

            [proxy.listen]
            host = "127.0.0.1"
            port = 0

            [[backends]]
            name = "local"
            transport = "stdio"
            command = "true"

            [[backends]]
            name = "off"
            transport = "http"
            url = "http://127.0.0.1:1/mcp"
            [backends.recovery]
            enabled = false

            [[backends]]
            name = "on"
            transport = "http"
            url = "http://127.0.0.1:1/mcp"
        "#;
        let config = ProxyConfig::parse(toml).unwrap();
        let manager = RecoveryManager::new(empty_proxy().await, &config);
        for name in ["local", "off"] {
            manager.observe(name, false, 1_000);
            assert_eq!(manager.status(name), RecoveryStatus::default());
        }
        let plans = manager.inner.plans.lock().unwrap();
        assert!(plans.contains_key("on"));
        assert!(!plans.contains_key("off") && !plans.contains_key("local"));
        let on = &plans["on"].recovery;
        assert_eq!(
            (
                on.consecutive_failures,
                on.initial_backoff_ms,
                on.max_backoff_ms
            ),
            (3, 5_000, 300_000)
        );
    }

    #[tokio::test]
    async fn healthy_backend_below_threshold_starts_nothing() {
        let hive = MockBackend::started().await;
        let config = config_for(&[("hive", &hive)]);
        let manager = RecoveryManager::new(empty_proxy().await, &config);

        manager.observe("hive", false, 1);
        assert!(!manager.inner.states.lock().unwrap()["hive"].in_flight);
        manager.observe("hive", true, 0);
        assert_eq!(manager.status("hive"), RecoveryStatus::default());
    }
}
