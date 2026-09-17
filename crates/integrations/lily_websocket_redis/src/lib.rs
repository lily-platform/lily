#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
#![deny(clippy::print_stderr, clippy::print_stdout)]

//! Redis Pub/Sub adapter for Lily's WebSocket backplane SPI.
//!
//! [`RedisWebSocketBackplane`] is selected explicitly through
//! `WsAppBuilder::backplane`. It resolves the application [`ConfigService`]
//! from the existing DI container and consumes `[websocket.backplane]`; there
//! is no constructor argument, second config authority, cache-pool reuse or
//! hidden DI container.
//!
//! Publisher and subscriber connections are dedicated and independently
//! reconnected with bounded exponential backoff. Publish admission and Redis
//! push ingestion are bounded. Redis Pub/Sub remains online and non-durable:
//! this adapter does not provide offline delivery, history, replay, global
//! acknowledgements, exact global ordering or exactly-once delivery.
//! The private channel uses Lily's `v4` routing-protocol generation. Mixed
//! protocol generations are intentionally channel-isolated and require a
//! coordinated deployment cutover.

use std::fmt;
use std::panic::AssertUnwindSafe;
use std::path::{Component, Path};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::FutureExt as _;
use lily_config::{ConfigService, RedisWebSocketBackplaneConfig};
use lily_injection::Extensions;
use lily_websocket::{
    WebSocketBackplane, WebSocketBackplaneError, WebSocketBackplaneErrorKind,
    WebSocketBackplaneEvent, WebSocketBackplaneFrame, WebSocketBackplaneInboundAdmission,
    WebSocketBackplaneInitError, WebSocketBackplaneInitErrorKind, WebSocketBackplanePublishReceipt,
};
use redis::aio::MultiplexedConnection;
use redis::{
    AsyncConnectionConfig, ConnectionAddr, IntoConnectionInfo, ProtocolVersion, PushInfo, PushKind,
    Value,
};
use tokio::io::AsyncReadExt as _;
use tokio::sync::{Mutex, Notify, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior, interval_at, sleep, timeout, timeout_at};
use tokio_util::sync::CancellationToken;

const MAX_CA_BUNDLE_BYTES: u64 = 1024 * 1024;
const MAX_CA_CERTIFICATES: usize = 32;
const CONTROL_EVENT_SLOTS: usize = 8;
const MIN_TRANSPORT_LIVENESS_INTERVAL: Duration = Duration::from_millis(250);
const MAX_TRANSPORT_LIVENESS_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlanError {
    Configuration,
    Transport,
}

#[derive(Clone)]
struct RedisBackplanePlan {
    client: redis::Client,
    channel: Arc<str>,
    publish_capacity: usize,
    ingress_capacity: usize,
    connection_timeout: Duration,
    operation_timeout: Duration,
    reconnect_initial_delay: Duration,
    reconnect_max_delay: Duration,
    reconnect_jitter_ratio: f64,
}

impl fmt::Debug for RedisBackplanePlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisBackplanePlan")
            .field("redis_endpoint", &"<redacted>")
            .field("channel", &self.channel)
            .field("publish_capacity", &self.publish_capacity)
            .field("ingress_capacity", &self.ingress_capacity)
            .field("connection_timeout", &self.connection_timeout)
            .field("operation_timeout", &self.operation_timeout)
            .field("reconnect_initial_delay", &self.reconnect_initial_delay)
            .field("reconnect_max_delay", &self.reconnect_max_delay)
            .field("reconnect_jitter_ratio", &self.reconnect_jitter_ratio)
            .finish()
    }
}

impl RedisBackplanePlan {
    async fn from_config(config: &RedisWebSocketBackplaneConfig) -> Result<Self, PlanError> {
        let mut connection = config
            .redis_url
            .as_str()
            .into_connection_info()
            .map_err(|_| PlanError::Configuration)?;
        match &connection.addr {
            ConnectionAddr::Tcp(_, _) if config.use_tls => {
                return Err(PlanError::Configuration);
            }
            ConnectionAddr::TcpTls { insecure: true, .. } => {
                return Err(PlanError::Configuration);
            }
            ConnectionAddr::TcpTls { .. } if !config.use_tls => {
                return Err(PlanError::Configuration);
            }
            ConnectionAddr::Unix(_) => return Err(PlanError::Configuration),
            _ => {}
        }
        connection.redis.protocol = ProtocolVersion::RESP3;

        let root_cert = load_custom_ca(config.custom_ca_bundle.as_deref()).await?;
        if root_cert.is_some() && !config.use_tls {
            return Err(PlanError::Configuration);
        }
        let client = match root_cert {
            Some(root_cert) => redis::Client::build_with_tls(
                connection,
                redis::TlsCertificates {
                    client_tls: None,
                    root_cert: Some(root_cert),
                },
            )
            .map_err(|_| PlanError::Configuration)?,
            None => redis::Client::open(connection).map_err(|_| PlanError::Configuration)?,
        };

        Ok(Self {
            client,
            channel: Arc::from(format!(
                "lily.websocket.v4.{}.{}.{}",
                config.environment_namespace,
                config.application_namespace,
                config.channel_namespace
            )),
            publish_capacity: config.publish_capacity,
            ingress_capacity: config.ingress_capacity,
            connection_timeout: Duration::from_millis(config.connection_timeout_millis),
            operation_timeout: Duration::from_millis(config.operation_timeout_millis),
            reconnect_initial_delay: Duration::from_millis(config.reconnect_initial_delay_millis),
            reconnect_max_delay: Duration::from_millis(config.reconnect_max_delay_millis),
            reconnect_jitter_ratio: config.reconnect_jitter_ratio,
        })
    }

    fn connection_config(&self) -> AsyncConnectionConfig {
        AsyncConnectionConfig::new()
            .set_connection_timeout(self.connection_timeout)
            .set_response_timeout(self.operation_timeout)
    }
}

async fn load_custom_ca(path: Option<&Path>) -> Result<Option<Vec<u8>>, PlanError> {
    let Some(path) = path else {
        return Ok(None);
    };

    let pem = read_bounded_ca_file(path).await?;
    validate_ca_pem(&pem)?;
    Ok(Some(pem))
}

async fn read_bounded_ca_file(path: &Path) -> Result<Vec<u8>, PlanError> {
    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(PlanError::Configuration);
    }
    let path_metadata = tokio::fs::symlink_metadata(path)
        .await
        .map_err(|_| PlanError::Configuration)?;
    if path_metadata.file_type().is_symlink() || !path_metadata.is_file() {
        return Err(PlanError::Configuration);
    }
    let canonical = tokio::fs::canonicalize(path)
        .await
        .map_err(|_| PlanError::Configuration)?;
    if canonical != path {
        return Err(PlanError::Configuration);
    }

    let file = tokio::fs::File::open(path)
        .await
        .map_err(|_| PlanError::Configuration)?;
    let opened_metadata = file
        .metadata()
        .await
        .map_err(|_| PlanError::Configuration)?;
    if !opened_metadata.is_file() {
        return Err(PlanError::Configuration);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;

        if path_metadata.dev() != opened_metadata.dev()
            || path_metadata.ino() != opened_metadata.ino()
        {
            return Err(PlanError::Configuration);
        }
    }
    let canonical_after_open = tokio::fs::canonicalize(path)
        .await
        .map_err(|_| PlanError::Configuration)?;
    if canonical_after_open != path {
        return Err(PlanError::Configuration);
    }

    let initial_capacity = usize::try_from(opened_metadata.len().min(MAX_CA_BUNDLE_BYTES))
        .map_err(|_| PlanError::Configuration)?;
    let mut pem = Vec::with_capacity(initial_capacity);
    let mut bounded_file = file.take(MAX_CA_BUNDLE_BYTES + 1);
    bounded_file
        .read_to_end(&mut pem)
        .await
        .map_err(|_| PlanError::Configuration)?;
    if pem.is_empty()
        || pem.len() > usize::try_from(MAX_CA_BUNDLE_BYTES).map_err(|_| PlanError::Configuration)?
    {
        return Err(PlanError::Configuration);
    }
    Ok(pem)
}

fn validate_ca_pem(pem: &[u8]) -> Result<(), PlanError> {
    use rustls_pki_types::pem::{PemObject as _, SectionKind};

    let mut certificates = 0_usize;
    for item in <(SectionKind, Vec<u8>)>::pem_slice_iter(pem) {
        match item.map_err(|_| PlanError::Configuration)? {
            (SectionKind::Certificate, _) => certificates += 1,
            _ => return Err(PlanError::Configuration),
        }
        if certificates > MAX_CA_CERTIFICATES {
            return Err(PlanError::Configuration);
        }
    }
    if certificates == 0 {
        return Err(PlanError::Configuration);
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublisherAttemptError {
    TimedOut,
    Unavailable,
    Publish,
}

#[async_trait]
trait PublisherTransport: Send {
    async fn ping(&mut self, deadline: Duration) -> Result<(), PublisherAttemptError>;

    async fn publish(
        &mut self,
        channel: &str,
        frame: &[u8],
        deadline: Instant,
    ) -> Result<(), PublisherAttemptError>;
}

#[async_trait]
impl PublisherTransport for MultiplexedConnection {
    async fn ping(&mut self, deadline: Duration) -> Result<(), PublisherAttemptError> {
        let pong = timeout(deadline, redis::cmd("PING").query_async::<String>(self))
            .await
            .map_err(|_| PublisherAttemptError::TimedOut)?
            .map_err(|_| PublisherAttemptError::Unavailable)?;
        if pong != "PONG" {
            return Err(PublisherAttemptError::Unavailable);
        }
        Ok(())
    }

    async fn publish(
        &mut self,
        channel: &str,
        frame: &[u8],
        deadline: Instant,
    ) -> Result<(), PublisherAttemptError> {
        let mut command = redis::cmd("PUBLISH");
        command.arg(channel).arg(frame);
        match timeout_at(deadline, command.query_async::<usize>(self)).await {
            Err(_) => Err(PublisherAttemptError::TimedOut),
            Ok(Ok(_)) => Ok(()),
            Ok(Err(error)) if error.is_unrecoverable_error() => {
                Err(PublisherAttemptError::Unavailable)
            }
            Ok(Err(_)) => Err(PublisherAttemptError::Publish),
        }
    }
}

#[async_trait]
trait PublisherConnector: Send + Sync {
    async fn connect(
        &self,
        plan: &RedisBackplanePlan,
    ) -> Result<Box<dyn PublisherTransport>, PlanError>;
}

struct RedisPublisherConnector;

#[async_trait]
impl PublisherConnector for RedisPublisherConnector {
    async fn connect(
        &self,
        plan: &RedisBackplanePlan,
    ) -> Result<Box<dyn PublisherTransport>, PlanError> {
        let connection = timeout(
            plan.connection_timeout,
            plan.client
                .get_multiplexed_async_connection_with_config(&plan.connection_config()),
        )
        .await
        .map_err(|_| PlanError::Transport)?
        .map_err(|_| PlanError::Transport)?;
        let mut connection: Box<dyn PublisherTransport> = Box::new(connection);
        connection
            .ping(plan.operation_timeout)
            .await
            .map_err(|_| PlanError::Transport)?;
        Ok(connection)
    }
}

fn transport_liveness_interval(plan: &RedisBackplanePlan) -> Duration {
    plan.operation_timeout.clamp(
        MIN_TRANSPORT_LIVENESS_INTERVAL,
        MAX_TRANSPORT_LIVENESS_INTERVAL,
    )
}

enum PublishFrame {
    Opaque(WebSocketBackplaneFrame),
    #[cfg(test)]
    Fixture(Arc<[u8]>),
}

impl PublishFrame {
    fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Opaque(frame) => frame.as_bytes(),
            #[cfg(test)]
            Self::Fixture(bytes) => bytes,
        }
    }
}

struct PublishRequest {
    frame: PublishFrame,
    generation: u64,
    deadline: Instant,
    reply: oneshot::Sender<Result<WebSocketBackplanePublishReceipt, WebSocketBackplaneError>>,
}

struct PublisherAdmission {
    generation: AtomicU64,
    available: AtomicBool,
}

impl PublisherAdmission {
    fn ready() -> Self {
        Self {
            generation: AtomicU64::new(0),
            available: AtomicBool::new(true),
        }
    }

    fn admitted_generation(&self) -> Option<u64> {
        let generation = self.generation.load(Ordering::Acquire);
        self.available.load(Ordering::Acquire).then_some(generation)
    }

    fn accepts(&self, generation: u64) -> bool {
        self.available.load(Ordering::Acquire)
            && self.generation.load(Ordering::Acquire) == generation
    }

    fn mark_unavailable(&self) {
        if self.available.swap(false, Ordering::AcqRel) {
            self.generation.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn mark_ready(&self) {
        self.available.store(true, Ordering::Release);
    }
}

fn reject_publish(request: PublishRequest, kind: WebSocketBackplaneErrorKind) {
    let _ = request.reply.send(Err(WebSocketBackplaneError::new(kind)));
}

async fn send_event(
    events: &mpsc::Sender<WebSocketBackplaneEvent>,
    cancellation: &CancellationToken,
    event: WebSocketBackplaneEvent,
) -> bool {
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => false,
        result = events.send(event) => result.is_ok(),
    }
}

fn next_backoff(current: Duration, maximum: Duration) -> Duration {
    current.saturating_mul(2).min(maximum)
}

fn subscriber_retry_delay_after_failure(
    attempted_delay: Option<Duration>,
    initial: Duration,
    maximum: Duration,
) -> Duration {
    attempted_delay.map_or(initial, |delay| next_backoff(delay, maximum))
}

fn jittered_backoff(base: Duration, ratio: f64) -> Duration {
    if base.is_zero() || ratio <= 0.0 {
        return base;
    }
    let bounded_ratio = ratio.clamp(0.0, 1.0);
    let factor = 1.0 - (rand::random::<f64>() * bounded_ratio);
    base.mul_f64(factor)
}

// This private actor boundary keeps every owned lifecycle handle explicit;
// collapsing them into an unvalidated public/config DTO would obscure task
// ownership without reducing runtime state.
#[allow(clippy::too_many_arguments)]
async fn publisher_loop(
    plan: Arc<RedisBackplanePlan>,
    admission: Arc<PublisherAdmission>,
    mut requests: mpsc::Receiver<PublishRequest>,
    events: mpsc::Sender<WebSocketBackplaneEvent>,
    cancellation: CancellationToken,
    connector: Arc<dyn PublisherConnector>,
    initial_connection: Box<dyn PublisherTransport>,
    immediate_probe: Arc<Notify>,
) {
    let mut connection = Some(initial_connection);
    let mut reconnect_delay = plan.reconnect_initial_delay;
    let mut publish_degraded = false;
    let liveness_interval = transport_liveness_interval(&plan);
    let mut liveness = interval_at(Instant::now() + liveness_interval, liveness_interval);
    liveness.set_missed_tick_behavior(MissedTickBehavior::Delay);

    'publisher: loop {
        if connection.is_none() {
            if !send_event(
                &events,
                &cancellation,
                WebSocketBackplaneEvent::PublisherReconnecting,
            )
            .await
            {
                break;
            }
            let current_delay = jittered_backoff(reconnect_delay, plan.reconnect_jitter_ratio);
            let reconnect = async {
                sleep(current_delay).await;
                connector.connect(&plan).await
            };
            tokio::pin!(reconnect);
            let reconnect_result = loop {
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => break 'publisher,
                    // Poll the reconnect future before request rejection. A
                    // continuously full admission queue must never starve the
                    // transport recovery deadline.
                    result = &mut reconnect => break result,
                    request = requests.recv() => match request {
                        Some(request) => reject_publish(
                            request,
                            WebSocketBackplaneErrorKind::Unavailable,
                        ),
                        None => break 'publisher,
                    },
                }
            };
            match reconnect_result {
                Ok(reconnected) => {
                    connection = Some(reconnected);
                    reconnect_delay = plan.reconnect_initial_delay;
                    liveness.reset_after(liveness_interval);
                    // The transport becomes admissible only after a new
                    // connection exists. Requests tagged with the prior
                    // generation remain in FIFO order but are rejected below;
                    // concurrent producers cannot turn them into late replay.
                    admission.mark_ready();
                    let event = if publish_degraded {
                        WebSocketBackplaneEvent::PublisherUnavailable
                    } else {
                        WebSocketBackplaneEvent::PublisherReady
                    };
                    if !send_event(&events, &cancellation, event).await {
                        break;
                    }
                }
                Err(_) => {
                    reconnect_delay = next_backoff(reconnect_delay, plan.reconnect_max_delay);
                    if !send_event(
                        &events,
                        &cancellation,
                        WebSocketBackplaneEvent::PublisherUnavailable,
                    )
                    .await
                    {
                        break;
                    }
                }
            }
            continue;
        }

        enum PublisherWork {
            Probe { announce_success: bool },
            Request(PublishRequest),
        }

        let work = tokio::select! {
            biased;
            _ = cancellation.cancelled() => break,
            _ = immediate_probe.notified() => PublisherWork::Probe {
                announce_success: true,
            },
            // A due liveness probe has priority over the request stream, so
            // sustained publish traffic cannot hide a dead idle connection.
            _ = liveness.tick() => PublisherWork::Probe {
                announce_success: false,
            },
            request = requests.recv() => match request {
                Some(request) => PublisherWork::Request(request),
                None => break,
            },
        };
        let mut request = match work {
            PublisherWork::Probe { announce_success } => {
                let probe = connection
                    .as_mut()
                    .expect("publisher connection is present")
                    .ping(plan.operation_timeout);
                let available = tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => break,
                    result = probe => result.is_ok(),
                };
                if !available {
                    admission.mark_unavailable();
                    connection = None;
                    if !send_event(
                        &events,
                        &cancellation,
                        WebSocketBackplaneEvent::PublisherUnavailable,
                    )
                    .await
                    {
                        break;
                    }
                } else if announce_success {
                    let event = if publish_degraded {
                        WebSocketBackplaneEvent::PublisherUnavailable
                    } else {
                        WebSocketBackplaneEvent::PublisherReady
                    };
                    if !send_event(&events, &cancellation, event).await {
                        break;
                    }
                }
                continue;
            }
            PublisherWork::Request(request) => request,
        };
        if request.reply.is_closed() {
            continue;
        }
        if !admission.accepts(request.generation) {
            reject_publish(request, WebSocketBackplaneErrorKind::Unavailable);
            continue;
        }
        if Instant::now() >= request.deadline {
            reject_publish(request, WebSocketBackplaneErrorKind::TimedOut);
            continue;
        }
        let attempt = {
            let active = connection
                .as_mut()
                .expect("publisher connection is present");
            let publish = AssertUnwindSafe(active.publish(
                plan.channel.as_ref(),
                request.frame.as_bytes(),
                request.deadline,
            ))
            .catch_unwind();
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => None,
                _ = request.reply.closed() => Some(None),
                result = publish => Some(Some(result)),
            }
        };
        let result = match attempt {
            None => {
                reject_publish(request, WebSocketBackplaneErrorKind::Interrupted);
                break;
            }
            Some(None) => continue,
            Some(Some(Ok(Err(PublisherAttemptError::TimedOut)))) => {
                admission.mark_unavailable();
                connection = None;
                Err(WebSocketBackplaneError::new(
                    WebSocketBackplaneErrorKind::TimedOut,
                ))
            }
            Some(Some(Ok(Err(PublisherAttemptError::Unavailable)))) => {
                admission.mark_unavailable();
                connection = None;
                Err(WebSocketBackplaneError::new(
                    WebSocketBackplaneErrorKind::Unavailable,
                ))
            }
            Some(Some(Ok(Err(PublisherAttemptError::Publish)))) => Err(
                WebSocketBackplaneError::new(WebSocketBackplaneErrorKind::Publish),
            ),
            Some(Some(Ok(Ok(())))) => Ok(WebSocketBackplanePublishReceipt::accepted()),
            Some(Some(Err(_))) => {
                admission.mark_unavailable();
                connection = None;
                Err(WebSocketBackplaneError::new(
                    WebSocketBackplaneErrorKind::Panicked,
                ))
            }
        };
        let unavailable = result.as_ref().is_err_and(|error| {
            matches!(
                error.kind(),
                WebSocketBackplaneErrorKind::Unavailable
                    | WebSocketBackplaneErrorKind::TimedOut
                    | WebSocketBackplaneErrorKind::Panicked
            )
        });
        let publish_failed = result
            .as_ref()
            .is_err_and(|error| error.kind() == WebSocketBackplaneErrorKind::Publish);
        let publish_recovered = result.is_ok() && publish_degraded;
        if publish_failed {
            publish_degraded = true;
        } else if publish_recovered {
            publish_degraded = false;
        }
        let _ = request.reply.send(result);
        let health_event = if unavailable || publish_failed {
            Some(WebSocketBackplaneEvent::PublisherUnavailable)
        } else if publish_recovered {
            Some(WebSocketBackplaneEvent::PublisherReady)
        } else {
            None
        };
        if let Some(event) = health_event
            && !send_event(&events, &cancellation, event).await
        {
            break;
        }
    }

    requests.close();
    while let Some(request) = requests.recv().await {
        reject_publish(request, WebSocketBackplaneErrorKind::Interrupted);
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_publisher_actor(
    plan: Arc<RedisBackplanePlan>,
    admission: Arc<PublisherAdmission>,
    requests: mpsc::Receiver<PublishRequest>,
    events: mpsc::Sender<WebSocketBackplaneEvent>,
    cancellation: CancellationToken,
    connector: Arc<dyn PublisherConnector>,
    initial_connection: Box<dyn PublisherTransport>,
    immediate_probe: Arc<Notify>,
) -> JoinHandle<()> {
    let failure_events = events.clone();
    let failure_cancellation = cancellation.clone();
    let failure_admission = Arc::clone(&admission);
    tokio::spawn(async move {
        let outcome = AssertUnwindSafe(publisher_loop(
            plan,
            admission,
            requests,
            events,
            cancellation,
            connector,
            initial_connection,
            immediate_probe,
        ))
        .catch_unwind()
        .await;
        if outcome.is_err() && !failure_cancellation.is_cancelled() {
            failure_admission.mark_unavailable();
            let _ = send_event(
                &failure_events,
                &failure_cancellation,
                WebSocketBackplaneEvent::PublisherUnavailable,
            )
            .await;
        }
    })
}

struct RedisSubscriberTransport {
    connection: MultiplexedConnection,
    pushes: mpsc::Receiver<PushInfo>,
    overflowed: Arc<AtomicBool>,
}

#[async_trait]
trait SubscriberTransport: Send {
    fn take_overflowed(&self) -> bool;

    async fn next_push(&mut self) -> Option<PushInfo>;

    async fn ping(&mut self, deadline: Duration) -> Result<(), PlanError>;

    async fn unsubscribe(&mut self, channel: &str, deadline: Duration);
}

#[async_trait]
impl SubscriberTransport for RedisSubscriberTransport {
    fn take_overflowed(&self) -> bool {
        self.overflowed.swap(false, Ordering::AcqRel)
    }

    async fn next_push(&mut self) -> Option<PushInfo> {
        self.pushes.recv().await
    }

    async fn ping(&mut self, deadline: Duration) -> Result<(), PlanError> {
        let pong = timeout(
            deadline,
            redis::cmd("PING").query_async::<String>(&mut self.connection),
        )
        .await
        .map_err(|_| PlanError::Transport)?
        .map_err(|_| PlanError::Transport)?;
        if pong != "PONG" {
            return Err(PlanError::Transport);
        }
        Ok(())
    }

    async fn unsubscribe(&mut self, channel: &str, deadline: Duration) {
        let _ = timeout(deadline, self.connection.unsubscribe(channel)).await;
    }
}

async fn connect_subscriber(
    plan: &RedisBackplanePlan,
) -> Result<Box<dyn SubscriberTransport>, PlanError> {
    let (pushes_tx, pushes) = mpsc::channel(plan.ingress_capacity);
    let overflowed = Arc::new(AtomicBool::new(false));
    let sender_overflowed = Arc::clone(&overflowed);
    let connection_config = plan.connection_config().set_push_sender(move |push| {
        pushes_tx.try_send(push).map_err(|_| {
            sender_overflowed.store(true, Ordering::Release);
        })
    });
    let mut connection = timeout(
        plan.connection_timeout,
        plan.client
            .get_multiplexed_async_connection_with_config(&connection_config),
    )
    .await
    .map_err(|_| PlanError::Transport)?
    .map_err(|_| PlanError::Transport)?;
    timeout(
        plan.operation_timeout,
        connection.subscribe(plan.channel.as_ref()),
    )
    .await
    .map_err(|_| PlanError::Transport)?
    .map_err(|_| PlanError::Transport)?;

    Ok(Box::new(RedisSubscriberTransport {
        connection,
        pushes,
        overflowed,
    }))
}

#[async_trait]
trait SubscriberConnector: Send + Sync {
    async fn connect(
        &self,
        plan: &RedisBackplanePlan,
    ) -> Result<Box<dyn SubscriberTransport>, PlanError>;
}

struct RedisSubscriberConnector;

#[async_trait]
impl SubscriberConnector for RedisSubscriberConnector {
    async fn connect(
        &self,
        plan: &RedisBackplanePlan,
    ) -> Result<Box<dyn SubscriberTransport>, PlanError> {
        connect_subscriber(plan).await
    }
}

enum SubscriberInput {
    Frame(Vec<u8>),
    Ignore,
    Reconnect,
}

fn parse_push(
    push: PushInfo,
    expected_channel: &str,
    maximum_frame_bytes: usize,
) -> SubscriberInput {
    if push.kind == PushKind::Disconnection {
        return SubscriberInput::Reconnect;
    }
    if push.kind != PushKind::Message || push.data.len() != 2 {
        return SubscriberInput::Ignore;
    }
    let mut data = push.data.into_iter();
    let channel = match data.next() {
        Some(Value::BulkString(bytes)) => bytes,
        _ => return SubscriberInput::Reconnect,
    };
    if channel.as_slice() != expected_channel.as_bytes() {
        return SubscriberInput::Reconnect;
    }
    let payload = match data.next() {
        Some(Value::BulkString(bytes)) => bytes,
        _ => return SubscriberInput::Reconnect,
    };
    if payload.is_empty() || payload.len() > maximum_frame_bytes {
        return SubscriberInput::Reconnect;
    }
    SubscriberInput::Frame(payload)
}

async fn subscriber_loop(
    plan: Arc<RedisBackplanePlan>,
    maximum_frame_bytes: usize,
    events: mpsc::Sender<WebSocketBackplaneEvent>,
    frames: mpsc::Sender<Vec<u8>>,
    cancellation: CancellationToken,
    connector: Arc<dyn SubscriberConnector>,
    publisher_probe: Arc<Notify>,
) {
    let mut active: Option<Box<dyn SubscriberTransport>> = None;
    // `None` is the one immediate startup attempt. Every retry, including the
    // first attempt after a runtime disconnect, waits the configured initial
    // delay with jitter before touching the shared Redis endpoint again.
    let mut retry_delay = None;
    let liveness_interval = transport_liveness_interval(&plan);
    let mut liveness = interval_at(Instant::now() + liveness_interval, liveness_interval);
    liveness.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        if active.is_none() {
            if !send_event(
                &events,
                &cancellation,
                WebSocketBackplaneEvent::SubscriptionReconnecting,
            )
            .await
            {
                break;
            }
            let attempt_delay = retry_delay;
            if let Some(delay) = attempt_delay {
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => break,
                    () = sleep(jittered_backoff(delay, plan.reconnect_jitter_ratio)) => {}
                }
            }
            let connection = tokio::select! {
                biased;
                _ = cancellation.cancelled() => break,
                result = connector.connect(&plan) => result,
            };
            match connection {
                Ok(connection) => {
                    active = Some(connection);
                    retry_delay = None;
                    liveness.reset_after(liveness_interval);
                    if !send_event(
                        &events,
                        &cancellation,
                        WebSocketBackplaneEvent::SubscriptionReady,
                    )
                    .await
                    {
                        break;
                    }
                }
                Err(_) => {
                    retry_delay = Some(subscriber_retry_delay_after_failure(
                        attempt_delay,
                        plan.reconnect_initial_delay,
                        plan.reconnect_max_delay,
                    ));
                    if !send_event(
                        &events,
                        &cancellation,
                        WebSocketBackplaneEvent::SubscriptionUnavailable,
                    )
                    .await
                    {
                        break;
                    }
                }
            }
            continue;
        }

        let subscriber = active.as_mut().expect("subscriber connection is present");
        if subscriber.take_overflowed() {
            active = None;
            retry_delay = Some(plan.reconnect_initial_delay);
            if !send_event(
                &events,
                &cancellation,
                WebSocketBackplaneEvent::SubscriptionUnavailable,
            )
            .await
            {
                break;
            }
            if !request_publisher_revalidation(&events, &cancellation, &publisher_probe).await {
                break;
            }
            continue;
        }

        enum SubscriberWork {
            Probe,
            Push(Option<PushInfo>),
        }

        let work = tokio::select! {
            biased;
            _ = cancellation.cancelled() => break,
            // A due probe wins over a hot Pub/Sub stream, so message traffic
            // cannot indefinitely hide a half-open subscriber transport.
            _ = liveness.tick() => SubscriberWork::Probe,
            push = subscriber.next_push() => SubscriberWork::Push(push),
        };
        let push = match work {
            SubscriberWork::Probe => {
                let probe = subscriber.ping(plan.operation_timeout);
                let available = tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => break,
                    result = probe => result.is_ok(),
                };
                if !available {
                    active = None;
                    retry_delay = Some(plan.reconnect_initial_delay);
                    if !send_event(
                        &events,
                        &cancellation,
                        WebSocketBackplaneEvent::SubscriptionUnavailable,
                    )
                    .await
                    {
                        break;
                    }
                    if !request_publisher_revalidation(&events, &cancellation, &publisher_probe)
                        .await
                    {
                        break;
                    }
                }
                continue;
            }
            SubscriberWork::Push(Some(push)) => push,
            SubscriberWork::Push(None) => {
                active = None;
                retry_delay = Some(plan.reconnect_initial_delay);
                if !send_event(
                    &events,
                    &cancellation,
                    WebSocketBackplaneEvent::SubscriptionUnavailable,
                )
                .await
                {
                    break;
                }
                if !request_publisher_revalidation(&events, &cancellation, &publisher_probe).await {
                    break;
                }
                continue;
            }
        };
        match parse_push(push, &plan.channel, maximum_frame_bytes) {
            SubscriberInput::Frame(frame) => {
                if cancellation.is_cancelled() {
                    break;
                }
                match frames.try_send(frame) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Closed(_)) => break,
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        // Never let a full application-frame stage pin this
                        // actor and hide transport health. Drop the saturated
                        // subscription, report degradation on the independent
                        // control channel, and reconnect after bounded backoff.
                        active = None;
                        retry_delay = Some(plan.reconnect_initial_delay);
                        if !send_event(
                            &events,
                            &cancellation,
                            WebSocketBackplaneEvent::SubscriptionUnavailable,
                        )
                        .await
                        {
                            break;
                        }
                        if !request_publisher_revalidation(&events, &cancellation, &publisher_probe)
                            .await
                        {
                            break;
                        }
                    }
                }
            }
            SubscriberInput::Ignore => {}
            SubscriberInput::Reconnect => {
                active = None;
                retry_delay = Some(plan.reconnect_initial_delay);
                if !send_event(
                    &events,
                    &cancellation,
                    WebSocketBackplaneEvent::SubscriptionUnavailable,
                )
                .await
                {
                    break;
                }
                if !request_publisher_revalidation(&events, &cancellation, &publisher_probe).await {
                    break;
                }
            }
        }
    }

    if let Some(mut subscriber) = active {
        subscriber
            .unsubscribe(plan.channel.as_ref(), plan.operation_timeout)
            .await;
    }
}

struct TerminalFailure {
    cancellation: CancellationToken,
    kind: StdMutex<Option<WebSocketBackplaneErrorKind>>,
}

impl TerminalFailure {
    fn new() -> Self {
        Self {
            cancellation: CancellationToken::new(),
            kind: StdMutex::new(None),
        }
    }

    fn fail(&self, kind: WebSocketBackplaneErrorKind) {
        let mut failure = self
            .kind
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if failure.is_none() {
            *failure = Some(kind);
        }
        drop(failure);
        self.cancellation.cancel();
    }

    async fn cancelled(&self) {
        self.cancellation.cancelled().await;
    }

    fn error(&self, fallback: WebSocketBackplaneErrorKind) -> WebSocketBackplaneError {
        let kind = self
            .kind
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .unwrap_or(fallback);
        WebSocketBackplaneError::new(kind)
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_subscriber_actor(
    plan: Arc<RedisBackplanePlan>,
    maximum_frame_bytes: usize,
    events: mpsc::Sender<WebSocketBackplaneEvent>,
    frames: mpsc::Sender<Vec<u8>>,
    cancellation: CancellationToken,
    connector: Arc<dyn SubscriberConnector>,
    publisher_probe: Arc<Notify>,
    terminal_failure: Arc<TerminalFailure>,
) -> JoinHandle<()> {
    let failure_cancellation = cancellation.clone();
    tokio::spawn(async move {
        let outcome = AssertUnwindSafe(subscriber_loop(
            plan,
            maximum_frame_bytes,
            events,
            frames,
            cancellation,
            connector,
            publisher_probe,
        ))
        .catch_unwind()
        .await;
        if outcome.is_err() && !failure_cancellation.is_cancelled() {
            terminal_failure.fail(WebSocketBackplaneErrorKind::Panicked);
        }
    })
}

async fn request_publisher_revalidation(
    events: &mpsc::Sender<WebSocketBackplaneEvent>,
    cancellation: &CancellationToken,
    publisher_probe: &Notify,
) -> bool {
    // Redis uses independent publisher and subscriber connections. A
    // subscriber outage therefore does not prove that publishing is down, but
    // it does invalidate the last publisher observation. Mark it degraded
    // before this task can announce a recovered subscription, then ask the
    // publisher actor for an immediate bounded PING. This closes the window in
    // which an idle stale publisher and a freshly reconnected subscriber could
    // make aggregate health appear ready.
    if !send_event(
        events,
        cancellation,
        WebSocketBackplaneEvent::PublisherReconnecting,
    )
    .await
    {
        return false;
    }
    publisher_probe.notify_one();
    true
}

async fn stop_task(task: Option<JoinHandle<()>>, deadline: Duration) -> bool {
    let Some(mut task) = task else {
        return true;
    };
    tokio::select! {
        result = &mut task => result.is_ok(),
        () = sleep(deadline) => {
            task.abort();
            let _ = task.await;
            false
        }
    }
}

struct RuntimeTasks {
    publisher: Option<JoinHandle<()>>,
    subscriber: Option<JoinHandle<()>>,
}

struct InboundReceivers {
    control: mpsc::Receiver<WebSocketBackplaneEvent>,
    frames: mpsc::Receiver<Vec<u8>>,
}

#[derive(Clone, Copy)]
enum RedisCloseState {
    Open,
    Closing,
    Closed,
    Failed(WebSocketBackplaneError),
}

struct CloseCompletion {
    state: StdMutex<RedisCloseState>,
    notify: Notify,
}

impl CloseCompletion {
    fn new() -> Self {
        Self {
            state: StdMutex::new(RedisCloseState::Open),
            notify: Notify::new(),
        }
    }

    fn state(&self) -> RedisCloseState {
        *self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn complete(&self, result: Result<(), WebSocketBackplaneError>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *state = match result {
            Ok(()) => RedisCloseState::Closed,
            Err(error) => RedisCloseState::Failed(error),
        };
        drop(state);
        self.notify.notify_waiters();
    }
}

/// Redis Pub/Sub implementation of Lily's transport-neutral WebSocket backplane.
///
/// Construct this type only through
/// `WsAppBuilder::backplane::<RedisWebSocketBackplane>(...)`. The adapter owns
/// dedicated Redis publisher/subscriber connections and closes them before
/// Lily disposes the application DI container.
pub struct RedisWebSocketBackplane {
    plan: Arc<RedisBackplanePlan>,
    publisher_admission: Arc<PublisherAdmission>,
    publish: mpsc::Sender<PublishRequest>,
    control_tx: mpsc::Sender<WebSocketBackplaneEvent>,
    frame_tx: mpsc::Sender<Vec<u8>>,
    inbound: Mutex<InboundReceivers>,
    tasks: StdMutex<RuntimeTasks>,
    subscriber_connector: Arc<dyn SubscriberConnector>,
    publisher_probe: Arc<Notify>,
    subscriber_failure: Arc<TerminalFailure>,
    cancellation: CancellationToken,
    closed: AtomicBool,
    close_completion: Arc<CloseCompletion>,
}

impl RedisWebSocketBackplane {
    async fn publish_frame(
        &self,
        frame: PublishFrame,
    ) -> Result<WebSocketBackplanePublishReceipt, WebSocketBackplaneError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(WebSocketBackplaneError::new(
                WebSocketBackplaneErrorKind::Unavailable,
            ));
        }
        let generation = self
            .publisher_admission
            .admitted_generation()
            .ok_or_else(|| {
                WebSocketBackplaneError::new(WebSocketBackplaneErrorKind::Unavailable)
            })?;
        let (reply, result) = oneshot::channel();
        let deadline = Instant::now() + self.plan.operation_timeout;
        self.publish
            .try_send(PublishRequest {
                frame,
                generation,
                deadline,
                reply,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => {
                    WebSocketBackplaneError::new(WebSocketBackplaneErrorKind::Saturated)
                }
                mpsc::error::TrySendError::Closed(_) => {
                    WebSocketBackplaneError::new(WebSocketBackplaneErrorKind::Unavailable)
                }
            })?;
        timeout_at(deadline, result)
            .await
            .map_err(|_| WebSocketBackplaneError::new(WebSocketBackplaneErrorKind::TimedOut))?
            .unwrap_or_else(|_| {
                Err(WebSocketBackplaneError::new(
                    WebSocketBackplaneErrorKind::Unavailable,
                ))
            })
    }

    async fn ensure_subscriber_started(
        &self,
        maximum_frame_bytes: usize,
    ) -> Result<(), WebSocketBackplaneError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(WebSocketBackplaneError::new(
                WebSocketBackplaneErrorKind::Unavailable,
            ));
        }
        let mut tasks = self
            .tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Close may have won after the optimistic check but before task
        // ownership was acquired. Rechecking under the same lock used by
        // close prevents a cancelled subscriber from being started late.
        if self.closed.load(Ordering::Acquire) {
            return Err(WebSocketBackplaneError::new(
                WebSocketBackplaneErrorKind::Unavailable,
            ));
        }
        if tasks.subscriber.is_none() {
            let plan = Arc::clone(&self.plan);
            let events = self.control_tx.clone();
            let frames = self.frame_tx.clone();
            let cancellation = self.cancellation.clone();
            let connector = Arc::clone(&self.subscriber_connector);
            let publisher_probe = Arc::clone(&self.publisher_probe);
            tasks.subscriber = Some(spawn_subscriber_actor(
                plan,
                maximum_frame_bytes,
                events,
                frames,
                cancellation,
                connector,
                publisher_probe,
                self.subscriber_failure.clone(),
            ));
        }
        Ok(())
    }

    fn begin_close(&self) {
        let mut state = self
            .close_completion
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !matches!(*state, RedisCloseState::Open) {
            return;
        }
        *state = RedisCloseState::Closing;
        self.closed.store(true, Ordering::Release);
        self.cancellation.cancel();
        let (publisher, subscriber) = {
            let mut tasks = self
                .tasks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (tasks.publisher.take(), tasks.subscriber.take())
        };
        let deadline = self.plan.operation_timeout;
        let completion = Arc::clone(&self.close_completion);
        // Cleanup is app-owned once close starts. Dropping or timing out one
        // caller cannot drop the task handles or turn an interrupted close
        // into false success for a later caller.
        tokio::spawn(async move {
            let (publisher_stopped, subscriber_stopped) = tokio::join!(
                stop_task(publisher, deadline),
                stop_task(subscriber, deadline),
            );
            let result = if publisher_stopped && subscriber_stopped {
                Ok(())
            } else {
                Err(WebSocketBackplaneError::new(
                    WebSocketBackplaneErrorKind::Shutdown,
                ))
            };
            completion.complete(result);
        });
    }

    async fn receive_event(&self) -> Result<WebSocketBackplaneEvent, WebSocketBackplaneError> {
        let mut inbound = tokio::select! {
            biased;
            _ = self.cancellation.cancelled() => {
                return Err(WebSocketBackplaneError::new(
                    WebSocketBackplaneErrorKind::Unavailable,
                ));
            }
            _ = self.subscriber_failure.cancelled() => {
                return Err(self.subscriber_failure.error(
                    WebSocketBackplaneErrorKind::Receive,
                ));
            }
            inbound = self.inbound.lock() => inbound,
        };
        // Split the mutex guard into disjoint field borrows before entering
        // `tokio::select!`. Borrowing both fields through `inbound` inside the
        // macro makes the generated futures appear to mutably borrow the whole
        // guard, even though the two receivers are independent.
        let InboundReceivers { control, frames } = &mut *inbound;
        tokio::select! {
            biased;
            _ = self.cancellation.cancelled() => Err(WebSocketBackplaneError::new(
                WebSocketBackplaneErrorKind::Unavailable,
            )),
            _ = self.subscriber_failure.cancelled() => Err(self.subscriber_failure.error(
                WebSocketBackplaneErrorKind::Receive,
            )),
            event = control.recv() => event.ok_or_else(|| {
                WebSocketBackplaneError::new(WebSocketBackplaneErrorKind::Receive)
            }),
            frame = frames.recv() => frame
                .map(WebSocketBackplaneEvent::Frame)
                .ok_or_else(|| {
                    WebSocketBackplaneError::new(WebSocketBackplaneErrorKind::Receive)
                }),
        }
    }
}

impl Drop for RedisWebSocketBackplane {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        self.cancellation.cancel();
        let tasks = self
            .tasks
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(task) = tasks.publisher.take() {
            task.abort();
        }
        if let Some(task) = tasks.subscriber.take() {
            task.abort();
        }
    }
}

#[async_trait]
impl WebSocketBackplane for RedisWebSocketBackplane {
    async fn new(extensions: Arc<Extensions>) -> Result<Self, WebSocketBackplaneInitError> {
        let config_service = extensions
            .get_service::<ConfigService>(None)
            .await
            .map_err(WebSocketBackplaneInitError::dependency)?;
        let config = config_service
            .get_lily_config()
            .await
            .websocket
            .and_then(|websocket| websocket.backplane)
            .ok_or_else(|| {
                WebSocketBackplaneInitError::new(WebSocketBackplaneInitErrorKind::Configuration)
            })?;
        let plan =
            Arc::new(RedisBackplanePlan::from_config(&config).await.map_err(
                |error| match error {
                    PlanError::Configuration => WebSocketBackplaneInitError::new(
                        WebSocketBackplaneInitErrorKind::Configuration,
                    ),
                    PlanError::Transport => {
                        WebSocketBackplaneInitError::new(WebSocketBackplaneInitErrorKind::Transport)
                    }
                },
            )?);
        let publisher_connector: Arc<dyn PublisherConnector> = Arc::new(RedisPublisherConnector);
        let publisher = publisher_connector.connect(&plan).await.map_err(|_| {
            WebSocketBackplaneInitError::new(WebSocketBackplaneInitErrorKind::Transport)
        })?;
        let (publish, publish_requests) = mpsc::channel(plan.publish_capacity);
        let (control_tx, control) = mpsc::channel(CONTROL_EVENT_SLOTS);
        let (frame_tx, frames) = mpsc::channel(plan.ingress_capacity);
        let cancellation = CancellationToken::new();
        let publisher_probe = Arc::new(Notify::new());
        let publisher_admission = Arc::new(PublisherAdmission::ready());
        let publisher_task = spawn_publisher_actor(
            Arc::clone(&plan),
            Arc::clone(&publisher_admission),
            publish_requests,
            control_tx.clone(),
            cancellation.clone(),
            publisher_connector,
            publisher,
            Arc::clone(&publisher_probe),
        );

        Ok(Self {
            plan,
            publisher_admission,
            publish,
            control_tx,
            frame_tx,
            inbound: Mutex::new(InboundReceivers { control, frames }),
            tasks: StdMutex::new(RuntimeTasks {
                publisher: Some(publisher_task),
                subscriber: None,
            }),
            subscriber_connector: Arc::new(RedisSubscriberConnector),
            publisher_probe,
            subscriber_failure: Arc::new(TerminalFailure::new()),
            cancellation,
            closed: AtomicBool::new(false),
            close_completion: Arc::new(CloseCompletion::new()),
        })
    }

    async fn publish(
        &self,
        frame: WebSocketBackplaneFrame,
    ) -> Result<WebSocketBackplanePublishReceipt, WebSocketBackplaneError> {
        self.publish_frame(PublishFrame::Opaque(frame)).await
    }

    async fn receive(
        &self,
        admission: WebSocketBackplaneInboundAdmission,
    ) -> Result<WebSocketBackplaneEvent, WebSocketBackplaneError> {
        self.ensure_subscriber_started(admission.maximum_frame_bytes())
            .await?;
        self.receive_event().await
    }

    async fn close(&self) -> Result<(), WebSocketBackplaneError> {
        self.begin_close();
        loop {
            let notified = self.close_completion.notify.notified();
            tokio::pin!(notified);
            // `notify_waiters` deliberately does not retain a permit. Register
            // this waiter before reading the terminal state so completion
            // cannot land between the state check and the first poll.
            notified.as_mut().enable();
            match self.close_completion.state() {
                RedisCloseState::Open => unreachable!("close owner did not start cleanup"),
                RedisCloseState::Closing => notified.as_mut().await,
                RedisCloseState::Closed => return Ok(()),
                RedisCloseState::Failed(error) => return Err(error),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::future::pending;
    use std::sync::atomic::AtomicUsize;

    struct ScriptedPublisherTransport {
        ping_results: StdMutex<VecDeque<Result<(), PublisherAttemptError>>>,
        publish_results: StdMutex<VecDeque<Result<(), PublisherAttemptError>>>,
    }

    impl ScriptedPublisherTransport {
        fn healthy() -> Self {
            Self {
                ping_results: StdMutex::new(VecDeque::new()),
                publish_results: StdMutex::new(VecDeque::new()),
            }
        }

        fn with_ping(result: Result<(), PublisherAttemptError>) -> Self {
            Self {
                ping_results: StdMutex::new(VecDeque::from([result])),
                publish_results: StdMutex::new(VecDeque::new()),
            }
        }

        fn with_publish(result: Result<(), PublisherAttemptError>) -> Self {
            Self {
                ping_results: StdMutex::new(VecDeque::new()),
                publish_results: StdMutex::new(VecDeque::from([result])),
            }
        }
    }

    #[async_trait]
    impl PublisherTransport for ScriptedPublisherTransport {
        async fn ping(&mut self, _deadline: Duration) -> Result<(), PublisherAttemptError> {
            self.ping_results
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pop_front()
                .unwrap_or(Ok(()))
        }

        async fn publish(
            &mut self,
            _channel: &str,
            _frame: &[u8],
            _deadline: Instant,
        ) -> Result<(), PublisherAttemptError> {
            self.publish_results
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pop_front()
                .unwrap_or(Ok(()))
        }
    }

    struct RecordingPublisherConnector {
        attempts: AtomicUsize,
    }

    struct CancellationAwarePublisherTransport {
        calls: Arc<AtomicUsize>,
        first_started: Arc<Notify>,
        first_dropped: Arc<AtomicBool>,
    }

    struct PublishDropSignal(Arc<AtomicBool>);

    impl Drop for PublishDropSignal {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[async_trait]
    impl PublisherTransport for CancellationAwarePublisherTransport {
        async fn ping(&mut self, _deadline: Duration) -> Result<(), PublisherAttemptError> {
            Ok(())
        }

        async fn publish(
            &mut self,
            _channel: &str,
            _frame: &[u8],
            _deadline: Instant,
        ) -> Result<(), PublisherAttemptError> {
            if self.calls.fetch_add(1, Ordering::AcqRel) == 0 {
                let _drop_signal = PublishDropSignal(Arc::clone(&self.first_dropped));
                self.first_started.notify_one();
                pending::<()>().await;
            }
            Ok(())
        }
    }

    #[async_trait]
    impl PublisherConnector for RecordingPublisherConnector {
        async fn connect(
            &self,
            _plan: &RedisBackplanePlan,
        ) -> Result<Box<dyn PublisherTransport>, PlanError> {
            self.attempts.fetch_add(1, Ordering::AcqRel);
            Ok(Box::new(ScriptedPublisherTransport::healthy()))
        }
    }

    struct PendingSubscriberConnector {
        entered: Notify,
    }

    struct PanickingSubscriberConnector;

    struct PanickingPublisherTransport;

    struct CountingPublisherTransport {
        publish_calls: Arc<AtomicUsize>,
    }

    struct ScriptedSubscriberTransport {
        ping_results: StdMutex<VecDeque<Result<(), PlanError>>>,
    }

    struct LivenessSubscriberConnector {
        attempts: AtomicUsize,
    }

    #[async_trait]
    impl SubscriberTransport for ScriptedSubscriberTransport {
        fn take_overflowed(&self) -> bool {
            false
        }

        async fn next_push(&mut self) -> Option<PushInfo> {
            pending::<Option<PushInfo>>().await
        }

        async fn ping(&mut self, _deadline: Duration) -> Result<(), PlanError> {
            self.ping_results
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pop_front()
                .unwrap_or(Ok(()))
        }

        async fn unsubscribe(&mut self, _channel: &str, _deadline: Duration) {}
    }

    #[async_trait]
    impl SubscriberConnector for LivenessSubscriberConnector {
        async fn connect(
            &self,
            _plan: &RedisBackplanePlan,
        ) -> Result<Box<dyn SubscriberTransport>, PlanError> {
            let attempt = self.attempts.fetch_add(1, Ordering::AcqRel);
            let ping_results = if attempt == 0 {
                VecDeque::from([Err(PlanError::Transport)])
            } else {
                VecDeque::new()
            };
            Ok(Box::new(ScriptedSubscriberTransport {
                ping_results: StdMutex::new(ping_results),
            }))
        }
    }

    #[async_trait]
    impl PublisherTransport for PanickingPublisherTransport {
        async fn ping(&mut self, _deadline: Duration) -> Result<(), PublisherAttemptError> {
            Ok(())
        }

        async fn publish(
            &mut self,
            _channel: &str,
            _frame: &[u8],
            _deadline: Instant,
        ) -> Result<(), PublisherAttemptError> {
            panic!("publisher transport fixture panic")
        }
    }

    #[async_trait]
    impl PublisherTransport for CountingPublisherTransport {
        async fn ping(&mut self, _deadline: Duration) -> Result<(), PublisherAttemptError> {
            Ok(())
        }

        async fn publish(
            &mut self,
            _channel: &str,
            _frame: &[u8],
            _deadline: Instant,
        ) -> Result<(), PublisherAttemptError> {
            self.publish_calls.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
    }

    #[async_trait]
    impl SubscriberConnector for PendingSubscriberConnector {
        async fn connect(
            &self,
            _plan: &RedisBackplanePlan,
        ) -> Result<Box<dyn SubscriberTransport>, PlanError> {
            self.entered.notify_one();
            pending().await
        }
    }

    #[async_trait]
    impl SubscriberConnector for PanickingSubscriberConnector {
        async fn connect(
            &self,
            _plan: &RedisBackplanePlan,
        ) -> Result<Box<dyn SubscriberTransport>, PlanError> {
            panic!("subscriber connector fixture panic")
        }
    }

    fn config(url: &str) -> RedisWebSocketBackplaneConfig {
        RedisWebSocketBackplaneConfig {
            redis_url: url.to_owned(),
            use_tls: false,
            custom_ca_bundle: None,
            application_namespace: "orders-api".to_owned(),
            environment_namespace: "test".to_owned(),
            channel_namespace: "events".to_owned(),
            publish_capacity: 8,
            ingress_capacity: 16,
            connection_timeout_millis: 500,
            operation_timeout_millis: 500,
            reconnect_initial_delay_millis: 10,
            reconnect_max_delay_millis: 100,
            reconnect_jitter_ratio: 0.0,
        }
    }

    async fn test_plan() -> Arc<RedisBackplanePlan> {
        let mut plan = RedisBackplanePlan::from_config(&config("redis://127.0.0.1:6379/0"))
            .await
            .unwrap();
        plan.operation_timeout = Duration::from_millis(25);
        plan.reconnect_initial_delay = Duration::from_millis(10);
        plan.reconnect_max_delay = Duration::from_millis(40);
        plan.reconnect_jitter_ratio = 0.0;
        Arc::new(plan)
    }

    fn fixture_request() -> (
        PublishRequest,
        oneshot::Receiver<Result<WebSocketBackplanePublishReceipt, WebSocketBackplaneError>>,
    ) {
        let (reply, result) = oneshot::channel();
        (
            PublishRequest {
                frame: PublishFrame::Fixture(Arc::from([1_u8, 2, 3])),
                generation: 0,
                deadline: Instant::now() + Duration::from_secs(5),
                reply,
            },
            result,
        )
    }

    fn fixture_inbound(
        frame_capacity: usize,
    ) -> (
        mpsc::Sender<WebSocketBackplaneEvent>,
        mpsc::Sender<Vec<u8>>,
        InboundReceivers,
    ) {
        let (control_tx, control) = mpsc::channel(CONTROL_EVENT_SLOTS);
        let (frame_tx, frames) = mpsc::channel(frame_capacity);
        (control_tx, frame_tx, InboundReceivers { control, frames })
    }

    fn test_backplane(
        plan: Arc<RedisBackplanePlan>,
        publisher: Option<JoinHandle<()>>,
    ) -> Arc<RedisWebSocketBackplane> {
        let (publish, _requests) = mpsc::channel(plan.publish_capacity);
        let (control_tx, frame_tx, inbound) = fixture_inbound(plan.ingress_capacity);
        Arc::new(RedisWebSocketBackplane {
            plan,
            publisher_admission: Arc::new(PublisherAdmission::ready()),
            publish,
            control_tx,
            frame_tx,
            inbound: Mutex::new(inbound),
            tasks: StdMutex::new(RuntimeTasks {
                publisher,
                subscriber: None,
            }),
            subscriber_connector: Arc::new(PendingSubscriberConnector {
                entered: Notify::new(),
            }),
            publisher_probe: Arc::new(Notify::new()),
            subscriber_failure: Arc::new(TerminalFailure::new()),
            cancellation: CancellationToken::new(),
            closed: AtomicBool::new(false),
            close_completion: Arc::new(CloseCompletion::new()),
        })
    }

    #[tokio::test]
    async fn plan_forces_resp3_builds_isolated_channel_and_redacts_endpoint() {
        let plan = RedisBackplanePlan::from_config(&config("redis://user:secret@127.0.0.1:6379/4"))
            .await
            .unwrap();
        assert_eq!(
            plan.channel.as_ref(),
            "lily.websocket.v4.test.orders-api.events"
        );
        let debug = format!("{plan:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("secret"));
        assert!(!debug.contains("127.0.0.1"));
        assert_eq!(
            plan.client.get_connection_info().redis.protocol,
            ProtocolVersion::RESP3
        );
    }

    #[tokio::test]
    async fn tls_mismatch_and_unix_socket_are_rejected_without_network_io() {
        let mut plaintext = config("redis://127.0.0.1:6379/0");
        plaintext.use_tls = true;
        assert_eq!(
            RedisBackplanePlan::from_config(&plaintext)
                .await
                .unwrap_err(),
            PlanError::Configuration
        );

        let unix = config("redis+unix:///tmp/redis.sock");
        assert_eq!(
            RedisBackplanePlan::from_config(&unix).await.unwrap_err(),
            PlanError::Configuration
        );
    }

    #[tokio::test]
    async fn bounded_ca_reader_accepts_exact_maximum_and_rejects_one_byte_more() {
        let directory = tempfile::tempdir().unwrap();
        let exact = directory.path().join("exact-maximum.pem");
        std::fs::write(
            &exact,
            vec![b'x'; usize::try_from(MAX_CA_BUNDLE_BYTES).unwrap()],
        )
        .unwrap();

        let bytes = read_bounded_ca_file(&exact).await.unwrap();
        assert_eq!(u64::try_from(bytes.len()).unwrap(), MAX_CA_BUNDLE_BYTES);

        let oversized = directory.path().join("maximum-plus-one.pem");
        std::fs::write(
            &oversized,
            vec![b'x'; usize::try_from(MAX_CA_BUNDLE_BYTES + 1).unwrap()],
        )
        .unwrap();

        assert_eq!(
            read_bounded_ca_file(&oversized).await.unwrap_err(),
            PlanError::Configuration
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bounded_ca_reader_rejects_symbolic_links() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("ca.pem");
        let link = directory.path().join("ca-link.pem");
        std::fs::write(&target, b"not parsed by the bounded reader").unwrap();
        symlink(&target, &link).unwrap();

        assert_eq!(
            read_bounded_ca_file(&link).await.unwrap_err(),
            PlanError::Configuration
        );
    }

    #[test]
    fn bounded_backoff_saturates_at_the_configured_maximum() {
        let initial = Duration::from_millis(10);
        let maximum = Duration::from_millis(25);
        assert_eq!(next_backoff(initial, maximum), Duration::from_millis(20));
        assert_eq!(next_backoff(Duration::from_millis(20), maximum), maximum);
        assert_eq!(next_backoff(maximum, maximum), maximum);
    }

    #[test]
    fn reconnect_jitter_is_bounded_and_can_be_disabled() {
        let base = Duration::from_secs(10);
        assert_eq!(jittered_backoff(base, 0.0), base);

        for _ in 0..128 {
            let delay = jittered_backoff(base, 0.2);
            assert!(delay >= Duration::from_secs(8));
            assert!(delay <= base);
        }
    }

    #[test]
    fn subscriber_retry_sequence_starts_at_initial_delay_then_backs_off() {
        let initial = Duration::from_millis(10);
        let maximum = Duration::from_millis(40);
        let first = subscriber_retry_delay_after_failure(None, initial, maximum);
        let second = subscriber_retry_delay_after_failure(Some(first), initial, maximum);
        let third = subscriber_retry_delay_after_failure(Some(second), initial, maximum);

        assert_eq!(first, Duration::from_millis(10));
        assert_eq!(second, Duration::from_millis(20));
        assert_eq!(third, maximum);
    }

    #[tokio::test]
    async fn idle_subscriber_liveness_failure_is_observed_and_recovers() {
        let plan = test_plan().await;
        let connector = Arc::new(LivenessSubscriberConnector {
            attempts: AtomicUsize::new(0),
        });
        let (events_tx, mut events) = mpsc::channel(8);
        let (frames_tx, _frames) = mpsc::channel(8);
        let cancellation = CancellationToken::new();
        let task = tokio::spawn(subscriber_loop(
            plan,
            1024,
            events_tx,
            frames_tx,
            cancellation.clone(),
            Arc::clone(&connector) as Arc<dyn SubscriberConnector>,
            Arc::new(Notify::new()),
        ));

        for (expectation, matches_event) in [
            (
                "initial reconnecting",
                WebSocketBackplaneEvent::SubscriptionReconnecting,
            ),
            ("initial ready", WebSocketBackplaneEvent::SubscriptionReady),
            (
                "liveness unavailable",
                WebSocketBackplaneEvent::SubscriptionUnavailable,
            ),
            (
                "publisher revalidation",
                WebSocketBackplaneEvent::PublisherReconnecting,
            ),
            (
                "bounded subscriber reconnect",
                WebSocketBackplaneEvent::SubscriptionReconnecting,
            ),
            (
                "subscriber recovered",
                WebSocketBackplaneEvent::SubscriptionReady,
            ),
        ] {
            let event = timeout(Duration::from_secs(1), events.recv())
                .await
                .unwrap_or_else(|_| panic!("{expectation} event deadline"))
                .unwrap_or_else(|| panic!("{expectation} event channel closed"));
            assert_eq!(
                std::mem::discriminant(&event),
                std::mem::discriminant(&matches_event),
                "unexpected event while waiting for {expectation}: {event:?}"
            );
        }
        assert_eq!(connector.attempts.load(Ordering::Acquire), 2);

        cancellation.cancel();
        timeout(Duration::from_secs(1), task)
            .await
            .expect("subscriber liveness task cancellation deadline")
            .unwrap();
    }

    #[tokio::test]
    async fn idle_liveness_failure_drives_bounded_reconnect_without_application_publish() {
        let plan = test_plan().await;
        let (requests, request_rx) = mpsc::channel(plan.publish_capacity);
        let (events_tx, mut events) = mpsc::channel(16);
        let cancellation = CancellationToken::new();
        let connector = Arc::new(RecordingPublisherConnector {
            attempts: AtomicUsize::new(0),
        });
        let publisher_probe = Arc::new(Notify::new());
        let task = tokio::spawn(publisher_loop(
            Arc::clone(&plan),
            Arc::new(PublisherAdmission::ready()),
            request_rx,
            events_tx,
            cancellation.clone(),
            Arc::clone(&connector) as Arc<dyn PublisherConnector>,
            Box::new(ScriptedPublisherTransport::with_ping(Err(
                PublisherAttemptError::Unavailable,
            ))),
            publisher_probe,
        ));
        tokio::task::yield_now().await;

        timeout(Duration::from_secs(1), async {
            assert!(matches!(
                events.recv().await,
                Some(WebSocketBackplaneEvent::PublisherUnavailable)
            ));
            assert!(matches!(
                events.recv().await,
                Some(WebSocketBackplaneEvent::PublisherReconnecting)
            ));
            assert!(matches!(
                events.recv().await,
                Some(WebSocketBackplaneEvent::PublisherReady)
            ));
        })
        .await
        .expect("idle publisher liveness recovery exceeded its bounded deadline");
        assert_eq!(connector.attempts.load(Ordering::Acquire), 1);

        cancellation.cancel();
        drop(requests);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn subscriber_requested_probe_revalidates_publisher_before_periodic_tick() {
        let mut plan = test_plan().await;
        Arc::get_mut(&mut plan).unwrap().operation_timeout = Duration::from_secs(2);
        let (requests, request_rx) = mpsc::channel(plan.publish_capacity);
        let (events_tx, mut events) = mpsc::channel(8);
        let cancellation = CancellationToken::new();
        let connector = Arc::new(RecordingPublisherConnector {
            attempts: AtomicUsize::new(0),
        });
        let publisher_probe = Arc::new(Notify::new());
        let task = tokio::spawn(publisher_loop(
            Arc::clone(&plan),
            Arc::new(PublisherAdmission::ready()),
            request_rx,
            events_tx,
            cancellation.clone(),
            Arc::clone(&connector) as Arc<dyn PublisherConnector>,
            Box::new(ScriptedPublisherTransport::healthy()),
            Arc::clone(&publisher_probe),
        ));

        publisher_probe.notify_one();
        assert!(matches!(
            timeout(Duration::from_millis(500), events.recv())
                .await
                .expect("publisher ignored the subscriber revalidation signal"),
            Some(WebSocketBackplaneEvent::PublisherReady)
        ));
        assert_eq!(connector.attempts.load(Ordering::Acquire), 0);

        cancellation.cancel();
        drop(requests);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn successful_publish_recovers_publish_specific_health_failure() {
        let plan = test_plan().await;
        let (requests, request_rx) = mpsc::channel(plan.publish_capacity);
        let (events_tx, mut events) = mpsc::channel(8);
        let cancellation = CancellationToken::new();
        let connector = Arc::new(RecordingPublisherConnector {
            attempts: AtomicUsize::new(0),
        });
        let task = tokio::spawn(publisher_loop(
            Arc::clone(&plan),
            Arc::new(PublisherAdmission::ready()),
            request_rx,
            events_tx,
            cancellation.clone(),
            connector,
            Box::new(ScriptedPublisherTransport::with_publish(Err(
                PublisherAttemptError::Publish,
            ))),
            Arc::new(Notify::new()),
        ));

        let (failed, failed_result) = fixture_request();
        requests.send(failed).await.unwrap();
        assert_eq!(
            timeout(Duration::from_secs(1), failed_result)
                .await
                .expect("publish failure reply deadline")
                .unwrap()
                .unwrap_err()
                .kind(),
            WebSocketBackplaneErrorKind::Publish
        );
        assert!(matches!(
            timeout(Duration::from_secs(1), events.recv())
                .await
                .expect("publish failure health deadline"),
            Some(WebSocketBackplaneEvent::PublisherUnavailable)
        ));

        let (recovered, recovered_result) = fixture_request();
        requests.send(recovered).await.unwrap();
        assert!(
            timeout(Duration::from_secs(1), recovered_result)
                .await
                .expect("publish recovery reply deadline")
                .unwrap()
                .is_ok()
        );
        assert!(matches!(
            timeout(Duration::from_secs(1), events.recv())
                .await
                .expect("publish recovery health deadline"),
            Some(WebSocketBackplaneEvent::PublisherReady)
        ));

        cancellation.cancel();
        drop(requests);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn expired_queued_publish_is_timed_out_without_transport_io() {
        let plan = test_plan().await;
        let admission = Arc::new(PublisherAdmission::ready());
        let calls = Arc::new(AtomicUsize::new(0));
        let (requests, request_rx) = mpsc::channel(plan.publish_capacity);
        let (events_tx, _events) = mpsc::channel(8);
        let cancellation = CancellationToken::new();
        let task = tokio::spawn(publisher_loop(
            Arc::clone(&plan),
            admission,
            request_rx,
            events_tx,
            cancellation.clone(),
            Arc::new(RecordingPublisherConnector {
                attempts: AtomicUsize::new(0),
            }),
            Box::new(CountingPublisherTransport {
                publish_calls: Arc::clone(&calls),
            }),
            Arc::new(Notify::new()),
        ));
        let (mut request, result) = fixture_request();
        request.deadline = Instant::now() - Duration::from_millis(1);
        requests.send(request).await.unwrap();

        assert_eq!(
            timeout(Duration::from_secs(1), result)
                .await
                .expect("expired publish reply deadline")
                .unwrap()
                .unwrap_err()
                .kind(),
            WebSocketBackplaneErrorKind::TimedOut
        );
        assert_eq!(calls.load(Ordering::Acquire), 0);

        cancellation.cancel();
        drop(requests);
        timeout(Duration::from_secs(1), task)
            .await
            .expect("expired publish actor shutdown deadline")
            .unwrap();
    }

    #[tokio::test]
    async fn prior_generation_publish_cannot_replay_on_recovered_connection() {
        let plan = test_plan().await;
        let admission = Arc::new(PublisherAdmission::ready());
        admission.mark_unavailable();
        admission.mark_ready();
        let calls = Arc::new(AtomicUsize::new(0));
        let (requests, request_rx) = mpsc::channel(plan.publish_capacity);
        let (events_tx, _events) = mpsc::channel(8);
        let cancellation = CancellationToken::new();
        let task = tokio::spawn(publisher_loop(
            Arc::clone(&plan),
            Arc::clone(&admission),
            request_rx,
            events_tx,
            cancellation.clone(),
            Arc::new(RecordingPublisherConnector {
                attempts: AtomicUsize::new(0),
            }),
            Box::new(CountingPublisherTransport {
                publish_calls: Arc::clone(&calls),
            }),
            Arc::new(Notify::new()),
        ));
        let (request, result) = fixture_request();
        assert_eq!(request.generation, 0);
        assert_eq!(admission.admitted_generation(), Some(1));
        requests.send(request).await.unwrap();

        assert_eq!(
            timeout(Duration::from_secs(1), result)
                .await
                .expect("prior-generation publish reply deadline")
                .unwrap()
                .unwrap_err()
                .kind(),
            WebSocketBackplaneErrorKind::Unavailable
        );
        assert_eq!(calls.load(Ordering::Acquire), 0);

        cancellation.cancel();
        drop(requests);
        timeout(Duration::from_secs(1), task)
            .await
            .expect("prior-generation actor shutdown deadline")
            .unwrap();
    }

    #[tokio::test]
    async fn full_adapter_admission_returns_typed_saturation_without_waiting() {
        let plan = test_plan().await;
        let (publish, _requests) = mpsc::channel(1);
        let (control_tx, frame_tx, inbound) = fixture_inbound(plan.ingress_capacity);
        let backplane = RedisWebSocketBackplane {
            plan,
            publisher_admission: Arc::new(PublisherAdmission::ready()),
            publish,
            control_tx,
            frame_tx,
            inbound: Mutex::new(inbound),
            tasks: StdMutex::new(RuntimeTasks {
                publisher: None,
                subscriber: None,
            }),
            subscriber_connector: Arc::new(PendingSubscriberConnector {
                entered: Notify::new(),
            }),
            publisher_probe: Arc::new(Notify::new()),
            subscriber_failure: Arc::new(TerminalFailure::new()),
            cancellation: CancellationToken::new(),
            closed: AtomicBool::new(false),
            close_completion: Arc::new(CloseCompletion::new()),
        };
        let (filler, _filler_result) = fixture_request();
        backplane.publish.try_send(filler).unwrap();

        let error = backplane
            .publish_frame(PublishFrame::Fixture(Arc::from([9_u8])))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), WebSocketBackplaneErrorKind::Saturated);
    }

    #[tokio::test]
    async fn control_health_preempts_a_saturated_application_frame_stage() {
        let plan = test_plan().await;
        let frame_capacity = plan.ingress_capacity;
        let backplane = test_backplane(plan, None);
        for sequence in 0..frame_capacity {
            backplane
                .frame_tx
                .try_send(vec![u8::try_from(sequence % 256).unwrap()])
                .expect("fill bounded application-frame stage");
        }
        backplane
            .control_tx
            .try_send(WebSocketBackplaneEvent::SubscriptionUnavailable)
            .expect("control channel remains independently available");

        assert!(matches!(
            timeout(Duration::from_secs(1), backplane.receive_event())
                .await
                .expect("control-priority receive deadline")
                .unwrap(),
            WebSocketBackplaneEvent::SubscriptionUnavailable
        ));
        assert!(matches!(
            timeout(Duration::from_secs(1), backplane.receive_event())
                .await
                .expect("queued frame receive deadline")
                .unwrap(),
            WebSocketBackplaneEvent::Frame(_)
        ));
        backplane.close().await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_publish_waiter_does_not_block_or_replay_later_work() {
        let plan = test_plan().await;
        let (requests, request_rx) = mpsc::channel(plan.publish_capacity);
        let (events_tx, mut events) = mpsc::channel(8);
        let cancellation = CancellationToken::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let first_started = Arc::new(Notify::new());
        let first_dropped = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn(publisher_loop(
            Arc::clone(&plan),
            Arc::new(PublisherAdmission::ready()),
            request_rx,
            events_tx,
            cancellation.clone(),
            Arc::new(RecordingPublisherConnector {
                attempts: AtomicUsize::new(0),
            }),
            Box::new(CancellationAwarePublisherTransport {
                calls: Arc::clone(&calls),
                first_started: Arc::clone(&first_started),
                first_dropped: Arc::clone(&first_dropped),
            }),
            Arc::new(Notify::new()),
        ));

        let (cancelled, cancelled_result) = fixture_request();
        requests.send(cancelled).await.unwrap();
        timeout(Duration::from_secs(1), first_started.notified())
            .await
            .expect("first publish did not start");
        drop(cancelled_result);
        timeout(Duration::from_secs(1), async {
            while !first_dropped.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled publish future remained active");

        let (next, next_result) = fixture_request();
        requests.send(next).await.unwrap();
        assert!(
            timeout(Duration::from_secs(1), next_result)
                .await
                .expect("post-cancellation publish reply deadline")
                .unwrap()
                .is_ok()
        );
        assert_eq!(calls.load(Ordering::Acquire), 2);
        assert!(events.try_recv().is_err());

        cancellation.cancel();
        drop(requests);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn publisher_transport_panic_is_typed_and_degrades_health() {
        let plan = test_plan().await;
        let (requests, request_rx) = mpsc::channel(plan.publish_capacity);
        let (events_tx, mut events) = mpsc::channel(8);
        let cancellation = CancellationToken::new();
        let task = spawn_publisher_actor(
            Arc::clone(&plan),
            Arc::new(PublisherAdmission::ready()),
            request_rx,
            events_tx,
            cancellation.clone(),
            Arc::new(RecordingPublisherConnector {
                attempts: AtomicUsize::new(0),
            }),
            Box::new(PanickingPublisherTransport),
            Arc::new(Notify::new()),
        );
        let (request, result) = fixture_request();
        requests.send(request).await.unwrap();
        assert_eq!(
            timeout(Duration::from_secs(1), result)
                .await
                .expect("publisher panic reply deadline")
                .unwrap()
                .unwrap_err()
                .kind(),
            WebSocketBackplaneErrorKind::Panicked
        );
        assert!(matches!(
            timeout(Duration::from_secs(1), events.recv())
                .await
                .expect("publisher panic health deadline"),
            Some(WebSocketBackplaneEvent::PublisherUnavailable)
        ));
        cancellation.cancel();
        drop(requests);
        timeout(Duration::from_secs(1), task)
            .await
            .expect("publisher panic supervisor deadline")
            .unwrap();
    }

    #[tokio::test]
    async fn subscriber_actor_panic_is_a_typed_terminal_failure() {
        let plan = test_plan().await;
        let (publish, _requests) = mpsc::channel(plan.publish_capacity);
        let (control_tx, frame_tx, inbound) = fixture_inbound(plan.ingress_capacity);
        let backplane = RedisWebSocketBackplane {
            plan,
            publisher_admission: Arc::new(PublisherAdmission::ready()),
            publish,
            control_tx,
            frame_tx,
            inbound: Mutex::new(inbound),
            tasks: StdMutex::new(RuntimeTasks {
                publisher: None,
                subscriber: None,
            }),
            subscriber_connector: Arc::new(PanickingSubscriberConnector),
            publisher_probe: Arc::new(Notify::new()),
            subscriber_failure: Arc::new(TerminalFailure::new()),
            cancellation: CancellationToken::new(),
            closed: AtomicBool::new(false),
            close_completion: Arc::new(CloseCompletion::new()),
        };
        backplane.ensure_subscriber_started(1024).await.unwrap();

        let error = timeout(Duration::from_secs(1), async {
            loop {
                match backplane.receive_event().await {
                    Ok(WebSocketBackplaneEvent::SubscriptionReconnecting) => {}
                    Ok(other) => panic!("unexpected subscriber event: {other:?}"),
                    Err(error) => return error,
                }
            }
        })
        .await
        .expect("subscriber panic remained hidden behind a pending receive");
        assert_eq!(error.kind(), WebSocketBackplaneErrorKind::Panicked);
        backplane.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sustained_outage_admission_cannot_starve_publisher_reconnect() {
        let plan = test_plan().await;
        let (requests, request_rx) = mpsc::channel(plan.publish_capacity);
        let (events_tx, mut events) = mpsc::channel(32);
        let cancellation = CancellationToken::new();
        let connector = Arc::new(RecordingPublisherConnector {
            attempts: AtomicUsize::new(0),
        });
        let publisher_probe = Arc::new(Notify::new());
        let task = tokio::spawn(publisher_loop(
            Arc::clone(&plan),
            Arc::new(PublisherAdmission::ready()),
            request_rx,
            events_tx,
            cancellation.clone(),
            Arc::clone(&connector) as Arc<dyn PublisherConnector>,
            Box::new(ScriptedPublisherTransport::with_publish(Err(
                PublisherAttemptError::Unavailable,
            ))),
            publisher_probe,
        ));

        let (first, first_result) = fixture_request();
        requests.send(first).await.unwrap();
        assert_eq!(
            timeout(Duration::from_secs(1), first_result)
                .await
                .expect("outage publish reply deadline")
                .unwrap()
                .unwrap_err()
                .kind(),
            WebSocketBackplaneErrorKind::Unavailable
        );
        assert!(matches!(
            timeout(Duration::from_secs(1), events.recv())
                .await
                .expect("publisher unavailable event deadline"),
            Some(WebSocketBackplaneEvent::PublisherUnavailable)
        ));
        assert!(matches!(
            timeout(Duration::from_secs(1), events.recv())
                .await
                .expect("publisher reconnecting event deadline"),
            Some(WebSocketBackplaneEvent::PublisherReconnecting)
        ));

        let keep_flooding = Arc::new(AtomicBool::new(true));
        let flood_flag = Arc::clone(&keep_flooding);
        let flood_requests = requests.clone();
        let flood = tokio::spawn(async move {
            while flood_flag.load(Ordering::Acquire) {
                let (request, result) = fixture_request();
                drop(result);
                if flood_requests.try_send(request).is_err() {
                    tokio::task::yield_now().await;
                }
            }
        });

        timeout(Duration::from_secs(1), async {
            loop {
                if matches!(
                    events.recv().await,
                    Some(WebSocketBackplaneEvent::PublisherReady)
                ) {
                    return;
                }
            }
        })
        .await
        .expect("publisher reconnect was starved by sustained admission");
        assert!(connector.attempts.load(Ordering::Acquire) >= 1);

        keep_flooding.store(false, Ordering::Release);
        flood.await.unwrap();
        cancellation.cancel();
        drop(requests);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn dropped_close_waiter_does_not_lose_terminal_cleanup_result() {
        let plan = test_plan().await;
        let publisher = tokio::spawn(async { pending::<()>().await });
        let backplane = test_backplane(plan, Some(publisher));
        let first_backplane = Arc::clone(&backplane);
        let first = tokio::spawn(async move { first_backplane.close().await });
        timeout(Duration::from_secs(1), async {
            while !matches!(backplane.close_completion.state(), RedisCloseState::Closing) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());

        let error = timeout(Duration::from_secs(1), backplane.close())
            .await
            .expect("detached close supervisor did not complete")
            .unwrap_err();
        assert_eq!(error.kind(), WebSocketBackplaneErrorKind::Shutdown);
        assert_eq!(backplane.close().await.unwrap_err().kind(), error.kind());
    }

    #[tokio::test]
    async fn close_releases_an_event_waiter_and_replays_success() {
        let plan = test_plan().await;
        let publisher = tokio::spawn(async {});
        let backplane = test_backplane(plan, Some(publisher));
        let waiting_backplane = Arc::clone(&backplane);
        let waiting = tokio::spawn(async move { waiting_backplane.receive_event().await });
        tokio::task::yield_now().await;

        backplane.close().await.unwrap();
        let error = timeout(Duration::from_secs(1), waiting)
            .await
            .expect("receive event waiter remained blocked after close")
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), WebSocketBackplaneErrorKind::Unavailable);
        backplane.close().await.unwrap();
    }

    #[tokio::test]
    async fn subscriber_connect_is_cancelled_without_waiting_for_transport_deadline() {
        let plan = test_plan().await;
        let connector = Arc::new(PendingSubscriberConnector {
            entered: Notify::new(),
        });
        let (events_tx, mut events) = mpsc::channel(8);
        let (frames_tx, _frames) = mpsc::channel(8);
        let cancellation = CancellationToken::new();
        let publisher_probe = Arc::new(Notify::new());
        let task = tokio::spawn(subscriber_loop(
            plan,
            1024,
            events_tx,
            frames_tx,
            cancellation.clone(),
            Arc::clone(&connector) as Arc<dyn SubscriberConnector>,
            publisher_probe,
        ));
        assert!(matches!(
            timeout(Duration::from_secs(1), events.recv())
                .await
                .expect("subscriber reconnecting event deadline"),
            Some(WebSocketBackplaneEvent::SubscriptionReconnecting)
        ));
        timeout(Duration::from_secs(1), connector.entered.notified())
            .await
            .expect("subscriber connector did not start");

        cancellation.cancel();
        timeout(Duration::from_secs(1), task)
            .await
            .expect("subscriber connect ignored cancellation")
            .unwrap();
    }

    #[test]
    fn push_parser_moves_only_exact_bounded_channel_payloads() {
        let channel = "lily.websocket.v4.test.orders-api.events";
        let valid = PushInfo {
            kind: PushKind::Message,
            data: vec![
                Value::BulkString(channel.as_bytes().to_vec()),
                Value::BulkString(vec![1, 2, 3]),
            ],
        };
        assert!(matches!(
            parse_push(valid, channel, 3),
            SubscriberInput::Frame(bytes) if bytes == vec![1, 2, 3]
        ));

        let oversized = PushInfo {
            kind: PushKind::Message,
            data: vec![
                Value::BulkString(channel.as_bytes().to_vec()),
                Value::BulkString(vec![0; 4]),
            ],
        };
        assert!(matches!(
            parse_push(oversized, channel, 3),
            SubscriberInput::Reconnect
        ));
    }
}
