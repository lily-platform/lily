use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use lily_http_api::AppBuilder;
use lily_http_api::{
    controller, CancellationToken, Controller, ControllerInitError, ControllerTrait, Extensions,
    FromRequestParts, HttpApiError, HttpHealthService, InjectionError, PlainText, Request, Service,
    ServiceTrait,
};
use lily_injection::Injectable;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

static ACTION_INVOCATIONS: AtomicUsize = AtomicUsize::new(0);
static SINGLETON_INITIALIZATIONS: AtomicUsize = AtomicUsize::new(0);
static SINGLETON_DISPOSALS: AtomicUsize = AtomicUsize::new(0);
static SCOPED_INITIALIZATIONS: AtomicUsize = AtomicUsize::new(0);
static SCOPED_DISPOSALS: AtomicUsize = AtomicUsize::new(0);
static TRANSIENT_INITIALIZATIONS: AtomicUsize = AtomicUsize::new(0);
static TRANSIENT_DISPOSALS: AtomicUsize = AtomicUsize::new(0);

trait SingletonContract: Send + Sync {
    fn sequence(&self) -> usize;
}

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton", interface = dyn SingletonContract)]
struct SingletonDependency {
    sequence: usize,
}

impl SingletonContract for SingletonDependency {
    fn sequence(&self) -> usize {
        self.sequence
    }
}

#[lily_http_api::async_trait::async_trait]
impl ServiceTrait for SingletonDependency {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        self.sequence = SINGLETON_INITIALIZATIONS.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        SINGLETON_DISPOSALS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

trait ScopedContract: Send + Sync {
    fn sequence(&self) -> usize;
}

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped", interface = dyn ScopedContract)]
struct ScopedDependency {
    sequence: usize,
}

impl ScopedContract for ScopedDependency {
    fn sequence(&self) -> usize {
        self.sequence
    }
}

#[lily_http_api::async_trait::async_trait]
impl ServiceTrait for ScopedDependency {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        self.sequence = SCOPED_INITIALIZATIONS.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        SCOPED_DISPOSALS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

trait TransientContract: Send + Sync {
    fn sequence(&self) -> usize;
}

#[derive(Default, Injectable)]
#[service(lifetime = "Transient", interface = dyn TransientContract)]
struct TransientDependency {
    sequence: usize,
}

impl TransientContract for TransientDependency {
    fn sequence(&self) -> usize {
        self.sequence
    }
}

#[lily_http_api::async_trait::async_trait]
impl ServiceTrait for TransientDependency {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        self.sequence = TRANSIENT_INITIALIZATIONS.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        TRANSIENT_DISPOSALS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Default, Injectable)]
#[service(lifetime = "Transient")]
struct FailingDependency;

#[lily_http_api::async_trait::async_trait]
impl ServiceTrait for FailingDependency {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        Err(InjectionError::InitError(
            "private-initialization-detail".to_string(),
        ))
    }
}

struct MissingDependency;

#[derive(Controller)]
#[base_path("/typed-services")]
struct TypedServiceController {
    singleton: Arc<SingletonDependency>,
}

#[lily_http_api::async_trait::async_trait]
impl ControllerTrait for TypedServiceController {
    async fn new(extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        let singleton = extensions
            .get_service::<SingletonDependency>(None)
            .await
            .map_err(ControllerInitError::dependency)?;
        Ok(Self { singleton })
    }
}

#[controller]
impl TypedServiceController {
    #[allow(clippy::too_many_arguments)]
    #[get("/lifecycle")]
    async fn lifecycle(
        &self,
        Service(singleton): Service<SingletonDependency>,
        Service(singleton_interface): Service<dyn SingletonContract>,
        Service(scoped_first): Service<ScopedDependency>,
        Service(scoped_interface): Service<dyn ScopedContract>,
        Service(scoped_second): Service<ScopedDependency>,
        Service(transient): Service<TransientDependency>,
        Service(transient_interface): Service<dyn TransientContract>,
    ) -> Result<PlainText, HttpApiError> {
        assert_eq!(self.singleton.sequence(), 1);
        assert_eq!(singleton.sequence(), 1);
        assert_eq!(singleton_interface.sequence(), 1);
        assert_eq!(
            Arc::as_ptr(&self.singleton) as *const (),
            Arc::as_ptr(&singleton) as *const ()
        );
        assert_eq!(
            Arc::as_ptr(&singleton) as *const (),
            Arc::as_ptr(&singleton_interface) as *const ()
        );

        let scoped_sequence = scoped_first.sequence();
        assert_ne!(scoped_sequence, 0);
        assert_eq!(scoped_interface.sequence(), scoped_sequence);
        assert_eq!(scoped_second.sequence(), scoped_sequence);
        assert_eq!(
            Arc::as_ptr(&scoped_first) as *const (),
            Arc::as_ptr(&scoped_interface) as *const ()
        );
        assert!(Arc::ptr_eq(&scoped_first, &scoped_second));

        assert_ne!(transient.sequence(), transient_interface.sequence());
        assert_ne!(
            Arc::as_ptr(&transient) as *const (),
            Arc::as_ptr(&transient_interface) as *const ()
        );
        ACTION_INVOCATIONS.fetch_add(1, Ordering::SeqCst);
        Ok(PlainText("service-lifecycles-ok".to_string()))
    }

    #[get("/missing")]
    async fn missing(&self, _service: Service<MissingDependency>) -> Result<(), HttpApiError> {
        panic!("missing dependency must reject before action invocation")
    }

    #[get("/failing")]
    async fn failing(&self, _service: Service<FailingDependency>) -> Result<(), HttpApiError> {
        panic!("initialization failure must reject before action invocation")
    }
}

async fn send_request(address: std::net::SocketAddr, path: &str) -> String {
    let mut connection = tokio::net::TcpStream::connect(address)
        .await
        .expect("typed service listener accepts a connection");
    let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    connection
        .write_all(request.as_bytes())
        .await
        .expect("HTTP request is written");

    let mut response = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(5),
        connection.read_to_end(&mut response),
    )
    .await
    .expect("HTTP response is bounded")
    .expect("HTTP response is readable");
    String::from_utf8(response).expect("HTTP response is UTF-8")
}

#[tokio::test]
async fn generated_service_extractor_preserves_di_lifetimes_and_safe_failures() {
    ACTION_INVOCATIONS.store(0, Ordering::SeqCst);
    SINGLETON_INITIALIZATIONS.store(0, Ordering::SeqCst);
    SINGLETON_DISPOSALS.store(0, Ordering::SeqCst);
    SCOPED_INITIALIZATIONS.store(0, Ordering::SeqCst);
    SCOPED_DISPOSALS.store(0, Ordering::SeqCst);
    TRANSIENT_INITIALIZATIONS.store(0, Ordering::SeqCst);
    TRANSIENT_DISPOSALS.store(0, Ordering::SeqCst);

    let reservation = TcpListener::bind("127.0.0.1:0").expect("reserve a loopback address");
    let address = reservation
        .local_addr()
        .expect("reserved listener has an address");
    drop(reservation);

    let app = AppBuilder::new(&address.to_string())
        .build()
        .await
        .expect("typed service application builds");
    assert_eq!(SINGLETON_INITIALIZATIONS.load(Ordering::SeqCst), 1);

    let extensions = app.extensions();
    let mut request = Request::from_transport_parts(
        "GET".to_string(),
        "/scope-invariant".to_string(),
        Vec::new(),
        &[],
    )
    .await
    .expect("scope invariant request builds");
    let scope_error =
        Service::<ScopedDependency>::from_request_parts(&mut request, extensions.as_ref())
            .await
            .expect_err("scoped resolution outside a request scope must fail");
    assert!(matches!(scope_error, InjectionError::ScopeRequired { .. }));
    let scope_error = HttpApiError::from(scope_error);
    assert_eq!(scope_error.http_status().0, 500);
    assert_eq!(
        scope_error.public_message(),
        "An internal server error occurred."
    );

    let health = extensions
        .get_service::<HttpHealthService>(None)
        .await
        .expect("HTTP health service resolves");
    let cancellation = CancellationToken::new();
    let cancel = cancellation.clone();
    let server = tokio::spawn(async move { app.start_with_cancellation(cancellation).await });

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if health
                .snapshot()
                .expect("health snapshot remains readable")
                .ready
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("typed service listener becomes ready");

    let success = send_request(address, "/typed-services/lifecycle").await;
    assert!(success.starts_with("HTTP/1.1 200"), "{success}");
    assert!(success.ends_with("service-lifecycles-ok"), "{success}");
    assert_eq!(ACTION_INVOCATIONS.load(Ordering::SeqCst), 1);
    assert_eq!(SCOPED_INITIALIZATIONS.load(Ordering::SeqCst), 1);
    assert_eq!(SCOPED_DISPOSALS.load(Ordering::SeqCst), 1);
    assert_eq!(TRANSIENT_INITIALIZATIONS.load(Ordering::SeqCst), 2);
    assert_eq!(TRANSIENT_DISPOSALS.load(Ordering::SeqCst), 2);

    let second_success = send_request(address, "/typed-services/lifecycle").await;
    assert!(
        second_success.starts_with("HTTP/1.1 200"),
        "{second_success}"
    );
    assert_eq!(ACTION_INVOCATIONS.load(Ordering::SeqCst), 2);
    assert_eq!(SCOPED_INITIALIZATIONS.load(Ordering::SeqCst), 2);
    assert_eq!(SCOPED_DISPOSALS.load(Ordering::SeqCst), 2);
    assert_eq!(TRANSIENT_INITIALIZATIONS.load(Ordering::SeqCst), 4);
    assert_eq!(TRANSIENT_DISPOSALS.load(Ordering::SeqCst), 4);

    let missing = send_request(address, "/typed-services/missing").await;
    assert!(missing.starts_with("HTTP/1.1 500"), "{missing}");
    assert!(missing.contains("\"code\":\"DEPENDENCY_INJECTION_ERROR\""));
    assert!(missing.contains("\"message\":\"An internal server error occurred.\""));
    assert!(!missing.contains("MissingDependency"));

    let failing = send_request(address, "/typed-services/failing").await;
    assert!(failing.starts_with("HTTP/1.1 500"), "{failing}");
    assert!(failing.contains("\"code\":\"INITIALIZATION_ERROR\""));
    assert!(failing.contains("\"message\":\"An internal server error occurred.\""));
    assert!(!failing.contains("private-initialization-detail"));

    assert_eq!(ACTION_INVOCATIONS.load(Ordering::SeqCst), 2);
    assert_eq!(SINGLETON_DISPOSALS.load(Ordering::SeqCst), 0);
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("typed service server shutdown is bounded")
        .expect("typed service server task does not panic")
        .expect("typed service server shuts down cleanly");
    assert_eq!(SINGLETON_DISPOSALS.load(Ordering::SeqCst), 1);
}
