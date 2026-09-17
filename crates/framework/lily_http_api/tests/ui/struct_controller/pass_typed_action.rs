mod support;

use std::future::{ready, Future};

use lily_http_api::{FromRequest, FromRequestParts, OptionalFromRequestParts, Service};
use support::*;

type ExecutionView = lily_cancellation::ExecutionCancellation;

struct ConcreteActionService;

trait ActionService: Send + Sync {}

struct TestParts;

impl FromRequestParts for TestParts {
    type Rejection = HttpApiError;

    fn from_request_parts(
        _request: &mut Request,
        _extensions: &Extensions,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(Ok(Self))
    }
}

struct OptionalParts;

impl OptionalFromRequestParts for OptionalParts {
    type Rejection = HttpApiError;

    fn from_request_parts(
        _request: &mut Request,
        _extensions: &Extensions,
    ) -> impl Future<Output = Result<Option<Self>, Self::Rejection>> + Send {
        ready(Ok(None))
    }
}

struct RawBody;

impl FromRequest for RawBody {
    type Rejection = HttpApiError;

    fn from_request(
        _request: &mut Request,
        _extensions: &Extensions,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(Ok(Self))
    }
}

struct ApplicationBody;

impl FromRequest for ApplicationBody {
    type Rejection = HttpApiError;

    fn from_request(
        _request: &mut Request,
        _extensions: &Extensions,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(Ok(Self))
    }
}

#[derive(Controller)]
#[base_path("/api/typed")]
struct TypedActionController;

impl_controller_trait!(TypedActionController);

#[controller]
impl TypedActionController {
    #[get("/cancellation")]
    async fn cancellation(&self, view: ExecutionView) -> Result<(), HttpApiError> {
        let _ = view.is_cancelled();
        Ok(())
    }

    #[get("/zero")]
    async fn zero(&self) -> Result<(), HttpApiError> {
        Ok(())
    }

    #[post("/extract")]
    async fn extract(
        &self,
        _parts: TestParts,
        _optional: Option<OptionalParts>,
        _body: RawBody,
        _request: &mut Request,
    ) -> Result<(), HttpApiError> {
        Ok(())
    }

    #[get("/manual")]
    async fn manual(&self, response: &mut Response) -> Result<(), HttpApiError> {
        response.status(202, "Accepted");
        Ok(())
    }

    #[post("/application-body")]
    async fn application_body(&self, _body: ApplicationBody) -> Result<(), HttpApiError> {
        Ok(())
    }

    #[get("/services")]
    async fn services(
        &self,
        _concrete: Service<ConcreteActionService>,
        _interface: Service<dyn ActionService>,
    ) -> Result<(), HttpApiError> {
        Ok(())
    }
}

fn main() {}
