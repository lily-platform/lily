use std::sync::{atomic::Ordering, Arc};

use crate::{
    contract::*,
    provider::{ApplicationContainer, ApplicationScopeFactory, InjectionError, ProcessContext},
};
use nested::RequestService;

#[tokio::test]
async fn single_dependency_preserves_injection_identity_and_lifecycle() -> Result<(), InjectionError>
{
    let container = ApplicationContainer::build().await?;
    assert_eq!(CLOCK_STARTS.load(Ordering::SeqCst), 1);
    assert_eq!(SCOPED_STARTS.load(Ordering::SeqCst), 0);
    let clock = container.resolve::<Clock>(None).await?;
    let interface = container.resolve::<dyn ClockApi>(None).await?;
    assert_eq!(
        Arc::as_ptr(&clock) as *const (),
        Arc::as_ptr(&interface) as *const ()
    );
    assert!(matches!(
        container.resolve::<RequestService>(None).await,
        Err(InjectionError::ScopeRequired { .. })
    ));
    assert!(matches!(
        container.resolve::<Disabled>(None).await,
        Err(InjectionError::ServiceNotFound(_))
    ));

    let first = container
        .run_scoped(ProcessContext::with_process_id(101), async {
            let first = container.resolve::<RequestService>(None).await?;
            let again = container
                .services()
                .get_service::<RequestService>(None)
                .await?;
            assert!(Arc::ptr_eq(&first, &again));
            assert!(Arc::ptr_eq(&first.clock, &interface));

            let transient = container.resolve::<TransientService>(None).await?;
            let another = container.resolve::<TransientService>(None).await?;
            assert!(!Arc::ptr_eq(&transient, &another));
            assert_eq!(transient.value, 17);
            assert!(Arc::ptr_eq(&transient.clock, &clock));
            assert_eq!(CLOCK_STARTS.load(Ordering::SeqCst), 1);
            Ok::<_, InjectionError>(first)
        })
        .await??;
    assert_eq!(SCOPED_STOPS.load(Ordering::SeqCst), 1);
    assert_eq!(TRANSIENT_STOPS.load(Ordering::SeqCst), 2);

    // The public job-scope wrapper must work through the same single dependency.
    let factory = Arc::new(ApplicationScopeFactory::new(&container));
    let second = factory
        .create_scope(ProcessContext::with_process_id(102))?
        .run(|extensions| {
            Box::pin(async move { extensions.get_service::<RequestService>(None).await })
        })
        .await?;
    assert!(!Arc::ptr_eq(&first, &second));
    assert!(Arc::ptr_eq(&second.clock, &interface));
    assert_eq!(SCOPED_STARTS.load(Ordering::SeqCst), 2);
    assert_eq!(SCOPED_STOPS.load(Ordering::SeqCst), 2);
    assert_eq!(CLOCK_STOPS.load(Ordering::SeqCst), 0);

    let report = container.close().await?;
    assert_eq!(report.active_scopes_remaining, 0);
    assert_eq!(report.cleanup_tasks_remaining, 0);
    assert_eq!(report.active_resolutions_remaining, 0);
    assert_eq!(report.root_lifecycle_entries_remaining, 0);
    assert_eq!(report.forced_scopes, 0);
    assert_eq!(report.cancelled_cleanup_tasks, 0);
    assert_eq!(CLOCK_STOPS.load(Ordering::SeqCst), 1);
    container.close().await?;
    assert_eq!(CLOCK_STOPS.load(Ordering::SeqCst), 1);
    Ok(())
}
