//! Deterministic shutdown races at the real dispatch preflight boundary.

use super::*;
use tokio::sync::Semaphore;

struct Gate {
    entered: AtomicBool,
    release: Semaphore,
}

tokio::task_local! {
    static PREFLIGHT_GATE: Arc<Gate>;
}

pub(crate) async fn pause_if_armed() {
    if let Ok(gate) = PREFLIGHT_GATE.try_with(Arc::clone) {
        gate.entered.store(true, Ordering::Release);
        gate.release.acquire().await.unwrap().forget();
    }
}

async fn preflight_shutdown_contract(wire_format: WsWireFormat) {
    for force in [false, true] {
        let manager = Arc::new(ConnectionManager::with_registered_namespaces(
            8,
            128,
            ["orders".to_owned()],
        ));
        let id = Uuid::new_v4();
        let (tx, mut rx) = mpsc::channel(2);
        manager
            .add_connection(id, tx, None, Some("orders".into()))
            .await
            .unwrap();
        let (backplane, _events) = RecordingBackplane::channel();
        let dispatcher = active_dispatcher(manager.clone(), backplane.clone());
        let command = BroadcastMessage {
            target: BroadcastTarget::NamespaceConnections {
                namespace: "orders".into(),
                connection_ids: vec![id],
            },
            message: WsMessageBody::try_new("orders:created", json!({"id": 17})).unwrap(),
            wire_format,
            exclude: Vec::new(),
        };
        let expected_frame = match wire_format {
            WsWireFormat::Text => command.message.to_message().unwrap(),
            WsWireFormat::Binary => command.message.to_binary_message().unwrap(),
        };
        let gate = Arc::new(Gate {
            entered: AtomicBool::new(false),
            release: Semaphore::new(0),
        });
        let mut admitted =
            Box::pin(PREFLIGHT_GATE.scope(gate.clone(), dispatcher.dispatch(command.clone())));
        assert!(futures_util::poll!(admitted.as_mut()).is_pending());
        assert!(gate.entered.load(Ordering::Acquire));
        assert_eq!(dispatcher.0.in_flight.load(Ordering::Acquire), 1);
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert!(backplane.published.lock().await.is_empty());
        assert_eq!(manager.metrics_snapshot().outbound_messages, 0);

        dispatcher.begin_drain();
        assert!(matches!(
            dispatcher.dispatch(command).await,
            Err(ConnectionError::InvalidOperation(
                crate::connection::ConnectionOperationError::DispatcherNotAccepting
            ))
        ));
        assert_eq!(dispatcher.0.in_flight.load(Ordering::Acquire), 1);
        if force {
            dispatcher.force_drain();
        }
        gate.release.add_permits(1);
        let result = timeout(Duration::from_secs(2), admitted).await.unwrap();
        if force {
            assert!(matches!(result, Err(ConnectionError::DispatchInterrupted)));
            assert!(matches!(
                rx.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
            assert!(backplane.published.lock().await.is_empty());
            assert_eq!(manager.metrics_snapshot().outbound_messages, 0);
            assert_eq!(manager.metrics_snapshot().backplane_publish_accepted, 0);
        } else {
            let receipt = result.unwrap();
            assert_eq!(
                *receipt.local(),
                BroadcastReport {
                    targeted: 1,
                    sent: 1,
                    ..Default::default()
                }
            );
            assert!(matches!(
                receipt.backplane(),
                WebSocketBackplaneDispatchReceipt::Accepted(_)
            ));
            assert_eq!(rx.try_recv().unwrap(), expected_frame);
            assert!(matches!(
                rx.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
            assert_eq!(backplane.published.lock().await.len(), 1);
            assert_eq!(manager.metrics_snapshot().outbound_messages, 1);
            assert_eq!(manager.metrics_snapshot().backplane_publish_accepted, 1);
        }
        assert_eq!(dispatcher.0.in_flight.load(Ordering::Acquire), 0);
        timeout(Duration::from_secs(2), dispatcher.wait_drained())
            .await
            .unwrap();
        manager.remove_connection(id).await.unwrap();
        timeout(Duration::from_secs(2), dispatcher.close_backplane())
            .await
            .unwrap()
            .unwrap();
        assert!(backplane.closed.load(Ordering::Acquire));
    }
}

#[tokio::test]
async fn text_preflight_drain_preserves_accepted_work_and_force_prevents_side_effects() {
    preflight_shutdown_contract(WsWireFormat::Text).await;
}

#[tokio::test]
async fn binary_preflight_drain_preserves_accepted_work_and_force_prevents_side_effects() {
    preflight_shutdown_contract(WsWireFormat::Binary).await;
}
