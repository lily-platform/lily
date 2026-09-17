#![no_main]

use libfuzzer_sys::fuzz_target;
use lily_websocket::__fuzzing::{
    FuzzHandshakeHeader, FuzzHandshakeIdentityAction, FuzzHandshakeMiddlewareAction,
    FuzzHandshakePlan, SECRET_SENTINEL, exercise_handshake,
};
use std::{sync::OnceLock, time::Duration};

const MAX_INPUT_BYTES: usize = 64 * 1024;

fn byte(input: &[u8], index: usize) -> u8 {
    input.get(index).copied().unwrap_or_default()
}

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime must build")
    })
}

fuzz_target!(|input: &[u8]| {
    if input.len() > MAX_INPUT_BYTES {
        return;
    }

    let names: [&[u8]; 8] = [
        b"origin",
        b"authorization",
        b"cookie",
        b"sec-websocket-protocol",
        b"host",
        b"sec-websocket-key",
        b"sec-websocket-version",
        b"x-fuzz",
    ];
    let header_count = usize::from(byte(input, 0) % 20);
    let mut headers = Vec::with_capacity(header_count);
    for index in 0..header_count {
        let control = byte(input, 8 + index * 2);
        let value_control = byte(input, 9 + index * 2);
        let name = if control & 0x80 == 0 {
            names[usize::from(control) % names.len()].to_vec()
        } else {
            vec![b'x', b'-', control]
        };
        let value = match value_control % 7 {
            0 => b"https://app.example".to_vec(),
            1 => b"https://denied.example".to_vec(),
            2 => b"lily.v1".to_vec(),
            3 => b"13".to_vec(),
            4 => SECRET_SENTINEL.as_bytes().to_vec(),
            5 => input.iter().skip(48 + index).copied().take(128).collect(),
            _ => Vec::new(),
        };
        headers.push(FuzzHandshakeHeader { name, value });
    }

    let middlewares = input
        .iter()
        .skip(1)
        .take(8)
        .map(|value| match value % 4 {
            0 => FuzzHandshakeMiddlewareAction::Accept,
            1 => FuzzHandshakeMiddlewareAction::RejectBadRequest,
            2 => FuzzHandshakeMiddlewareAction::RejectUnauthorized,
            _ => FuzzHandshakeMiddlewareAction::RejectForbidden,
        })
        .collect();
    let identity = match byte(input, 2) % 6 {
        0 => FuzzHandshakeIdentityAction::Disabled,
        1 => FuzzHandshakeIdentityAction::Anonymous,
        2 => FuzzHandshakeIdentityAction::Authenticated,
        3 => FuzzHandshakeIdentityAction::CredentialRequired,
        4 => FuzzHandshakeIdentityAction::RejectUnauthorized,
        _ => FuzzHandshakeIdentityAction::Unavailable,
    };
    let plan = FuzzHandshakePlan {
        headers,
        query: (byte(input, 3) & 1 != 0).then(|| {
            format!(
                "namespace={}",
                String::from_utf8_lossy(input.get(32..96).unwrap_or_default())
            )
        }),
        allowed_origins: (byte(input, 4) & 1 != 0)
            .then(|| vec!["https://app.example".to_string()])
            .unwrap_or_default(),
        allow_any_origin: byte(input, 4) & 2 != 0,
        allow_missing_origin: byte(input, 4) & 4 != 0,
        supported_protocols: (byte(input, 5) & 1 != 0)
            .then(|| vec!["lily.v1".to_string()])
            .unwrap_or_default(),
        require_subprotocol: byte(input, 5) & 2 != 0,
        identity,
        middlewares,
    };
    runtime().block_on(async move {
        let mut task = tokio::spawn(exercise_handshake(plan));
        let snapshot = match tokio::time::timeout(Duration::from_millis(250), &mut task).await {
            Ok(result) => result.expect("handshake exercise task must not panic"),
            Err(_) => {
                task.abort();
                let _ = task.await;
                panic!("handshake exercise exceeded its absolute deadline");
            }
        };
        assert!((100..=599).contains(&snapshot.status));
        assert!(snapshot.secret_safe);
    });
});
