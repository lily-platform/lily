#![no_main]

use async_trait::async_trait;
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use lily_core::structs::RawHeader;
use lily_http_api::__private::fuzzing::RequestBodyStream;
use lily_http_api::{Request, RequestBodyError, RequestExt};
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, OnceLock,
    },
    time::Duration,
};

const MAX_FUZZ_INPUT_BYTES: usize = 64 * 1024;

struct StaticBodyStream {
    chunks: VecDeque<Bytes>,
    length: usize,
    exact_hint: bool,
    active: Arc<AtomicUsize>,
}

impl Drop for StaticBodyStream {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
    }
}

#[async_trait]
impl RequestBodyStream for StaticBodyStream {
    async fn next_chunk(&mut self) -> Result<Option<Bytes>, RequestBodyError> {
        Ok(self.chunks.pop_front())
    }

    fn size_hint(&self) -> (u64, Option<u64>) {
        let length = self.length as u64;
        if self.exact_hint {
            (length, Some(length))
        } else {
            (0, None)
        }
    }
}

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("fuzz runtime must build")
    })
}

fuzz_target!(|input: &[u8]| {
    if input.len() > MAX_FUZZ_INPUT_BYTES || input.len() < 2 {
        return;
    }
    let control = input[0];
    let Some(separator) = input[1..].iter().position(|byte| *byte == b'\n') else {
        return;
    };
    let separator = separator + 1;
    let content_type_bytes = input[1..separator]
        .strip_suffix(b"\r")
        .unwrap_or(&input[1..separator]);
    let content_type = String::from_utf8_lossy(content_type_bytes).into_owned();
    let mut body = input[separator + 1..].to_vec();
    if control & 0x20 != 0 {
        if let Some(marker) = body
            .windows(b"BINARY:".len())
            .position(|window| window == b"BINARY:")
        {
            for byte in &mut body[marker + b"BINARY:".len()..] {
                *byte ^= u8::MAX;
            }
        }
    }
    let limit = match control & 0x03 {
        0 => 64,
        1 => 256,
        2 => 1024,
        _ => 4096,
    };
    let chunk_size = usize::from((control >> 4).max(1)).min(4096);
    let active = Arc::new(AtomicUsize::new(1));
    let chunks = body
        .chunks(chunk_size)
        .map(Bytes::copy_from_slice)
        .collect();
    let stream = StaticBodyStream {
        chunks,
        length: body.len(),
        exact_hint: control & 0x08 == 0,
        active: Arc::clone(&active),
    };
    let mut headers = vec![RawHeader {
        name: "Content-Type".to_string(),
        value: content_type.clone(),
        line_number: 1,
        raw_line: "Content-Type: <redacted>".to_string(),
    }];
    if control & 0x04 != 0 {
        headers.push(RawHeader {
            name: "content-type".to_string(),
            value: content_type,
            line_number: 2,
            raw_line: "content-type: <redacted>".to_string(),
        });
    }

    runtime().block_on(async {
        let active_for_task = Arc::clone(&active);
        let mut task = tokio::spawn(async move {
            let request = Request::from_streaming_transport_parts(
                "POST".to_string(),
                "/multipart".to_string(),
                headers,
                Some(Box::new(stream)),
                limit,
            );
            if let Ok(request) = request {
                let _ = request.multipart().await;
            }
            drop(active_for_task);
        });
        match tokio::time::timeout(Duration::from_millis(100), &mut task).await {
            Ok(Ok(())) => {}
            Ok(Err(join_error)) if join_error.is_panic() => {
                std::panic::resume_unwind(join_error.into_panic());
            }
            Ok(Err(_)) => panic!("multipart fuzz task was cancelled unexpectedly"),
            Err(_) => {
                task.abort();
                let _ = task.await;
                assert_eq!(active.load(Ordering::Acquire), 0);
                panic!("multipart iteration exceeded its deadline");
            }
        }
        assert_eq!(active.load(Ordering::Acquire), 0);
    });
});
