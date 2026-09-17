#![no_main]

use libfuzzer_sys::fuzz_target;
use lily_websocket::{
    LilyEnvelopeCodec, Message, RawEnvelope, WebSocketFrameCodec, WebSocketPayloadCodec,
};

const MAX_FUZZ_INPUT_BYTES: usize = 1024 * 1024;

fuzz_target!(|input: &[u8]| {
    if input.len() > MAX_FUZZ_INPUT_BYTES {
        return;
    }
    let (control, payload) = input
        .split_first()
        .map_or((0, input), |(head, tail)| (*head, tail));
    let limit = match control & 0b11 {
        0 => 0,
        1 => 64,
        2 => 4 * 1024,
        _ => MAX_FUZZ_INPUT_BYTES,
    };
    let message = match (control >> 2) % 5 {
        0 => Message::Binary(payload.to_vec().into()),
        1 => Message::Text(String::from_utf8_lossy(payload).into_owned().into()),
        2 => Message::Ping(payload.iter().copied().take(125).collect::<Vec<_>>().into()),
        3 => Message::Pong(payload.iter().copied().take(125).collect::<Vec<_>>().into()),
        _ => Message::Close(None),
    };
    let codec = LilyEnvelopeCodec;
    if let Ok(decoded) =
        RawEnvelope::try_from_message(message, limit).and_then(|frame| codec.decode_frame(frame))
    {
        let _ = codec.decode_payload(decoded.into_payload());
    }
});
