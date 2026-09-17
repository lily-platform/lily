# syntax=docker/dockerfile:1
FROM rust:1.96.1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends libpq-dev libssl-dev pkg-config && rm -rf /var/lib/apt/lists/*
WORKDIR /source
ENV CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_TARGET_DIR=/source/target
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY examples ./examples
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/source/target \
    cargo build --locked --manifest-path examples/Cargo.toml --workspace --bins -j 4 \
    && cargo build --locked --manifest-path examples/clients/Cargo.toml -j 4 \
    && mkdir /out \
    && cp /source/target/debug/lily-example-http /source/target/debug/lily-example-websocket /source/target/debug/lily-example-consumer /source/target/debug/lily-example-client /out/

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libpq5 libssl3 ca-certificates && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 10001 --create-home lily && mkdir -p /work/logs && chown -R lily:lily /work
COPY --from=build /out/ /usr/local/bin/
COPY examples/lily.toml /work/lily.toml
WORKDIR /work
USER lily
ENV LILY_CONFIG_PATH=/work/lily.toml LILY_CONFIG_MODE=development EXAMPLE_LOG_DIR=/work/logs
CMD ["lily-example-http"]
