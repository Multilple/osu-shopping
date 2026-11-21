# Shopping Sessions: a single static Rust binary.
FROM rust:1.97.0-slim-bookworm AS build
# (Diagnostic build only: the kit's data generator, to time a full-size run.)
COPY gen /gen
RUN cd /gen && RUSTFLAGS="-C target-feature=+crt-static" cargo build --release
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo
COPY src ./src
RUN cargo build --release --locked

FROM scratch
COPY --from=build /src/target/release/ss /ss
COPY --from=build /gen/target/release/generate /gen
USER 1000:1000
# Diagnostic build: it describes the machine in its log and writes no output (see SS_DIAG in src/main.rs).
ENV SS_DIAG=log3
ENV SS_DIAG_END=fail
CMD ["/ss"]
