# Shopping Sessions: a single static Rust binary.
FROM rust:1.97.0-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo
COPY src ./src
RUN cargo build --release --locked

FROM scratch
COPY --from=build /src/target/release/ss /ss
USER 1000:1000
# Diagnostic build: the run time is padded to report measurements (see SS_DIAG in src/main.rs).
ENV SS_AB="SS_MMAP=0,SS_NOTOUCH=1,SS_DEDUP=wide,SS_ROOM=0"
CMD ["/ss"]
