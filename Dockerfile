# Shopping Sessions: a single static Rust binary.
FROM rust:1.97.0-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo
COPY src ./src
COPY pgo ./pgo
# Built with the execution profile in pgo/ (made by running an instrumented
# build on generated data); without it, should that ever fail, as usual.
RUN RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static -C profile-use=/src/pgo/ss.profdata" cargo build --release --locked \
 || (cargo clean && cargo build --release --locked)

FROM scratch
COPY --from=build /src/target/release/ss /ss
USER 1000:1000
CMD ["/ss"]
