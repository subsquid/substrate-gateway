FROM rust:1.61.0 as builder
WORKDIR /archive-gateway
COPY ./ .
RUN cargo build --release -j 1

FROM debian:bullseye-slim
RUN apt-get update && apt-get install -y openssl
WORKDIR /archive-gateway
COPY --from=builder /archive-gateway/target/release/archive-gateway ./archive-gateway
ENTRYPOINT ["/archive-gateway/archive-gateway"]
