FROM rust:1.56.1 as builder
WORKDIR /archive-gateway
COPY ./ .
RUN cargo build --release

FROM debian:buster-slim
RUN apt-get update && apt-get install -y openssl
WORKDIR /archive-gateway
COPY --from=builder /archive-gateway/target/release/archive-gateway ./archive-gateway
ENTRYPOINT ["/archive-gateway/archive-gateway"]
