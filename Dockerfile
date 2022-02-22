FROM rust:1.56.1 as builder
WORKDIR /archive-gateway
COPY ./ .
RUN cargo build --release

FROM debian:buster-slim
WORKDIR /archive-gateway
COPY --from=builder /archive-gateway/target/release/archive-gateway-example ./archive-gateway
CMD ["/archive-gateway/archive-gateway"]
