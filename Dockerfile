FROM --platform=$BUILDPLATFORM rust:1.61.0 AS builder
ARG TARGETPLATFORM
ARG BUILDPLATFORM
WORKDIR /substrate-gateway
COPY ./ .
RUN scripts/build.sh $TARGETPLATFORM

FROM debian:bullseye-slim
WORKDIR /substrate-gateway
COPY --from=builder /substrate-gateway/target/release/substrate-gateway ./substrate-gateway
ENTRYPOINT ["/substrate-gateway/substrate-gateway"]
EXPOSE 8000
