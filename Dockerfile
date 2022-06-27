FROM --platform=$BUILDPLATFORM rust:1.61.0 AS builder
ARG TARGETPLATFORM
ARG BUILDPLATFORM
WORKDIR /archive-gateway
COPY ./ .
RUN scripts/build.sh $TARGETPLATFORM

FROM debian:bullseye-slim
WORKDIR /archive-gateway
COPY --from=builder /archive-gateway/target/release/archive-gateway ./archive-gateway
ENTRYPOINT ["/archive-gateway/archive-gateway"]
EXPOSE 8000
