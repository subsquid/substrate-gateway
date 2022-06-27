#!/bin/bash

target=$1

if [ "$target" == "linux/arm64" ]
then
    apt-get update && apt-get upgrade -y || exit 1
    apt-get install -y g++-aarch64-linux-gnu libc6-dev-arm64-cross || exit 1
    rustup target add aarch64-unknown-linux-gnu
    rustup toolchain install stable-aarch64-unknown-linux-gnu

    CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
    CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc \
    CXX_aarch64_unknown_linux_gnu=aarch64-linux-gnu-g++ \
    cargo build --release --target aarch64-unknown-linux-gnu
    mv ./target/aarch64-unknown-linux-gnu/release/archive-gateway ./target/release/
else
    cargo build --release
fi
