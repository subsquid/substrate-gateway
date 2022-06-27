#!/bin/bash

release=$1
tag=$2

version="$(python scripts/pkg-version.py)" || exit 1
major=$(echo "$version" | cut -d '.' -f1) || exit 1

git tag -a "v${version}" -m "v${version}"

docker buildx build . --platform "linux/amd64,linux/arm64" \
    --push \
    --label "org.opencontainers.image.url=https://github.com/subsquid/substrate-gateway/tree/$(git rev-parse HEAD)" \
    -t "subsquid/substrate-gateway:$version" \
    -t "subsquid/substrate-gateway:$major" \
    -t "subsquid/substrate-gateway:$tag" \
    -t "subsquid/substrate-gateway:$release" || exit 1
