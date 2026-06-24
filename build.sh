#!/bin/sh

sudo rm -rf dst \
    && mkdir dst \
    && docker run -v ./src:/app/src \
        -v ./.cargo:/app/.cargo \
        -v ./Cargo.toml:/app/Cargo.toml -v ./Cargo.lock:/app/Cargo.lock \
        -v ./dst:/app/target -t webtalk:builder /root/.cargo/bin/cargo build --release --features log,es \
    && cp dst/release/webtalk ./ \
    && sudo rm -rf dst

zip webtalk.zip webtalk  && scp ./webtalk.zip root@10.10.181.175:/data/workspace/webtalk/ && ssh root@10.10.181.175