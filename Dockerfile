# FROM rust:1.75-buster

# COPY ./libs /app/libs
# COPY ./metrics /app/metrics
# COPY ./nix /app/nix
# COPY ./prisma-fmt /app/prisma-fmt
# COPY ./prisma-schema-wasm /app/prisma-schema-wasm
# COPY ./psl /app/psl
# COPY ./quaint /app/quaint
# COPY ./query-engine /app/query-engine
# COPY ./schema-engine /app/schema-engine
# COPY ./script /app/script
# COPY ./Cargo.lock /app/Cargo.lock
# COPY ./Cargo.toml /app/Cargo.toml
# COPY ./Makefile /app/Makefile
# COPY ./rustfmt.toml /app/rustfmt.toml
# COPY ./flake.nix /app/flake.nix

# WORKDIR /app
# RUN  echo "[source.crates-io]\n\ 
# replace-with = 'rsproxy-sparse'\n\
# [source.rsproxy]\n\
# registry = \"https://rsproxy.cn/crates.io-index\"\n\
# [source.rsproxy-sparse]\n\ 
# registry = \"sparse+https://rsproxy.cn/index/\"\n\ 
# [registries.rsproxy]\n\ 
# index = \"https://rsproxy.cn/crates.io-index\"\n\ 
# [net]\n\
# git-fetch-with-cli = true\n" >> $CARGO_HOME/config

# RUN cargo build --package query-engine

FROM prisma

RUN cargo build --release