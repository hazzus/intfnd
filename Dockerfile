FROM rust:1-bookworm AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y cmake libssl-dev pkg-config && rm -rf /var/lib/apt/lists/*

# cache dependencies — only reruns when Cargo.toml/Cargo.lock change
COPY Cargo.toml Cargo.lock ./
COPY migrations ./migrations
RUN mkdir src && echo 'fn main() {}' > src/main.rs
RUN cargo build --release
RUN rm -f target/release/deps/intfnd*

# build real source
COPY src ./src
COPY templates ./templates
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y libssl3 ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/intfnd /usr/local/bin/intfnd
EXPOSE 3000
CMD ["intfnd"]
