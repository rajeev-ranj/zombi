# Build stage
FROM rust:1.88-bookworm AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    clang \
    cmake \
    libclang-dev \
    llvm-dev \
    protobuf-compiler \
    pkg-config \
    libssl-dev \
  && rm -rf /var/lib/apt/lists/*

# Copy manifests
COPY Cargo.toml Cargo.lock* ./
COPY build.rs ./
COPY proto ./proto

# Pre-fetch dependencies to improve layer caching.
RUN mkdir -p src benches \
    && echo "fn main() {}" > src/main.rs \
    && echo "" > src/lib.rs \
    && echo "fn main() {}" > benches/write_throughput.rs \
    && cargo fetch \
    && rm -rf src benches


# Copy real source code
COPY src ./src
COPY benches ./benches

# Build the application
ENV LIBCLANG_PATH=/usr/lib/llvm-14/lib

RUN touch src/main.rs src/lib.rs && cargo build --release

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the binary
COPY --from=builder /app/target/release/zombi /usr/local/bin/zombi

# Create data directory
RUN mkdir -p /var/lib/zombi

# Environment variables
ENV ZOMBI_DATA_DIR=/var/lib/zombi
ENV ZOMBI_HOST=0.0.0.0
ENV ZOMBI_PORT=8080
ENV RUST_LOG=zombi=info

EXPOSE 8080

ENTRYPOINT ["zombi"]
