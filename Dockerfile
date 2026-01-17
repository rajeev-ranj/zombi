# Build stage
FROM rust:1.75-bookworm as builder

WORKDIR /app

# Install protobuf compiler
RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*

# Copy manifests
COPY Cargo.toml Cargo.lock* ./
COPY build.rs ./
COPY proto ./proto

# Create dummy src to cache dependencies
RUN mkdir src && echo "fn main() {}" > src/main.rs && echo "" > src/lib.rs

# Build dependencies only
RUN cargo build --release && rm -rf src

# Copy real source code
COPY src ./src

# Build the application
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
