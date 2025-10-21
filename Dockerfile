# syntax=docker/dockerfile:1.6

FROM node:20-alpine AS web
WORKDIR /web
COPY frontend/package*.json ./
RUN npm install
COPY frontend/ .
RUN npm run build

FROM rust:1.88-slim AS dev
WORKDIR /workspace
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libssl-dev ca-certificates nodejs npm \
    && rm -rf /var/lib/apt/lists/*
ENV CARGO_HOME=/usr/local/cargo
ENV RUSTUP_HOME=/usr/local/rustup
CMD ["bash"]

FROM dev AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && echo "fn main() {}" > src/main.rs \
    && cargo fetch \
    && rm -rf src
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim AS runtime
RUN useradd --uid 1000 --create-home pgmon \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && install -d -o pgmon -g pgmon /var/lib/pgmon/history \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /home/pgmon
COPY --from=build /app/target/release/pgmon /usr/local/bin/pgmon
COPY --from=web /web/dist /opt/pgmon/ui
ENV PGMON_DATA_DIR=/var/lib/pgmon
USER pgmon
EXPOSE 8181
ENTRYPOINT ["/usr/local/bin/pgmon"]
