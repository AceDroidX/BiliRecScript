FROM rust:1.95-alpine AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src/ ./src/

RUN apk add --no-cache musl-dev && \
    cargo build --release

FROM alpine:3.21

WORKDIR /app

RUN apk add --no-cache ca-certificates

COPY --from=builder /app/target/release/bilirec-script /app/bilirec-script

RUN adduser --disabled-password --gecos '' appuser && chown -R appuser /app
USER appuser

CMD ["/app/bilirec-script"]
