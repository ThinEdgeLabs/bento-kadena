FROM rust:1.86 AS builder
COPY . .
RUN cargo build --release --features transactions