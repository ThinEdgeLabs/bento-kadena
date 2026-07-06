ARG BUILDER_IMAGE
FROM ${BUILDER_IMAGE} AS builder

FROM debian:bookworm-slim
RUN apt-get update && apt-get install libpq-dev -y && apt-get install ca-certificates -y
COPY --from=builder ./target/release/indexer ./indexer
COPY .env .env
ENV USE_POLLING=false
# With no arguments: poll for new blocks if the USE_POLLING env var is "true" at
# runtime, otherwise use the SSE headers stream (default mode). Arguments
# (e.g. `backfill`, `gaps`) are passed through to the indexer as-is.
ENTRYPOINT ["/bin/sh", "-c", "if [ $# -gt 0 ]; then exec ./indexer \"$@\"; elif [ \"$USE_POLLING\" = \"true\" ]; then exec ./indexer poll; else exec ./indexer; fi", "--"]
