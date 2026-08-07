# The server image.
#
# Section 4.9 requires the ledger to survive a host reboot, which means the
# server has to be a service under a restart policy rather than a process
# somebody left running in a terminal.

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------
FROM rust:1.97-bookworm AS build

WORKDIR /src

# The migrations are copied before the build on purpose. sqlx::migrate! embeds
# them into the binary at COMPILE time, so a build without them produces a
# server that cannot migrate anything.
COPY Cargo.toml Cargo.lock ./
COPY apps/server/Cargo.toml apps/server/Cargo.toml
COPY migrations migrations
COPY apps/server/src apps/server/src

RUN cargo build --release --locked --bin fc-market

# ---------------------------------------------------------------------------
# Runtime
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# pg_dump must be at least as new as the server it reads, and Debian's own
# package is version 15 against a version 17 server. The PostgreSQL archive
# supplies a matching one.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl gnupg \
 && install -d /usr/share/postgresql-common/pgdg \
 && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
      -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
 && echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" \
      > /etc/apt/sources.list.d/pgdg.list \
 && apt-get update \
 && apt-get install -y --no-install-recommends postgresql-client-17 \
 && apt-get purge -y gnupg \
 && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/*

# Not root. The only thing this process needs to write is a socket.
RUN useradd --system --uid 10001 --create-home --home-dir /app app
WORKDIR /app

COPY --from=build /src/target/release/fc-market /usr/local/bin/fc-market
# Read at runtime by the fixture source, which stands in until a provider is
# available.
COPY fixtures /app/fixtures

# Migrate, then serve. One command, so the server can never start against a
# schema that has not been brought forward, and `exec` so that the stop signal
# reaches the server rather than the shell and graceful shutdown works.
RUN printf '%s\n' \
    '#!/bin/sh' \
    'set -e' \
    'fc-market migrate' \
    'exec fc-market serve' \
    > /usr/local/bin/entrypoint.sh \
 && chmod +x /usr/local/bin/entrypoint.sh

USER app
EXPOSE 8090
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
