ARG POSTGRES_VERSION=18
FROM postgres:${POSTGRES_VERSION}-bookworm

ARG POSTGRES_VERSION=18

# Install prerequisites and configure PostgreSQL for wal2json
RUN apt-get update && apt-mark hold locales && \
    apt-get install -y curl ca-certificates && \
    install -d /usr/share/postgresql-common/pgdg && \
    curl -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc --fail https://www.postgresql.org/media/keys/ACCC4CF8.asc && \
    sh -c 'echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" > /etc/apt/sources.list.d/pgdg.list'

RUN echo "Setting up PostgreSQL ${POSTGRES_VERSION} with wal2json" && \
    apt-get update && \
    apt-get install -y postgresql-server-dev-${POSTGRES_VERSION} && \
    export PATH=/usr/lib/postgresql/${POSTGRES_VERSION}/bin:$PATH && \
    apt-get install -y postgresql-${POSTGRES_VERSION}-wal2json

# output_plugin_libraries allowlists logical decoding plugins; it was
# backported to the latest minor release of every major version we test, so
# bake it into the conf template initdb uses regardless of major version.
RUN echo "output_plugin_libraries = 'pgoutput, test_decoding, wal2json'" >> /usr/share/postgresql/${POSTGRES_VERSION}/postgresql.conf.sample
