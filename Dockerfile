FROM postgres:16

RUN apt-get update
RUN apt-mark hold locales
RUN apt-get install curl ca-certificates -y
RUN install -d /usr/share/postgresql-common/pgdg
RUN curl -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc --fail https://www.postgresql.org/media/keys/ACCC4CF8.asc
RUN sh -c 'echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
RUN apt-get update
RUN apt-get install postgresql-server-dev-16 -y
RUN sh -c 'export PATH=/usr/lib/postgresql/16/bin:$PATH'
RUN apt-get install postgresql-16-wal2json -y
