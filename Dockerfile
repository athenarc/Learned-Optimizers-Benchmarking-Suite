# Use an official Ubuntu 22.04 base image
FROM ubuntu:22.04

ENV OS_LOCALE=en_US.UTF-8 DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y locales && locale-gen ${OS_LOCALE}

RUN apt-get install -y curl ca-certificates gnupg2 lsb-release nano postgresql-common && apt-get clean

# PostgreSQL 12 Repository
RUN install -d /usr/share/postgresql-common/pgdg
RUN curl -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc --fail https://www.postgresql.org/media/keys/ACCC4CF8.asc
RUN sh -c 'echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'

# Install PostgreSQL 12
RUN apt-get update
RUN apt-get install -y postgresql-12 postgresql-client-12 postgresql-server-dev-12 && apt-get clean
EXPOSE 5432/tcp

RUN mkdir -p /var/lib/postgresql/data && chown -R postgres:postgres /var/lib/postgresql

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Switch to the PostgreSQL user and initialize the database
USER postgres

# Run the PostgreSQL server
CMD ["/usr/local/bin/entrypoint.sh"]
