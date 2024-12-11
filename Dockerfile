# Use an official Ubuntu base image
FROM ubuntu:22.04

# Set environment variables to non-interactive for automated builds
ENV DEBIAN_FRONTEND=noninteractive

# Define PostgreSQL version
ENV INSTALL_DIR=/usr/local/pgsql
ENV POSTGRES_VERSION=12.5 

# Define directories for benchmarks, data, and PostgreSQL binary
ENV BENCHMARKS_DIR="/app/benchmark_scripts"
ENV DATA_DIR="/app/datasets"
ENV DB_CLUSTER_DIR="/app/db"
ENV POSTGRES_BIN="${INSTALL_DIR}/${POSTGRES_VERSION}/bin"
ENV PG_CONF="${DB_CLUSTER_DIR}/postgresql.conf"
ENV PG_HBA="${DB_CLUSTER_DIR}/pg_hba.conf"
ENV PATCH_DIR="${BENCHMARKS_DIR}/patches"

# Set the path to the PostgreSQL binaries
ENV PSQL="${POSTGRES_BIN}/psql"
ENV PG_CTL="${POSTGRES_BIN}/pg_ctl"
ENV CREATE_DB="${POSTGRES_BIN}/createdb"

# Update and install dependencies
RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
        software-properties-common \
        build-essential \
        zlib1g-dev \
        libncurses5-dev \
        libgdbm-dev \
        libnss3-dev \
        libssl-dev \
        libreadline-dev \
        libffi-dev \
        wget \
        gcc \
        g++ \
        make \
        git \
        zip \
        unzip \
        g++-9 \
        gcc-9 \
        bison \
        flex && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Create postgres user and group
RUN groupadd -r postgres && useradd -r -g postgres -m postgres

# Download and install PostgreSQL if not already installed
WORKDIR /tmp
RUN wget https://ftp.postgresql.org/pub/source/v${POSTGRES_VERSION}/postgresql-${POSTGRES_VERSION}.tar.gz && \
    tar xzvf postgresql-${POSTGRES_VERSION}.tar.gz

# Set up directories for benchmarks, datasets, and database cluster
WORKDIR /app
RUN mkdir -p \
    benchmark_scripts/{patches,job/utility,ssb/utility,stats,tpc-ds/tpc-ds-tool/{answer_sets,query_templates,query_variants,specification,tests,tools},tpc-ds/utility,tpc-h/tpc-h-tool/{dbgen/{answers,check_answers,queries,reference,tests,variants},dev-tools,ref_data/{1,100,1000,10000,100000,300,3000,30000}}} \
    datasets db

# Copy benchmark load scripts
COPY benchmark_scripts/ /app/benchmark_scripts/

WORKDIR /tmp/postgresql-${POSTGRES_VERSION}
# Necessary source modifications for the benchmarks to work with PostgreSQL
COPY psql_source_modification.sh /app/psql_source_modification.sh
RUN chmod +x /app/psql_source_modification.sh && /app/psql_source_modification.sh
# RUN patch -s -p1 < ${PATCH_DIR}/stats_benchmark.patch

RUN ./configure --prefix=${INSTALL_DIR}/${POSTGRES_VERSION} --enable-depend --enable-cassert CFLAGS="-ggdb -O0" && \
    make && \
    make install && \
    rm -rf /tmp/postgresql-${POSTGRES_VERSION} /tmp/postgresql-${POSTGRES_VERSION}.tar.gz

# Expose default PostgreSQL port
EXPOSE 5432

WORKDIR /app
# Custom script that starts the database
COPY start_postgresql.sh /app/start_postgresql.sh
RUN chmod +x /app/start_postgresql.sh

# Custom script that configures the database -> will run in start_postgresql.sh
COPY psql_configurations.sh /app/psql_configurations.sh
RUN chmod +x /app/psql_configurations.sh

# Custom script that handles the privileges users have on the database -> will run in start_postgresql.sh
COPY psql_privileges.sh /app/psql_privileges.sh
RUN chmod +x /app/psql_privileges.sh

# Custom script that loads the benchmark datasets && workloads -> will run in start_postgresql.sh
COPY benchmark_loader.sh /app/benchmark_loader.sh
RUN chmod +x /app/benchmark_loader.sh

RUN chown -R postgres:postgres ${DATA_DIR}
RUN chown -R postgres:postgres ${DB_CLUSTER_DIR}
RUN chown -R postgres:postgres ${BENCHMARKS_DIR}

# Switch to the postgres user
USER postgres

ENTRYPOINT ["/app/start_postgresql.sh"]