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
ENV WORKLOAD_DIR="/app/workloads"
ENV POSTGRES_BIN="${INSTALL_DIR}/${POSTGRES_VERSION}/bin"
ENV PG_CONF="${DB_CLUSTER_DIR}/postgresql.conf"
ENV PG_HBA="${DB_CLUSTER_DIR}/pg_hba.conf"
ENV PATCH_DIR="${BENCHMARKS_DIR}/patches"

# Set the path to the PostgreSQL binaries
ENV PSQL="${POSTGRES_BIN}/psql -p 5468"
ENV PG_CTL="${POSTGRES_BIN}/pg_ctl"
ENV CREATE_DB="${POSTGRES_BIN}/createdb -p 5468 --encoding=UTF8 --lc-collate=en_US.UTF-8 --lc-ctype=en_US.UTF-8"

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
        flex \
        nano \
        sudo \
        locales \
        perl \    
        libperl-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && \
dpkg-reconfigure --frontend=noninteractive locales && \
update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8

ENV LC_ALL=en_US.UTF-8
ENV LANG=en_US.UTF-8

# Create postgres user and group
RUN groupadd -r postgres && useradd -r -g postgres -m postgres

# Set up directories for benchmarks, datasets, and database cluster
WORKDIR /app
RUN mkdir -p \
    benchmark_scripts/{patches,job/{utility,workload},ssb/utility,stats/workload,tpc-ds/tpc-ds-tool/{answer_sets,query_templates,query_variants,specification,tests,tools},tpc-ds/utility,tpc-h/tpc-h-tool/{dbgen/{answers,check_answers,queries,reference,tests,variants},dev-tools,ref_data/{1,100,1000,10000,100000,300,3000,30000}}} \
    datasets db workloads/

# Copy benchmark load scripts and installation scripts
COPY benchmark_scripts/ /app/benchmark_scripts/
COPY installation_scripts/ /app/installation_scripts/
COPY start_postgresql.sh /app/start_postgresql.sh

# Install PostgreSQL from local tar file
WORKDIR /tmp
RUN cp /app/installation_scripts/augmented_postgresql-${POSTGRES_VERSION}.tar.gz . && \
    tar xzvf augmented_postgresql-${POSTGRES_VERSION}.tar.gz

# Apply Lero patch before compiling PostgreSQL
WORKDIR /tmp/postgresql-${POSTGRES_VERSION}
RUN patch -s -p1 < ${PATCH_DIR}/0001-init-lero.patch || \
    (echo "Some hunks failed to apply. Check .rej files" && exit 0)

WORKDIR /tmp/postgresql-${POSTGRES_VERSION}
# Necessary source modifications for the benchmarks to work with PostgreSQL (commenting out for now, because it is breaking due to the lero patch)
# RUN chmod +x /app/installation_scripts/psql_source_modification.sh && /app/installation_scripts/psql_source_modification.sh
# RUN patch -s -p1 < ${PATCH_DIR}/stats_benchmark.patch

# Configure PostgreSQL with --with-perl
RUN ./configure --prefix=${INSTALL_DIR}/${POSTGRES_VERSION} --without-readline --with-perl && \
    make -j && \
    make install && \
    rm -rf /tmp/postgresql-${POSTGRES_VERSION} /tmp/augmented_postgresql-${POSTGRES_VERSION}.tar.gz

# Expose default PostgreSQL port
EXPOSE 5468

WORKDIR /app
RUN cp /app/installation_scripts/bao.tar.gz . && \
    tar -xvf bao.tar.gz

RUN cp ${BENCHMARKS_DIR}/patches/Makefile_pg_bao /app/BaoForPostgreSQL/pg_extension/Makefile

# Build and install the PostgreSQL extension
WORKDIR /app/BaoForPostgreSQL/pg_extension
RUN make USE_PGXS=1 install

# Clone pg_hint_plan repository
WORKDIR /app
RUN cp /app/installation_scripts/pg_hint_plan.tar.gz . && \
    tar xzvf pg_hint_plan.tar.gz
RUN cp ${BENCHMARKS_DIR}/patches/Makefile_pg_hint_plan /app/pg_hint_plan/Makefile

# Build and install the PostgreSQL extension for pg_hint_plan
WORKDIR /app/pg_hint_plan
RUN make && make install

WORKDIR /app
# Custom scripts for starting and configuring the database
RUN chmod +x /app/start_postgresql.sh
RUN chmod +x /app/installation_scripts/psql_configurations.sh
RUN chmod +x /app/installation_scripts/psql_privileges.sh
RUN chmod +x /app/installation_scripts/psql_privileges_job.sh
RUN chmod +x /app/installation_scripts/benchmark_loader.sh
RUN chmod +x /app/installation_scripts/job_loader.sh
RUN chmod +x /app/installation_scripts/clear_cache.sh

# Create the log file and set permissions
RUN touch /app/installation_scripts/udf_log.txt && \
    chown postgres:postgres /app/installation_scripts/udf_log.txt && \
    chmod 666 /app/installation_scripts/udf_log.txt

RUN chown -R postgres:postgres ${DATA_DIR}
RUN chown -R postgres:postgres ${DB_CLUSTER_DIR} && chmod 775 ${DB_CLUSTER_DIR}
RUN chown -R postgres:postgres ${BENCHMARKS_DIR}
RUN chown -R postgres:postgres ${WORKLOAD_DIR}

# Add postgres user to the sudo group
RUN usermod -aG sudo postgres

# Configure sudo to allow passwordless access for postgres (optional)
RUN echo "postgres ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Create suite_user and add to sudoers
RUN useradd -m -s /bin/bash suite_user && \
    usermod -aG sudo suite_user && \
    echo "suite_user ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Switch to the postgres user
USER postgres

ENTRYPOINT ["/app/start_postgresql.sh"]