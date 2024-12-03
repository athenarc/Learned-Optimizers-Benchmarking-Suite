# Use an official Ubuntu 22.04 base image
FROM ubuntu:22.04

# Set environment variables
ENV OS_LOCALE=en_US.UTF-8 DEBIAN_FRONTEND=noninteractive

# Install necessary dependencies
RUN apt-get update && apt-get install -y locales curl ca-certificates gnupg2 lsb-release nano postgresql-common \
    python3 python3-pip python3-venv python3-dev libpq-dev git wget && apt-get clean

# PostgreSQL 12 Repository
RUN install -d /usr/share/postgresql-common/pgdg
RUN curl -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc --fail https://www.postgresql.org/media/keys/ACCC4CF8.asc
RUN sh -c 'echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'

# Install PostgreSQL 12
RUN apt-get update
RUN apt-get install -y postgresql-12 postgresql-client-12 postgresql-server-dev-12 && apt-get clean
EXPOSE 5432/tcp

RUN mkdir -p /var/lib/postgresql/data && chown -R postgres:postgres /var/lib/postgresql

# Define a default value for JOB_SETUP if not provided
ARG JOB_SETUP=false
ENV JOB_SETUP=${JOB_SETUP}

# Conditionally clone and install Cinemagoer based on JOB_SETUP
RUN if [ "$JOB_SETUP" = "true" ]; then \
        echo "Cloning Cinemagoer repository..."; \
        git clone https://github.com/cinemagoer/cinemagoer.git /cinemagoer && \
        cd /cinemagoer && \
        echo "Creating virtual environment..."; \
        python3 -m venv /cinemagoer/myenv && \
        /cinemagoer/myenv/bin/pip install --upgrade pip && \
        /cinemagoer/myenv/bin/pip install git+https://github.com/cinemagoer/cinemagoer && \
        /cinemagoer/myenv/bin/pip install psycopg2 && \
        /cinemagoer/myenv/bin/pip install -r /cinemagoer/requirements.txt && \
        /cinemagoer/myenv/bin/pip install --upgrade sqlalchemy==1.4 && \
        echo "Cinemagoer setup complete."; \
    else \
        echo "Skipping Cinemagoer setup."; \
    fi
  
RUN if [ "$JOB_SETUP" = "true" ]; then \
    echo "Creating job-zips directory..."; \
    mkdir -p /cinemagoer/job-zips && chown postgres:postgres /cinemagoer/job-zips; \
fi

# Copy the new scripts
COPY db_setup.sh /usr/local/bin/db_setup.sh
COPY populate_imdb.sh /usr/local/bin/populate_imdb.sh
RUN chmod +x /usr/local/bin/db_setup.sh /usr/local/bin/populate_imdb.sh

# Copy the entrypoint
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Switch to the PostgreSQL user
USER postgres

# Run the PostgreSQL server
CMD ["/usr/local/bin/entrypoint.sh"]
