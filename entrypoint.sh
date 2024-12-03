#!/bin/bash
set -e

# Initialize database if necessary
if [ ! -d "/var/lib/postgresql/data/PG_VERSION" ]; then
    /usr/local/bin/db_setup.sh
fi

# Start PostgreSQL server
exec /usr/lib/postgresql/12/bin/postgres -D "/var/lib/postgresql/data" &

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
sleep 10

# If JOB_SETUP is true, populate IMDb data
if [ "$JOB_SETUP" = "true" ]; then
    echo "Starting IMDb data population..."

    # Check if the job-zips folder exists and if it's empty
    if [ -d "/cinemagoer/job-zips" ] && [ "$(ls -A /cinemagoer/job-zips)" ]; then
        echo "IMDb data already populated."
    else
        /usr/local/bin/populate_imdb.sh
    fi
else
    echo "Skipping IMDb data population (JOB_SETUP is not 'true')."
fi

wait