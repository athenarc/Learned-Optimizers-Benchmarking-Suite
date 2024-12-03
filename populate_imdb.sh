#!/bin/bash
set -e

echo "Populating IMDb database..."

# Download IMDb data
wget -P /cinemagoer/job-zips ftp://ftp.fu-berlin.de/misc/movies/database/frozendata/*gz

# Run the script to populate the database
/cinemagoer/myenv/bin/python3 /cinemagoer/bin/imdbpy2sql.py -d /cinemagoer/job-zips/ -u "postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@localhost/imdbload?client_encoding=UTF8"