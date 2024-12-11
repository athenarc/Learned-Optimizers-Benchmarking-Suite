#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -x

# Configure PostgreSQL to allow remote access
echo "listen_addresses = '*'" >> "$PG_CONF"