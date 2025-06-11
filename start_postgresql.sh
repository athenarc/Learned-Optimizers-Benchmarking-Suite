#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Initialize PostgreSQL database cluster if it doesn't already exist
if [ ! -f ${DB_CLUSTER_DIR}/PG_VERSION ]; then
    echo 'Initializing database cluster...'
    $PG_CTL -D "$DB_CLUSTER_DIR" initdb
fi

# Run the database configuration script
echo 'Running database configurations setup...'
/app/installation_scripts/psql_configurations.sh

# Move the extra PostgreSQL configuration files to the database cluster directory
echo 'Moving extra PostgreSQL configuration files...'
mv /app/installation_scripts/postgresql_lero.conf "$DB_CLUSTER_DIR/postgresql_lero.conf"
mv /app/installation_scripts/postgresql_usual.conf "$DB_CLUSTER_DIR/postgresql_usual.conf"

# Start PostgreSQL in the background
echo 'Starting PostgreSQL...'
$PG_CTL -D "$DB_CLUSTER_DIR" start -o "-c logging_collector=on -c log_directory='/app/db/log' -c log_filename='postgresql-%Y-%m-%d_%H%M%S.log'" &

# Wait for PostgreSQL to be ready
echo 'Waiting for PostgreSQL to be ready...'
sleep 5

# Load the benchmarks datasets and their workloads into the database
echo 'Running benchmark loaders...'
/app/installation_scripts/benchmark_loader.sh
# /app/installation_scripts/job_loader.sh

# Run the database configuration script
echo 'Revoking database privileges...'
/app/installation_scripts/psql_privileges.sh
# /app/installation_scripts/psql_privileges_job.sh

# Create the plperl and plperlu extensions
echo 'Creating plperl and plperlu extensions...'
$PSQL -U suite_user -d imdbload -c "CREATE EXTENSION plperl;"
$PSQL -U suite_user -d imdbload -c "CREATE LANGUAGE plperlu;"

# Define the clear_cache() function
echo 'Defining the clear_cache() function...'
$PSQL -U suite_user -d imdbload -c "CREATE OR REPLACE FUNCTION clear_cache() RETURNS boolean AS \$\$
my \$script = '/app/installation_scripts/clear_cache.sh';
my \$log_file = '/app/installation_scripts/udf_log.txt';
if (-e \$script) {
    system(\"nohup \$script >> \$log_file 2>&1 &\") == 0 or die \"Script execution failed: \$!\";
    return 1;
} else {
    die \"Script not found: \$script\";
}
return 0;
\$\$ LANGUAGE plperlu;"

# Define the write_lero_card_file() function
echo 'Defining the write_lero_card_file() function...'
$PSQL -U suite_user -d imdbload -c "CREATE OR REPLACE FUNCTION write_lero_card_file(
    file_name TEXT,
    content TEXT
) RETURNS boolean AS \$\$
my \$data_dir = '/app/db/';
my \$file_path = \"\$data_dir/\$_[0]\";

# Write the file
open(my \$fh, '>', \$file_path) or die \"Cannot open file \$file_path: \$!\";
print \$fh \$_[1];
close(\$fh);

# Set proper permissions
chmod 0664, \$file_path or die \"Failed to set file permissions: \$!\";

return 1;
\$\$ LANGUAGE plperlu SECURITY DEFINER;"

# Create the lero_files directory if it doesn't exist
$PSQL -U suite_user -d imdbload -c "SELECT write_lero_card_file('.keep', '')"

# Keep the container running
tail -f /dev/null
