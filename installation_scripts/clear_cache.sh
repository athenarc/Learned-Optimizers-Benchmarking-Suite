#!/bin/bash

# Log file location
LOG_FILE="/app/installation_scripts/udf_log.txt"

# Append a timestamped log entry
echo "Script started execution at $(date)" >> "$LOG_FILE"

# Function to display memory usage
display_memory_usage() {
    echo "Memory usage before clearing cache:" >> "$LOG_FILE"
    free -h >> "$LOG_FILE"
    echo "" >> "$LOG_FILE"
}

# Display memory usage before clearing cache
display_memory_usage

# Flush filesystem buffers
echo "Flushing filesystem buffers..." >> "$LOG_FILE"
sync

# Clear the cache
echo "Clearing cache..." >> "$LOG_FILE"
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'

# Restart PostgreSQL
# sudo -u postgres /usr/local/pgsql/12.5/bin/pg_ctl restart -D /app/db/ -l /app/db/server.log -o "-p 5468"

# Display memory usage after clearing cache
echo "" >> "$LOG_FILE"
echo "Memory usage after clearing cache:" >> "$LOG_FILE"
free -h >> "$LOG_FILE"

# Append a timestamped log entry
echo "Script finished execution at $(date)" >> "$LOG_FILE"