#!/bin/bash

# Exit immediately if a command exits with a non-zero status and print each command
set -e
set +x

# Check if SSB_DIR is provided
SSB_DIR="$1"

if [ -z "$SSB_DIR" ]; then
    echo "Error: SSB_DIR is required as the first argument."
    exit 1
fi

# Verify if the SSB_DIR exists and is a directory
if [ ! -d "$SSB_DIR" ]; then
    echo "Error: Directory $SSB_DIR does not exist."
    exit 1
fi

# Column names for each table (adjust this based on your schema)
declare -A COLUMNS
COLUMNS["lineorder"]="LO_ORDERKEY|LO_LINENUMBER|LO_CUSTKEY|LO_PARTKEY|LO_SUPPKEY|LO_ORDERDATE|LO_ORDERPRIORITY|LO_SHIPPRIORITY|LO_QUANTITY|LO_EXTENDEDPRICE|LO_ORDTOTALPRICE|LO_DISCOUNT|LO_REVENUE|LO_SUPPLYCOST|LO_TAX|LO_COMMITDATE|LO_SHIPMODE"
COLUMNS["part"]="P_PARTKEY|P_NAME|P_MFGR|P_CATEGORY|P_BRAND1|P_COLOR|P_TYPE|P_SIZE|P_CONTAINER"
COLUMNS["supplier"]="S_SUPPKEY|S_NAME|S_ADDRESS|S_CITY|S_NATION|S_REGION|S_PHONE"
COLUMNS["customer"]="C_CUSTKEY|C_NAME|C_ADDRESS|C_CITY|C_NATION|C_REGION|C_PHONE|C_MKTSEGMENT"
COLUMNS["date"]="D_DATEKEY|D_DATE|D_DAYOFWEEK|D_MONTH|D_YEAR|D_YEARMONTHNUM|D_YEARMONTH|D_DAYNUMINWEEK|D_DAYNUMINMONTH|D_DAYNUMINYEAR|D_MONTHNUMINYEAR|D_WEEKNUMINYEAR|D_SELLINGSEASON|D_LASTDAYINWEEKFL|D_LASTDAYINMONTHFL|D_HOLIDAYFL|D_WEEKDAYFL"

# Function to prepend a line to a file
prepend_line() {
    local filename="$1"
    local line="$2"

    if [ -f "$filename" ]; then
        { echo "$line"; cat "$filename"; } > "${filename}.tmp" && mv "${filename}.tmp" "$filename" >/dev/null 2>&1
    else
        echo "File not found: $filename"
    fi
}

# Main loop to process each table
for table_name in "${!COLUMNS[@]}"; do
    filename="${SSB_DIR}/${table_name}.tbl"
    line="${COLUMNS[$table_name]}"
    prepend_line "$filename" "$line"
    sed -i 's/|$//' "$filename"
    echo "Removed last delimiter from $filename"    
done