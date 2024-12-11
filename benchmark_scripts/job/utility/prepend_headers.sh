#!/bin/bash

# Exit immediately if a command exits with a non-zero status and print each command
set -e
set +x

# Check if JOB_DIR is provided
JOB_DIR="$1"

if [ -z "$JOB_DIR" ]; then
    echo "Error: JOB_DIR is required as the first argument."
    exit 1
fi

# Verify if the JOB_DIR exists and is a directory
if [ ! -d "$JOB_DIR" ]; then
    echo "Error: Directory $JOB_DIR does not exist."
    exit 1
fi

# Column names for the tables
declare -A COLUMNS=(
    ["aka_name"]="id,person_id,name,imdb_index,name_pcode_cf,name_pcode_nf,surname_pcode,md5sum"
    ["aka_title"]="id,movie_id,title,imdb_index,kind_id,production_year,phonetic_code,episode_of_id,season_nr,episode_nr,note,md5sum"
    ["cast_info"]="id,person_id,movie_id,person_role_id,note,nr_order,role_id"
    ["char_name"]="id,name,imdb_index,imdb_id,name_pcode_nf,surname_pcode,md5sum"
    ["company_name"]="id,name,country_code,imdb_id,name_pcode_nf,name_pcode_sf,md5sum"
    ["company_type"]="id,kind"
    ["comp_cast_type"]="id,kind"
    ["complete_cast"]="id,movie_id,subject_id,status_id"
    ["info_type"]="id,info"
    ["keyword"]="id,keyword,phonetic_code"
    ["kind_type"]="id,kind"
    ["link_type"]="id,link"
    ["movie_companies"]="id,movie_id,company_id,company_type_id,note"
    ["movie_info"]="id,movie_id,info_type_id,info,note"
    ["movie_info_idx"]="id,movie_id,info_type_id,info,note"
    ["movie_keyword"]="id,movie_id,keyword_id"
    ["movie_link"]="id,movie_id,linked_movie_id,link_type_id"
    ["name"]="id,name,imdb_index,imdb_id,gender,name_pcode_cf,name_pcode_nf,surname_pcode,md5sum"
    ["person_info"]="id,person_id,info_type_id,info,note"
    ["role_type"]="id,role"
    ["title"]="id,title,imdb_index,kind_id,production_year,imdb_id,phonetic_code,episode_of_id,season_nr,episode_nr,series_years,md5sum"
)

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
    filename="${JOB_DIR}/${table_name}.csv"
    line="${COLUMNS[$table_name]}"
    prepend_line "$filename" "$line"
done

echo "Headers prepended successfully to all CSV files in $JOB_DIR."
