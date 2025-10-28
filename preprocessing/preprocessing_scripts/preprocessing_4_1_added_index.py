import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import query_directory, TABLES, DB_CONFIG, connect_to_db, get_alchemy_engine
import pandas as pd
from tqdm import tqdm
from sql_parser import parse_sql
from file_utils import read_queries_from_directory
from query_template import get_query_template_no_correl, get_query_template, get_query_template_no_selectivity
from selectivity import get_database_schema, DatabaseCache
from typing import Dict, Any
from selectivity import fetch_column_data, estimate_selectivity
import tqdm
from pathlib import Path
import psycopg2

def get_selectivity_from_query_template(query_template, db_schema):
    """
    Given a query template, selectivity and the database schema, return queries with similar selectivities
    """
    # Extract the filter conditions from the query template
    filter_conditions = query_template['filter_conditions']
    
    # Initialize a list to store the selectivities
    selectivities = []
    
    # Iterate through the filter conditions
    for condition in filter_conditions:
        # Get the table and column from the condition
        table, column = condition.split('.')
        
        # Fetch the data for the column from the database
        column_data = fetch_column_data(db_schema, table, column)
        
        if column_data is not None:
            # Estimate the selectivity for the condition
            selectivity = estimate_selectivity(column_data, condition)
            selectivities.append(selectivity)
    
    return selectivities

def generate_selectivity_queries(conn, table_name: str, column_name: str, operator: str, old_value: str,
                              original_query: str, queryName: str, num_joins: int, output_dir: Path) -> Dict[str, str]:
    """
    Generate queries with filters at different selectivity points (10%, 25%, 75%, 100%)
    and write them to SQL files in the specified directory structure.
    
    Args:
        conn: Active database connection object
        table_name: Name of the table to analyze
        column_name: Name of the column to use for filtering
        operator: The operator used in the original filter (=, >, <, etc.)
        original_query: The base query to modify with filters
        base_output_dir: Base directory for output files
        query_id: Identifier for this query (e.g., '55311')
    
    Returns:
        Dictionary with selectivity levels as keys and file paths as values
    """
    # Filter out all whitespace and newlines
    original_query = re.sub(r'\s+', ' ', original_query).strip()
    print()
    print(f"Generating selectivity queries for {queryName} with {num_joins} joins")
    print(f"Original query: {original_query}")
    print(f"Table: {table_name}, Column: {column_name}, Operator: {operator}, Old Value: {old_value}")
    query_id = queryName.split('.')[0]  # Extract the query ID from the filename
    # First check if we have a valid connection
    if conn is None or conn.closed:
        print("Error: Database connection is not available or closed")
        return {}

    # Create directory structure if it doesn't exist
    parent_dir = output_dir / f'{num_joins}_joins'
    query_dir = parent_dir / query_id
    os.makedirs(query_dir, exist_ok=True)
    
    # Use percentiles to get reliable values without NULLs
    dist_query = f"""
    WITH valid_values AS (
        SELECT {column_name} 
        FROM {table_name}
        WHERE {column_name} IS NOT NULL
    ),
    percentiles AS (
        SELECT 
            percentile_disc(0.01) WITHIN GROUP (ORDER BY {column_name}) AS p01,
            percentile_disc(0.10) WITHIN GROUP (ORDER BY {column_name}) AS p10,
            percentile_disc(0.20) WITHIN GROUP (ORDER BY {column_name}) AS p20,
            percentile_disc(0.40) WITHIN GROUP (ORDER BY {column_name}) AS p40,
            percentile_disc(0.60) WITHIN GROUP (ORDER BY {column_name}) AS p60,
            percentile_disc(0.80) WITHIN GROUP (ORDER BY {column_name}) AS p80,
            percentile_disc(0.99) WITHIN GROUP (ORDER BY {column_name}) AS p99
        FROM valid_values
    )
    SELECT 
        p01 AS "1",
        p10 AS "10",
        p20 AS "20",
        p40 AS "40",
        p60 AS "60",
        p80 AS "80",
        p99 AS "99"
    FROM percentiles;
    """
    
    try:
        # Execute the distribution query
        dist_df = pd.read_sql(dist_query, conn)
        
        if dist_df.empty:
            return {}
        
        # Get sample values for each selectivity range
        samples = {
            '0%': dist_df.iloc[0]['1'],
            '10%': dist_df.iloc[0]['10'],
            '20%': dist_df.iloc[0]['20'],
            '40%': dist_df.iloc[0]['40'],
            '60%': dist_df.iloc[0]['60'],
            '80%': dist_df.iloc[0]['80'],
            '100%': dist_df.iloc[0]['99'],
        }

        generated_files = {}
        
        # Generate queries and write to files
        for key, value in samples.items():
            if pd.isna(value):  # Skip if we got NULL despite precautions
                continue
            
            if value is not None:
                # If the column is not a string column, do the below
                if not isinstance(value, str):
                    # Replace the filter in the base query with the new value
                    modified_query = original_query.replace(
                        f"{column_name} {operator} {old_value}",  # Note the quotes around old_value
                        f"{column_name} < {value}"
                    )
                else:
                    escaped_value = value.replace("'", "''")
                    modified_query = original_query.replace(
                        f"{column_name} {operator} '{old_value}'",  # Note the quotes around ?
                        f"{column_name} < '{escaped_value}'"  # Wrap value in quotes
                    )                    
                
                # Determine filename suffix based on selectivity level
                suffix = {
                    '0%': '0',
                    '10%': '10',
                    '20%': '20',
                    '40%': '40',
                    '60%': '60',
                    '80%': '80',
                    '100%': '100'
                }.get(key, 'x')
                
                parent_dir = query_dir / key 
                os.makedirs(parent_dir, exist_ok=True)
                
                # Create the output filename
                output_file = parent_dir / f"{query_id}_{suffix}.sql"
                
                # Write the query to file with proper indentation
                with open(output_file, 'w') as f:
                    f.write(modified_query)
                
                generated_files[key] = str(output_file)
        
        return generated_files
    
    except Exception as e:
        print(f"Error generating selectivity queries: {e}")
        return {}

import time
import json
from sqlalchemy import text

def execute_explain_analyze_default(conn, query: str) -> dict:
    conn = psycopg2.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        dbname=DB_CONFIG['dbname'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password']
    )
    """Execute EXPLAIN ANALYZE on the given query and return the QEP data"""
    try:
        explain_query = f"EXPLAIN (ANALYZE, FORMAT JSON) {query}"
        cursor = conn.cursor()
        cursor.execute(explain_query)
        qep_data = cursor.fetchall()
        
        # Return the QEP data
        return qep_data[0][0]
    except Exception as e:
        print(f"Error executing EXPLAIN ANALYZE: {str(e)}")
        return {}

def execute_explain_analyze_force_seq_scan(conn, query: str, examined_table: str) -> dict:
    conn = psycopg2.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        dbname=DB_CONFIG['dbname'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password']
    )
    """Execute EXPLAIN ANALYZE on the given query and return the QEP data"""
    try:
        pg_hint = f"/*+ SeqScan({examined_table}) */ "
        query = pg_hint + query
        explain_query = f"EXPLAIN (ANALYZE, FORMAT JSON) {query}"
        cursor = conn.cursor()
        cursor.execute(explain_query)
        qep_data = cursor.fetchall()
        
        # Return the QEP data
        return qep_data[0][0]
    except Exception as e:
        print(f"Error executing EXPLAIN ANALYZE: {str(e)}")
        return {}

def execute_explain_analyze_force_index_scan(conn, query: str, examined_table: str) -> dict:
    conn = psycopg2.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        dbname=DB_CONFIG['dbname'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password']
    )
    """Execute EXPLAIN ANALYZE on the given query and return the QEP data"""
    try:
        pg_hint = f"/*+ IndexScan({examined_table}) */ "
        query = pg_hint + query
        explain_query = f"EXPLAIN (ANALYZE, FORMAT JSON) {query}"
        cursor = conn.cursor()
        cursor.execute(explain_query)
        qep_data = cursor.fetchall()
        
        # Return the QEP data
        return qep_data[0][0]
    except Exception as e:
        print(f"Error executing EXPLAIN ANALYZE: {str(e)}")
        return {}
    
def save_qep_to_file(qep_data: dict, output_path: Path):
    """Save the QEP data to a JSON file"""
    try:
        with open(output_path, 'w') as f:
            json.dump(qep_data, f, indent=2)
        print(f"Saved QEP to {output_path}")
    except Exception as e:
        print(f"Error saving QEP to file: {str(e)}")

def process_generated_queries(conn, queries: dict, examined_table: str = ""):
    """Process all generated queries and save their QEPs"""
    for sel_level, query_path in queries.items():
        try:
            # Read the generated query
            with open(query_path, 'r') as f:
                query = f.read().strip()
            
            if not query:
                print(f"Skipping empty query in {query_path}")
                continue
            
            # Get QEP
            qep_data = execute_explain_analyze_default(conn, query)
            if not qep_data:
                continue
            
            # Save to classic_qep.json in the same directory
            output_path = Path(query_path).parent / "classic_qep.json"
            save_qep_to_file(qep_data, output_path)
            
            seq_scan_qep_data = execute_explain_analyze_force_seq_scan(conn, query, examined_table)
            if not seq_scan_qep_data:
                continue
            # Save to classic_qep.json in the same directory
            output_path = Path(query_path).parent / "seq_scan_qep.json"
            save_qep_to_file(seq_scan_qep_data, output_path)
            index_scan_qep_data = execute_explain_analyze_force_index_scan(conn, query, examined_table)

            if not index_scan_qep_data:
                continue
            # Save to classic_qep.json in the same directory
            output_path = Path(query_path).parent / "index_scan_qep.json"
            save_qep_to_file(index_scan_qep_data, output_path)
            
        except Exception as e:
            print(f"Error processing {query_path}: {str(e)}")

import re

def process_queries(query_directory, output_dir: Path, max_queries=None):
    """Main function to process SQL queries from a directory."""
    try:
        queries, query_names = read_queries_from_directory(query_directory)
        print(f"Read {len(queries)} queries from {query_directory}")

        for count, (query, query_name) in enumerate(zip(queries, query_names)):
            if max_queries and count >= max_queries:
                break

            print(f"\nProcessing query {count}: {query_name}")
            process_single_query(query, query_name, output_dir)

    except Exception as e:
        print(f"Error processing queries: {str(e)}")

def process_single_query(query, query_name, output_dir: Path):
    """Process a single SQL query."""
    try:
        parsed_query = parse_sql(query)
        num_joins = len(parsed_query['joins'])
        filters = parsed_query['filters']
        aliases = parsed_query['aliases']
        
        # Check for production_year filter
        filter_condition = find_production_year_filter(filters, query_name)
        if not filter_condition:
            return

        # Extract filter components
        alias, column_name, operator, filter_value = extract_filter_components(filter_condition)
        table_name = resolve_table_name(alias, aliases)
        
        if not table_name:
            print(f"Could not resolve table name for alias {alias} in query {query_name}")
            return

        examined_column = f"{alias}.{column_name}"
        
        # Connect to database and process
        conn = connect_to_db()
        if not conn:
            print("Failed to connect to database")
            return

        try:
            # Generate and process selectivity queries
            selectivity_queries = generate_selectivity_queries(
                conn, table_name, column_name, operator, filter_value, 
                query, query_name, num_joins, output_dir
            )
            
            print("\nGenerated queries with different selectivities:")
            for selectivity, q in selectivity_queries.items():
                print(f"{selectivity}: {q}")

            process_generated_queries(conn, selectivity_queries, examined_column)
            
        finally:
            conn.close()

    except Exception as e:
        print(f"Error processing query {query_name}: {str(e)}")

def find_production_year_filter(filters, query_name):
    """Find and validate production_year filter condition."""
    for filter_cond in filters:
        if 't.production_year' in filter_cond:
            if any(op in filter_cond for op in ('=', 'BETWEEN')):
                print(f"Skipping query {query_name} - uses '=' or 'BETWEEN'")
                return None
            return filter_cond
    
    print(f"Skipping query {query_name} - no production_year filter")
    return None

def extract_filter_components(filter_condition):
    """Extract components from a filter condition."""
    parts = filter_condition.split()
    alias_col = parts[0].split('.')
    alias = alias_col[0]
    column_name = alias_col[1]
    operator = parts[1]
    filter_value = ' '.join(parts[2:])  # Handle multi-word values
    
    return alias, column_name, operator, filter_value

def resolve_table_name(alias, aliases_dict):
    """Resolve table name from alias."""
    return aliases_dict.get(alias)

if __name__ == "__main__":
    QUERY_DIRECTORY = '../../workloads/imdb_pg_dataset/job/'
    OUTPUT_DIR = Path("../../experiments/experiment4/4.1/added_index")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    MAX_QUERIES = None  # Set to None to process all queries
    
    process_queries(QUERY_DIRECTORY, OUTPUT_DIR, MAX_QUERIES)