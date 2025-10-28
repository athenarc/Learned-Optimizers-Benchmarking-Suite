import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import query_directory, TABLES, DB_CONFIG, connect_to_db, get_alchemy_engine
import pandas as pd
from tqdm import tqdm
from sql_parser import parse_sql
from file_utils import read_queries_from_directory
from selectivity import get_database_schema, DatabaseCache
from typing import Dict, Any
from selectivity import fetch_column_data, estimate_selectivity
from pathlib import Path
import psycopg2
import random
from sqlalchemy.pool import QueuePool

# At module level (only create once)
engine = get_alchemy_engine(
    pool_size=5, 
    max_overflow=10,
    pool_recycle=3600
)

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

import time
import json
from sqlalchemy import text

def execute_explain_analyze(engine, query: str) -> dict:
    """Execute EXPLAIN ANALYZE on the given query and return the QEP data"""
    try:
        with engine.connect() as conn:
            conn.execute(text("SET max_parallel_workers_per_gather = 0;"))
            explain_query = f"EXPLAIN (ANALYZE, FORMAT JSON) {query}"
            result = conn.execute(text(explain_query))
            qep_data = result.fetchall()
            return qep_data[0][0]
    except Exception as e:
        print(f"Error executing EXPLAIN ANALYZE: {str(e)}")
        return {}

import orjson  # Faster than json

def save_qep_to_file(qep_data: dict, output_path: Path):
    """Save the QEP data to a JSON file"""
    try:
        with open(output_path, 'wb') as f:  # Note 'wb' for binary mode
            f.write(orjson.dumps(qep_data, option=orjson.OPT_INDENT_2))
    except Exception as e:
        print(f"Error saving QEP to file: {str(e)}")

import re
RUNS = 1

def process_queries_selectivity_generalization(query_directory, output_dir: Path):
    """Process up to 25 SQL queries from different join levels with production_year filter."""

    try:
        queries, query_names = read_queries_from_directory(query_directory)
        print(f"Read {len(queries)} queries from {query_directory}")

        join_level_groups = {}
        valid_queries = []

        # Group queries by join level
        for query, query_name in zip(queries, query_names):
            parsed_query = parse_sql(query, split_parentheses=True)
            num_joins = len(parsed_query['joins'])
            filters = parsed_query['filters']

            if find_production_year_filter(filters, query_name):
                if num_joins not in join_level_groups:
                    join_level_groups[num_joins] = []
                join_level_groups[num_joins].append((query, query_name))

        print(f"Found join levels: {list(join_level_groups.keys())}")

        # Flatten to at most 25 queries spread across join levels
        selected_queries = []
        join_levels = sorted(join_level_groups.keys())

        while len(selected_queries) < 25 and join_levels:
            for level in join_levels:
                if join_level_groups[level]:
                    selected_queries.append(join_level_groups[level].pop(0))
                    if len(selected_queries) >= 25:
                        break
                if not join_level_groups[level]:
                    join_levels.remove(level)

        print(f"Selected {len(selected_queries)} queries for processing")

        for count, (query, query_name) in enumerate(tqdm(selected_queries, desc="Processing selected queries")):
            print(f"\nProcessing query {count + 1}: {query_name}")
            process_single_query_selectivity_generalization(query, query_name, output_dir)

    except Exception as e:
        print(f"Error processing queries: {str(e)}")

def process_single_query_selectivity_generalization(query, query_name, output_dir: Path):
    """Generate query variants with uniform selectivity across all filters, splitting into train/test sets."""
    try:
        with engine.connect() as conn:
            if conn is None or conn.closed:
                print("Error: Database connection not available")
                return {}
            # Clean and parse query
            original_query = re.sub(r'\s+', ' ', query).strip()
            parsed_query = parse_sql(original_query, split_parentheses=True)
            num_joins = len(parsed_query['joins'])
            filters = parsed_query['filters']
            aliases = parsed_query['aliases']
            query_id = query_name.split('.')[0]

            print(f"\nProcessing {query_name} with {num_joins} joins")
            print(f"Original query: {original_query}")

            # Skip if no filters
            if not filters:
                print("No filters found - skipping")
                return {}

            # Create directory structure
            train_dir = output_dir / "train"
            os.makedirs(train_dir, exist_ok=True)
            test_dir = output_dir / "test"
            os.makedirs(test_dir, exist_ok=True)

            filter_condition = find_production_year_filter(filters, query_name)
            if not filter_condition:
                print(f"Skipping query {query_name} - no valid production_year filter")
                return {}
            alias, column_name, operator, old_value = extract_filter_components(filter_condition)
            table_name = resolve_table_name(alias, aliases)
            filter_data = {}
            
            if not table_name:
                print(f"Processing filter: {alias}.{column_name} {operator} {old_value}")
                print(f"Couldn't resolve table for alias {alias}")

            dist_query = f"""
            WITH valid_values AS (
                SELECT {column_name} FROM {table_name}
                WHERE {column_name} IS NOT NULL
            ),
            percentiles AS (
                SELECT 
                    percentile_disc(0.01) WITHIN GROUP (ORDER BY {column_name}) AS p01,
                    percentile_disc(0.10) WITHIN GROUP (ORDER BY {column_name}) AS p10,
                    percentile_disc(0.20) WITHIN GROUP (ORDER BY {column_name}) AS p20,
                    percentile_disc(0.60) WITHIN GROUP (ORDER BY {column_name}) AS p60,
                    percentile_disc(0.80) WITHIN GROUP (ORDER BY {column_name}) AS p80,
                    percentile_disc(0.99) WITHIN GROUP (ORDER BY {column_name}) AS p99
                FROM valid_values
            )
            SELECT p01 AS "1", p10 AS "10", p20 AS "20",
                p60 AS "60", p80 AS "80", p99 AS "99"
            FROM percentiles;
            """            

            try:
                dist_df = pd.read_sql(dist_query, conn)
                filter_data[filter_condition] = {
                    'type': 'numeric',
                    'components': (alias, column_name, operator, old_value),
                    'values': {
                        '0%': dist_df.iloc[0]['1'],
                        '10%': dist_df.iloc[0]['10'],
                        '20%': dist_df.iloc[0]['20'],
                        '60%': dist_df.iloc[0]['60'],
                        '80%': dist_df.iloc[0]['80'],
                        '100%': dist_df.iloc[0]['99']
                    }
                }
            except Exception as e:
                print(f"Error getting percentiles for {table_name}.{column_name}: {e}")
                
            if not filter_data:
                print("No valid filters found - skipping")
                return {}
                
            # Process each selectivity level
            results = {'train': [], 'test': []}
            
            for selectivity in ['0%', '10%', '20%', '60%', '80%', '100%']:
                # Apply uniform selectivity to all filters
                for filter_cond, data in filter_data.items():
                    value = data['values'][selectivity]
                    col_name = data['components'][1]
                    old_op = data['components'][2]
                    old_val = data['components'][3]
                    
                    # Build replacement based on column type and original operator
                    if data['type'] == 'numeric':
                        # Numeric column handling
                        if not isinstance(value, str):
                            new_cond = f"{col_name} < {value}"
                        else:
                            escaped_val = value.replace("'", "''")
                            new_cond = f"{col_name} < '{escaped_val}'"
                    
                    # Replace in query
                    modified_query = original_query.replace(
                        f"{col_name} {old_op} {old_val}",
                        new_cond
                    )
                    
                # Determine output paths
                suffix = {'0%':'0', '10%':'10', '20%':'20', 
                        '60%':'60', '80%':'80', '100%':'100'}[selectivity]
                
                if selectivity in ['0%', '10%', '20%']:  # Train set
                    os.makedirs(train_dir, exist_ok=True)
                    output_path = train_dir / f"{query_id}_{suffix}.sql"
                    
                    with open(output_path, 'w') as f:
                        f.write(modified_query)
                    results['train'].append(str(output_path))
                    
                else:  # Test set (execute 3 times)
                    for run in range(1, 4):
                        os.makedirs(test_dir / f"run{run}" / query_id / selectivity, exist_ok=True)
                        output_path = test_dir / f"run{run}" / query_id / selectivity / f"{query_id}_{suffix}.sql"
                        with open(output_path, 'w') as f:
                            f.write(modified_query)

                        qep = execute_explain_analyze(engine, modified_query)
                        if qep:
                            qep_path = test_dir / f"run{run}" / query_id / selectivity / f"classic_qep.json"
                            save_qep_to_file(qep, qep_path)
                            results['test'].append(str(qep_path))
        return results

    except Exception as e:
        print(f"Error processing {query_name}: {str(e)}")
        if 'conn' in locals() and conn:
            conn.close()
        return {'train': [], 'test': []}

def find_production_year_filter(filters, query_name):
    """Find and validate production_year filter condition."""
    for filter_cond in filters:
        if 't.production_year' in filter_cond:
            if any(op in filter_cond for op in ('BETWEEN')):
                print(f"Skipping query {query_name} - uses 'BETWEEN'")
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
    # Dimension 3: Selectivity Generalization
    QUERY_DIRECTORY_SELECTIVITY = '../../workloads/imdb_pg_dataset/job_synthetic/'
    OUTPUT_DIR_SELECTIVITY = Path("../../experiments/experiment5/5.1/selectivity_generalization_alt/")
    OUTPUT_DIR_SELECTIVITY.mkdir(parents=True, exist_ok=True)
    
    process_queries_selectivity_generalization(QUERY_DIRECTORY_SELECTIVITY, OUTPUT_DIR_SELECTIVITY)