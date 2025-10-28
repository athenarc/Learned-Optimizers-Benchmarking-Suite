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
    db_name='imdbload'
)

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

def process_queries(query_directory):
    """Main function to process SQL queries from a directory with progress tracking."""
    try:
        queries = read_queries_from_directory(query_directory)
        print(f"Read {len(queries)} queries from {query_directory}")
        
        # Wrap the zip with tqdm for progress tracking
        for count, (query, file_location, query_name) in enumerate(tqdm(queries, 
                                                        total=len(queries),
                                                        desc="Processing queries")):
            print(f"\nProcessing query {count}: {query_name}")  # Keep this if you want per-query output
            process_single_query(query, query_name, file_location, query_directory)

    except Exception as e:
        print(f"Error processing queries: {str(e)}")

def process_single_query(query, query_name, file_location: Path, base_directory: str):
    try:
        file_location = Path(file_location)
        base_directory = Path(base_directory)
        with engine.connect() as conn:
            if conn is None or conn.closed:
                print("Error: Database connection not available")
                return {}

            qep = execute_explain_analyze(engine, query)
            if qep:
                qep_path = base_directory / file_location.parent / "classic_qep.json"
                print(f"Saving QEP to {qep_path}")
                save_qep_to_file(qep, qep_path)

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
    from pathlib import Path
    REPO_ROOT = Path(__file__).resolve()
    while REPO_ROOT.name != "Learned-Optimizers-Benchmarking-Suite" and REPO_ROOT.parent != REPO_ROOT:
        REPO_ROOT = REPO_ROOT.parent
    BASE_EXPERIMENT_DIR = REPO_ROOT / 'experiments' / 'experiment5'
    BASE_EXPERIMENT_DIR.mkdir(parents=True, exist_ok=True)
    # Dimension 3: Selectivity Generalization    
    QUERY_DIRECTORY_SELECTIVITY = BASE_EXPERIMENT_DIR / '5.1' / 'selectivity_generalization' / 'test'
    process_queries(QUERY_DIRECTORY_SELECTIVITY)