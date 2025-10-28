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
import time
import json

def execute_explain_analyze(conn, query: str, join_method: str = None) -> dict:
    conn = psycopg2.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        dbname=DB_CONFIG['dbname'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password']
    )
    """Execute EXPLAIN ANALYZE on the given query and return the QEP data"""
    try:

        cursor = conn.cursor()
        # Apply join method settings if specified
        if join_method:
            join_method = join_method.lower()
            if join_method == 'nestloop':
                cursor.execute("SET enable_hashjoin = off")
                cursor.execute("SET enable_mergejoin = off")
                cursor.execute("SET enable_nestloop = on")
            elif join_method == 'hashjoin':
                cursor.execute("SET enable_nestloop = off")
                cursor.execute("SET enable_mergejoin = off")
                cursor.execute("SET enable_hashjoin = on")
            elif join_method == 'mergejoin':
                cursor.execute("SET enable_nestloop = off")
                cursor.execute("SET enable_hashjoin = off")
                cursor.execute("SET enable_mergejoin = on")

        explain_query = f"EXPLAIN (ANALYZE, FORMAT JSON) {query}"                        
        cursor.execute(explain_query)
        qep_data = cursor.fetchall()

        # Reset to default settings
        if join_method:
            cursor.execute("SET enable_nestloop = on")
            cursor.execute("SET enable_hashjoin = on")
            cursor.execute("SET enable_mergejoin = on")
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

def process_generated_queries(conn, queries: dict):
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
            qep_data = execute_explain_analyze(conn, query)
            if not qep_data:
                continue
            
            # Save to classic_qep.json in the same directory
            output_path = Path(query_path).parent / "classic_qep.json"
            save_qep_to_file(qep_data, output_path)
            
        except Exception as e:
            print(f"Error processing {query_path}: {str(e)}")

import re

if __name__ == "__main__":
    from pathlib import Path
    REPO_ROOT = Path(__file__).resolve()
    while REPO_ROOT.name != "Learned-Optimizers-Benchmarking-Suite" and REPO_ROOT.parent != REPO_ROOT:
        REPO_ROOT = REPO_ROOT.parent    
    BASE_DIR = REPO_ROOT
    query_directory = BASE_DIR / "workloads/imdb_pg_dataset/job/"
    queries, queryNames = read_queries_from_directory(query_directory)
    join_method = "mergejoin"  # Example join method, can be changed to "nestloop" or "mergejoin"
    print("Connecting to the database...")
    conn = connect_to_db()
    if conn:
        for query, queryName in zip(queries, queryNames):
            parsed_query = parse_sql(query)
            num_joins = len(parsed_query['joins'])
            parent_dir = f'{num_joins}_joins'
            queryId = queryName.split(".")[0]
            query_dir = BASE_DIR / f"workloads/imdb_pg_dataset/experiment4/4.3/{join_method}" / parent_dir / queryId
            os.makedirs(query_dir, exist_ok=True)
            # Move the query to the new directory
            query_path = Path(query_dir) / queryName
            with open(query_path, 'w') as f:
                f.write(query)

            # For every query execute EXPLAIN ANALYZE
            qep_data = execute_explain_analyze(conn, query, join_method)
            if not qep_data:
                continue
            # Save to classic_qep.json in the same directory
            output_path = Path(query_dir) / "classic_qep.json"
            save_qep_to_file(qep_data, output_path)
        conn.close()
    else:
        print("Failed to connect to database")