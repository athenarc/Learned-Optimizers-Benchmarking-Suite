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

import time
import json
from sqlalchemy import text

def execute_explain_analyze(conn, query: str, dbname = None) -> dict:
    db = dbname if dbname else DB_CONFIG['dbname']
    conn = psycopg2.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        dbname=db,
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

def save_qep_to_file(qep_data: dict, output_path: Path):
    """Save the QEP data to a JSON file"""
    try:
        with open(output_path, 'w') as f:
            json.dump(qep_data, f, indent=2)
        print(f"Saved QEP to {output_path}")
    except Exception as e:
        print(f"Error saving QEP to file: {str(e)}")

import re
RUNS = 3

def process_queries(query_directory, train_output_dir: Path, test_output_dir: Path, dbname=None): 
    """Main function to process SQL queries from a directory."""
    try:
        queries = read_queries_from_directory(query_directory)  # <-- returns list of tuples
        print(f"Read {len(queries)} queries from {query_directory}")
        run = 0
        
        while run < RUNS:
            for count, (query, file_location, query_name) in enumerate(queries):
                print(f"\nProcessing query {count}: {query_name}")
                query_name = query_name + ".sql"
                process_single_query(run, query, query_name, test_output_dir, dbname)

                # Save query to train_output_dir
                query_path  = train_output_dir / query_name
                with open(query_path, 'w', encoding='utf-8') as f:
                    f.write(query)
            run += 1

    except Exception as e:
        print(f"Error processing queries: {str(e)}")

def process_single_query(runID, query, query_name, output_dir: Path, dbname=None):
    """Process a single SQL query."""
    try:
        conn = connect_to_db()
        if not conn:
            print("Failed to connect to database")
            return
                
        qep_data = execute_explain_analyze(conn, query, dbname)
        if not qep_data:
            print(f"Failed to get QEP for query {query_name}")
            return
        queryId = query_name.split('.')[0]  # Extract the query ID from the filename
        # Save the QEP data to a JSON file
        qep_output_path = output_dir / f"run{runID + 1}" /queryId / f"classic_qep.json"
        qep_output_path.parent.mkdir(parents=True, exist_ok=True)
        save_qep_to_file(qep_data, qep_output_path)
        query_output_path = output_dir / f"run{runID + 1}" / queryId / f"{query_name}"
        query_output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(query_output_path, 'w') as f:
            f.write(query)
        print(f"Processed query {query_name} and saved QEP to {qep_output_path}")

        conn.close()

    except Exception as e:
        print(f"Error processing query {query_name}: {str(e)}")

if __name__ == "__main__":
    # QUERY_DIRECTORY = '../../workloads/imdb_pg_dataset/job/'
    # TRAIN_OUTPUT_DIR = Path("../../experiments/experiment1/job/train/")
    # TEST_OUTPUT_DIR = Path("../../experiments/experiment1/job/test/")
    # TRAIN_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    # TEST_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # process_queries(QUERY_DIRECTORY, TRAIN_OUTPUT_DIR, TEST_OUTPUT_DIR, dbname='imdbload')
    
    QUERY_DIRECTORY = '../../workloads/tpcds/'
    TRAIN_OUTPUT_DIR = Path("../../experiments/experiment1/tpcds/train/")
    TEST_OUTPUT_DIR = Path("../../experiments/experiment1/tpcds/test/")
    TRAIN_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    TEST_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    process_queries(QUERY_DIRECTORY, TRAIN_OUTPUT_DIR, TEST_OUTPUT_DIR, dbname='tpcds')

    # QUERY_DIRECTORY = '../../workloads/tpcds/'
    # TRAIN_OUTPUT_DIR = Path("../../experiments/experiment1/tpcds/train/")
    # TEST_OUTPUT_DIR = Path("../../experiments/experiment1/tpcds/test/")
    # TRAIN_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    # TEST_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        
    # process_queries(QUERY_DIRECTORY, TRAIN_OUTPUT_DIR, TEST_OUTPUT_DIR, dbname='tpcds')