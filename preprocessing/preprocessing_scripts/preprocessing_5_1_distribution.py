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

import time
import json
from sqlalchemy import text

def execute_explain_analyze(engine, query: str) -> dict:
    """Execute EXPLAIN ANALYZE on the given query and return the QEP data"""
    try:
        with engine.connect() as conn:
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

def process_queries(query_directory, output_dir: Path):
    """Main function to process SQL queries from a directory."""
    queries, query_names = read_queries_from_directory(query_directory)
    print(f"Read {len(queries)} queries from {query_directory}")
    
    # Outer progress bar for runs
    for run in tqdm(range(RUNS), desc="Total runs", position=0):
        run_dir = output_dir / f"run{run + 1}" if RUNS > 1 else output_dir        
        # Inner progress bar for queries
        for count, (query, query_name) in enumerate(
            tqdm(zip(queries, query_names), 
            desc=f"Run {run + 1}/{RUNS}", 
            position=1, 
            leave=False,
            total=len(queries))
        ):
            query_id = query_name.split('.')[0]
            qep_output_path = run_dir / query_id / "classic_qep.json"

            # Skip if this query has already been processed in this run
            if qep_output_path.exists():
                print(f"Skipping already processed query {query_name} in run {run + 1}")
                continue

            process_single_query(run, query, query_name, output_dir)

def process_single_query(runID, query, query_name, output_dir: Path):
    """Process a single SQL query."""
    try:
        qep_data = execute_explain_analyze(engine, query)
        if not qep_data:
            print(f"Failed to get QEP for query {query_name}")
            return
        queryId = query_name.split('.')[0]  # Extract the query ID from the filename

        # Save the QEP data to a JSON file
        if RUNS > 1:
            output_dir = output_dir / f"run{runID + 1}"
        qep_output_path = output_dir /queryId / f"classic_qep.json"
        qep_output_path.parent.mkdir(parents=True, exist_ok=True)

        # Skip if already processed (extra check)
        if qep_output_path.exists():
            print(f"Skipping already processed query {query_name}")
            return

        save_qep_to_file(qep_data, qep_output_path)
        query_output_path = output_dir / queryId / f"{query_name}"
        query_output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(query_output_path, 'w') as f:
            f.write(query)
        print(f"Processed query {query_name} and saved QEP to {qep_output_path}")

    except Exception as e:
        print(f"Error processing query {query_name}: {str(e)}")

def process_queries_complexity_generelization(query_directory, output_dir: Path):
    """Main function to process SQL queries from a directory."""
    try:
        queries, query_names = read_queries_from_directory(query_directory)
        print(f"Read {len(queries)} queries from {query_directory}")
        for count, (query, query_name) in enumerate(zip(queries, query_names)):
            print(f"\nProcessing query {count}: {query_name}")
            process_single_query_complexity_generalization(query, query_name, output_dir)

    except Exception as e:
        print(f"Error processing queries: {str(e)}")

def process_single_query_complexity_generalization(query, query_name, output_dir: Path):
    """Process a single SQL query."""
    try:
        parsed_query = parse_sql(query)
        num_joins = len(parsed_query['joins'])
        filters = parsed_query['filters']
        aliases = parsed_query['aliases']
        train_path = output_dir / "train"
        test_path = output_dir / "test"
        train_path.mkdir(parents=True, exist_ok=True)
        test_path.mkdir(parents=True, exist_ok=True)

        # If the query has less than 10 joins, we add it to the training set
        # Otherwise, it is executed and added to the test set
        if num_joins < 10:
            output_path = train_path / query_name
            print(f"Adding query {query_name} to training set with {num_joins} joins")
            with open(output_path, 'w') as f:
                f.write(query)
        else:
            # conn = connect_to_db()
            # if not conn:
            #     print("Failed to connect to database")
            #     return
            
            runID = 0
            while runID < RUNS:
                qep_data = execute_explain_analyze(engine, query)
                if not qep_data:
                    print(f"Failed to get QEP for query {query_name}")
                    return
                queryId = query_name.split('.')[0]  # Extract the query ID from the filename
                # Save the QEP data to a JSON file
                qep_output_path = test_path / f"run{runID + 1}" /queryId / f"classic_qep.json"
                qep_output_path.parent.mkdir(parents=True, exist_ok=True)
                save_qep_to_file(qep_data, qep_output_path)
                query_output_path = test_path / f"run{runID + 1}" / queryId / f"{query_name}"
                query_output_path.parent.mkdir(parents=True, exist_ok=True)
                with open(query_output_path, 'w') as f:
                    f.write(query)
                print(f"Processed query {query_name} and saved QEP to {qep_output_path}")
                runID += 1

    except Exception as e:
        print(f"Error processing query {query_name}: {str(e)}")

if __name__ == "__main__":
    from pathlib import Path
    REPO_ROOT = Path(__file__).resolve()
    while REPO_ROOT.name != "Learned-Optimizers-Benchmarking-Suite" and REPO_ROOT.parent != REPO_ROOT:
        REPO_ROOT = REPO_ROOT.parent
    # Dimension 1: Distribution Generalization

    IMDB_PG_DATASET_DIR = REPO_ROOT / 'workloads/imdb_pg_dataset'
    JOB_EXTENDED_DIR = 'job_extended'
    JOB_DYNAMIC_DIR = 'job_d'
    JOB_LIGHT_DIR = 'job_light'
    JOB_SYNTHETIC_DIR = 'job_synthetic'
    
    QUERY_DIRS = [
        (JOB_EXTENDED_DIR, "job_extended"),
        (JOB_DYNAMIC_DIR, "job_d"),
        (JOB_LIGHT_DIR, "job_light"),
        (JOB_SYNTHETIC_DIR, "job_synthetic")
    ]
    
    OUTPUT_BASE_DIR = Path("../../experiments/experiment5/5.1/distribution_generalization")
    OUTPUT_BASE_DIR.mkdir(parents=True, exist_ok=True)
    
    for query_dir, subdir in QUERY_DIRS:
        print(f"Processing queries from {query_dir}...")
        QUERY_DIRECTORY = IMDB_PG_DATASET_DIR / query_dir
        OUTPUT_DIR = OUTPUT_BASE_DIR / subdir
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        
        print(f"Processing queries from {QUERY_DIRECTORY} into {OUTPUT_DIR}")
        process_queries(QUERY_DIRECTORY, OUTPUT_DIR)
    
    # Dimension 2: Complexity Generalization
    # QUERY_DIRECTORY_COMPLEXITY = '../../workloads/imdb_pg_dataset/job/'
    # OUTPUT_DIR_COMPLEXITY = Path("../../experiments/experiment5/5.1/complexity_generalization/")
    # OUTPUT_DIR_COMPLEXITY.mkdir(parents=True, exist_ok=True)
    
    # process_queries_complexity_generelization(QUERY_DIRECTORY_COMPLEXITY, OUTPUT_DIR_COMPLEXITY)