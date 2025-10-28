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

RANDOM_SPLIT_1__TEST_QUERIES = [
    '1c.sql', '2c.sql', '4b.sql', '4c.sql', '5c.sql', '6a.sql', '6c.sql', '6e.sql', '8b.sql', '8c.sql', '9c.sql',
    '11d.sql', '15a.sql', '17b.sql', '17e.sql', '18b.sql',
    '20a.sql', '21a.sql', '25c.sql', '28b.sql',
    '32b.sql', '33a.sql'
]
    
RANDOM_SPLIT_2__TEST_QUERIES = [
    '1a.sql', '4c.sql', '5c.sql', '6c.sql', '6d.sql', '7b.sql', '8c.sql',
    '10a.sql', '11a.sql', '11d.sql', '13c.sql', '13d.sql', '15d.sql', '16a.sql', '17b.sql', '19a.sql',
    '20a.sql', '22b.sql', '25b.sql', '29b.sql',
    '31a.sql', '32b.sql'
]
    
RANDOM_SPLIT_3__TEST_QUERIES = [
    '2a.sql', '3b.sql', '6d.sql', '9b.sql',
    '10b.sql', '11b.sql', '11c.sql', '13c.sql', '13d.sql', '16b.sql', '18c.sql', '19c.sql',
    '21c.sql', '22a.sql', '22d.sql', '26a.sql', '26b.sql', '27c.sql', '28a.sql', '28c.sql',
    '30a.sql', '33c.sql'
]

BASE_QUERY_SPLIT_1__TEST_QUERIES = [
    '2a.sql', '2b.sql', '2c.sql', '2d.sql',
    '7a.sql', '7b.sql', '7c.sql',
    '15a.sql', '15b.sql', '15c.sql', '15d.sql',
    '24a.sql', '24b.sql', 
    '25a.sql', '25b.sql', '25c.sql',
    '31a.sql', '31b.sql', '31c.sql',
]

BASE_QUERY_SPLIT_2__TEST_QUERIES = [
    '13a.sql', '13b.sql', '13c.sql', '13d.sql',
    '15a.sql', '15b.sql', '15c.sql', '15d.sql',
    '20a.sql', '20b.sql', '20c.sql',
    '26a.sql', '26b.sql', '26c.sql',
    '29a.sql', '29b.sql', '29c.sql',
    '30a.sql', '30b.sql', '30c.sql',
    '33a.sql', '33b.sql', '33c.sql'
]

BASE_QUERY_SPLIT_3__TEST_QUERIES = [
    '1a.sql', '1b.sql', '1c.sql', '1d.sql',
    '5a.sql', '5b.sql', '5c.sql',
    '12a.sql', '12b.sql', '12c.sql',
    '17a.sql', '17b.sql', '17c.sql', '17d.sql', '17e.sql', '17f.sql',
    '22a.sql', '22b.sql', '22c.sql', '22d.sql',
    '27a.sql', '27b.sql', '27c.sql',
    '28a.sql', '28b.sql', '28c.sql'
]

LEAVE_ONE_OUT_SPLIT_1__TEST_QUERIES = [
    '1c.sql', '2a.sql', '3b.sql', '4a.sql', '5a.sql', '6b.sql', '7c.sql', '8c.sql', '9c.sql',
    '10b.sql', '11b.sql', '12c.sql', '13b.sql', '14a.sql', '15b.sql', '16c.sql', '17c.sql', '18b.sql', '19a.sql',
    '20c.sql', '21c.sql', '22b.sql', '23b.sql', '24a.sql', '25a.sql', '26c.sql', '27c.sql', '28a.sql', '29b.sql',
    '30a.sql', '31b.sql', '32b.sql', '33c.sql'
]

LEAVE_ONE_OUT_SPLIT_2__TEST_QUERIES = [
    '1d.sql', '2d.sql', '3a.sql', '4b.sql', '5c.sql', '6d.sql', '7a.sql', '8c.sql', '9c.sql',
    '10a.sql', '11a.sql', '12a.sql', '13d.sql', '14b.sql', '15b.sql', '16a.sql', '17f.sql', '18a.sql', '19d.sql',
    '20a.sql', '21b.sql', '22c.sql', '23b.sql', '24b.sql', '25a.sql', '26a.sql', '27b.sql', '28c.sql', '29a.sql',
    '30b.sql', '31a.sql', '32b.sql', '33b.sql'
]

LEAVE_ONE_OUT_SPLIT_3__TEST_QUERIES = [
    '1c.sql', '2d.sql', '3b.sql', '4a.sql', '5c.sql', '6d.sql', '7b.sql', '8a.sql', '9a.sql',
    '10c.sql', '11d.sql', '12a.sql', '13a.sql', '14b.sql', '15a.sql', '16d.sql', '17b.sql', '18b.sql', '19d.sql',
    '20b.sql', '21a.sql', '22a.sql', '23b.sql', '24a.sql', '25b.sql', '26a.sql', '27a.sql', '28b.sql', '29c.sql',
    '30a.sql', '31a.sql', '32a.sql', '33c.sql'
]

SPLIT_CONFIGS = {
    'random': {
        1: RANDOM_SPLIT_1__TEST_QUERIES,
        2: RANDOM_SPLIT_2__TEST_QUERIES,
        3: RANDOM_SPLIT_3__TEST_QUERIES
    },
    'base_query': {
        1: BASE_QUERY_SPLIT_1__TEST_QUERIES,
        2: BASE_QUERY_SPLIT_2__TEST_QUERIES,
        3: BASE_QUERY_SPLIT_3__TEST_QUERIES
    },
    'leave_one_out': {
        1: LEAVE_ONE_OUT_SPLIT_1__TEST_QUERIES,
        2: LEAVE_ONE_OUT_SPLIT_2__TEST_QUERIES,
        3: LEAVE_ONE_OUT_SPLIT_3__TEST_QUERIES
    }
}

def extract_execution_time(qep_data: dict) -> float:
    """Extract the execution time from QEP data in milliseconds"""
    if not qep_data or not isinstance(qep_data, list):
        return float('inf')
    
    plan = qep_data[0].get('Plan', {})
    return plan.get('Actual Total Time', float('inf'))

def write_join_complexity_order_file(queries, query_names, test_queries, output_dir):
    """Creates a file listing training queries ordered by join complexity (least to most joins)"""
    join_counts = []

    for query, query_name in zip(queries, query_names):
        if query_name in test_queries:
            continue
        try:
            parsed = parse_sql(query)
            num_joins = len(parsed.get('joins', []))
            join_counts.append((query_name, num_joins))
        except Exception as e:
            print(f"Failed to parse {query_name}: {e}")
            continue

    join_counts.sort(key=lambda x: x[1])

    join_complexity_file = output_dir / "ascending_join_complexity_query_order.txt"
    with open(join_complexity_file, 'w') as f:
        for query_name, _ in join_counts:
            f.write(f"{query_name}\n")
    
    print(f"Created join complexity query list at {join_complexity_file}")

def process_training_queries(conn, queries, query_names, split_type, split_num, base_output_dir):
    """Process training queries for a split and create sorted latency file"""
    train_dir = Path(base_output_dir) / "train" / split_type / "queries"
    test_queries = SPLIT_CONFIGS[split_type][split_num]
    
    # Collect all training queries (non-test queries)
    training_query_data = []
    for query, query_name in zip(queries, query_names):
        if query_name not in test_queries:
            training_query_data.append((query_name, query))
    
    # Execute and time all training queries
    query_times = []
    for query_name, query in training_query_data:
        qep_data = execute_explain_analyze(conn, query)
        if not qep_data:
            continue
            
        exec_time = extract_execution_time(qep_data)
        query_times.append((query_name, exec_time))
    
    # Sort by execution time (fastest first)
    query_times.sort(key=lambda x: x[1])
    
    # Create ascending_latency_query_order.txt
    latency_file = train_dir / "ascending_latency_query_order.txt"
    with open(latency_file, 'w') as f:
        for query_name, _ in query_times:
            f.write(f"{query_name}\n")
    
    print(f"Created sorted training query list at {latency_file}")

    # Write join complexity order file
    write_join_complexity_order_file(queries, query_names, test_queries, train_dir)

def process_split(conn, queries, query_names, split_type, split_num, base_output_dir, RUNS = 3):
    """Process a single split of queries"""
    train_dir = Path(base_output_dir) / "train" / split_type / "queries"
    os.makedirs(train_dir, exist_ok=True)
    
    test_queries = SPLIT_CONFIGS[split_type][split_num]
    
    # Process test queries
    count = 0
    while count < RUNS:
        for query, query_name in zip(queries, query_names):
            if not query:
                print(f"Skipping empty query in {query_name}")
                continue
                
            if query_name not in test_queries:
                # Add to train directory
                query_path = train_dir / query_name
                with open(query_path, 'w') as f:
                    f.write(query)
            else:
                # Add to test directory and get QEP
                queryId = query_name.split(".")[0]
                runId = f"run{count+1}"
                test_dir = Path(base_output_dir) / "test" / split_type / runId / queryId
                os.makedirs(test_dir, exist_ok=True)
                
                query_path = test_dir / query_name
                with open(query_path, 'w') as f:
                    f.write(query)
                    
                qep_data = execute_explain_analyze(conn, query)
                if not qep_data:
                    continue
                    
                output_path = test_dir / "classic_qep.json"
                save_qep_to_file(qep_data, output_path)

        count += 1
    
    # Process training queries and create sorted latency file
    process_training_queries(conn, queries, query_names, split_type, split_num, base_output_dir)

def execute_explain_analyze(conn, query: str) -> dict:
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
        explain_query = f"EXPLAIN (ANALYZE, FORMAT JSON) {query}"
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

if __name__ == "__main__":
    query_directory = "../../workloads/imdb_pg_dataset/job/"
    base_output_dir = "../../experiments/experiment2/"
    RUNS = 3
    
    queries, query_names = read_queries_from_directory(query_directory)
    print("Connecting to the database...")
    conn = connect_to_db()
    
    if conn:
        # Process only one split (e.g., split_num = 1) for each split type
        split_num = 1  # Change this if you want a different split
        for split_type in ['random', 'base_query', 'leave_one_out']:
            print(f"Processing {split_type} split {split_num}")
            process_split(conn, queries, query_names, split_type, split_num, base_output_dir, RUNS)
        
        conn.close()