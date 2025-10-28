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
    query_directory = "../../workloads/imdb_pg_dataset/job/"
    output_dir = Path("../../experiments/experiment4/4.4/")
    output_dir.mkdir(parents=True, exist_ok=True)    
    queries, queryNames = read_queries_from_directory(query_directory)
    RUNS = 3
    print("Connecting to the database...")
    conn = connect_to_db()
    count = 0
    if conn:
        while count < RUNS:
            for query, queryName in zip(queries, queryNames):
                parsed_query = parse_sql(query)
                queryId = queryName.split(".")[0]
                runId = f"run{count+1}"
                # Create a directory for the query based on its ID and run ID
                query_dir = output_dir / runId /queryId
                os.makedirs(query_dir, exist_ok=True)
                # Move the query to the new directory
                query_path = Path(query_dir) / queryName
                with open(query_path, 'w') as f:
                    f.write(query)

                # For every query execute EXPLAIN ANALYZE
                qep_data = execute_explain_analyze(conn, query)
                if not qep_data:
                    continue
                # Save to classic_qep.json in the same directory
                output_path = Path(query_dir) / "classic_qep.json"
                save_qep_to_file(qep_data, output_path)
            count += 1
            print(f"Run {count} completed")
        conn.close()
    else:
        print("Failed to connect to database")