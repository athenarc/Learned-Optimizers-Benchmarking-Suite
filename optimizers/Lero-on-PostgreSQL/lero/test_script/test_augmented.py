import argparse

from utils import *
import glob
import os
import random

def ensure_lero_directory(sql_file_path):
    """Ensure a LERO directory exists for the SQL file"""
    dir_path = os.path.dirname(sql_file_path)
    lero_dir = os.path.join(dir_path, "LERO")
    os.makedirs(lero_dir, exist_ok=True)
    return lero_dir

def save_plan(plan, output_path):
    """Save execution plan to a file"""
    with open(output_path, 'w') as f:
        json.dump(plan, f, indent=2)

def find_sql_files(directory):
    """Recursively find all .sql files in a directory"""
    sql_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.sql'):
                full_path = os.path.join(root, file)
                sql_files.append(full_path)
    return sql_files

# def lero_start_server():
#     # Change to the previous directory and run server.py
#     os.chdir("..")
#     os.system("python3 server.py > server.log 2>&1 &")
#     return os.path.abspath("server.log")

def lero_execute_query(query, query_path, run_args, lero_dir):
    """Execute a query with LERO and save its plan"""
    query_name = os.path.splitext(os.path.basename(query_path))[0]
    
    try:
        # Get the execution plan with LERO
        plan_json = explain_analyze_query(query, run_args)
        
        # Save the full plan
        plan_path = os.path.join(lero_dir, f"{query_name}_lero_plan.json")
        save_plan(plan_json, plan_path)
        print(f"Saved LERO plan to {plan_path}")
        
        return True
    except Exception as e:
        print(f"Error processing query {query_name}: {str(e)}")
        return False

# python test.py --query_path ../reproduce/test_query/stats.txt --output_query_latency_file stats.test
if __name__ == "__main__":
    parser = argparse.ArgumentParser("Model training helper")
    parser.add_argument("--query_path",
                        metavar="PATH",
                        help="Load the queries")
    parser.add_argument("--output_query_latency_file", metavar="PATH")

    args = parser.parse_args()
    test_queries = []
    sql_files = find_sql_files(args.query_path)

    # Load the queries
    test_queries = []
    for fp in sql_files:
        with open(fp) as f:
            query = f.read()
        test_queries.append((fp, query))
    print("Found", len(test_queries), "SQL files to execute.")

    # print("Starting LERO server...")
    # lero_start_server()

    # LERO configuration
    lero_run_args = [
        "SET enable_lero TO True",
        f"SET lero_server_host TO '{LERO_SERVER_HOST}'",
        f"SET lero_server_port TO {LERO_SERVER_PORT}"
    ]

    # Process each query
    success_count = 0
    for query_name, query in test_queries:
        print(f"\nProcessing query: {query_name}")
        queryId = os.path.splitext(os.path.basename(query_name))[0]
        print(f"Query ID: {queryId}")
        print(f"Executing query with LERO...")
        # Ensure the SQL file path is valid
        query_path = os.path.join(args.query_path, query_name)
        print(f"Query path: {query_path}")
        if not os.path.exists(query_path):
            print(f"Query file {query_path} does not exist.")
            continue
        # Create LERO directory for this query
        lero_dir = ensure_lero_directory(query_path)
        
        # Execute with LERO and save plans
        if lero_execute_query(query, query_name, lero_run_args, lero_dir):
            success_count += 1
    
    print(f"\nCompleted {success_count}/{len(test_queries)} queries successfully")
