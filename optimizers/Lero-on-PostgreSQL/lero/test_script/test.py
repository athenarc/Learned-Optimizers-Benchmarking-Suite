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

def lero_start_server():
    # Change to the previous directory and run server.py
    os.chdir("..")
    os.system("python3 server.py > server.log 2>&1 &")
    return os.path.abspath("server.log")

# python test.py --query_path ../reproduce/test_query/stats.txt --output_query_latency_file stats.test
if __name__ == "__main__":
    parser = argparse.ArgumentParser("Model training helper")
    parser.add_argument("--query_path",
                        metavar="PATH",
                        help="Load the queries")
    parser.add_argument("--output_query_latency_file", metavar="PATH")

    args = parser.parse_args()
    test_queries = []
    test_queries = find_sql_files(args.query_path)

    for (fp, q) in test_queries:
        do_run_query(q, fp, ["SET enable_lero TO True", f"SET lero_server_host TO '{LERO_SERVER_HOST}'", f"SET lero_server_port TO {LERO_SERVER_PORT}"], args.output_query_latency_file, True, None, None)
