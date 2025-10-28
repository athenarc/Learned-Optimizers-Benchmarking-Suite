import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import query_directory, TABLES, DB_CONFIG, connect_to_db, get_alchemy_engine
import json
from pathlib import Path
from tqdm import tqdm
from typing import List, Tuple, Dict

from dotenv import load_dotenv

def load_repo_env():
    """Load .env from repo root, even if this script is nested deeply."""
    current_dir = Path(__file__).resolve().parent
    while True:
        env_path = current_dir / ".env"
        if env_path.exists():
            load_dotenv(env_path)
            break
        if current_dir.parent == current_dir:
            raise FileNotFoundError(".env file not found in any parent directory.")
        current_dir = current_dir.parent

load_repo_env()

# --- SQLAlchemy Imports ---
from sqlalchemy import text
from sqlalchemy.engine import Engine

DB_CONFIG = {
    'host': os.getenv("DB_HOST", "localhost"),
    'port': int(os.getenv("DB_PORT", 5469)),
    'user': os.getenv("DB_USER", "suite_user"),
    'password': os.getenv("DB_PASS", "")
}

# Optional: dataset-specific port logic
def get_db_port(db_name: str) -> int:
    db_lower = db_name.lower()
    port_map = {
        "imdb": os.getenv("IMDB_PORT"),
        "tpch": os.getenv("TPCH_PORT"),
        "tpcds": os.getenv("TPCDS_PORT"),
        "ssb": os.getenv("SSB_PORT"),
        "stack": os.getenv("STACK_PORT")
    }
    return int(port_map.get(db_lower, DB_CONFIG['port']))

RUNS = 1
BASE_EXPERIMENT_DIR = Path("../../experiments/experiment5/5.3/")

# Define the workloads to process in order
WORKLOADS_TO_RUN = [
    {
        "name": "stack_sampled",
        "query_dir": "../../workloads/stack_sampled/",
    },
    # {
    #     "name": "stack",
    #     "query_dir": "../../workloads/stack/",
    # }
]

# Define the databases to process. The keys will be used as aliases in filenames.
DATABASES_TO_PROCESS = {
    "2011": "stack_2011",
    # "2015": "stack_2015",
    # "2019": "stack_2019",
}

def read_queries_from_directory(directory: str) -> List[Tuple[str, str]]:
    """Read SQL queries and their relative paths from a directory."""
    queries = []
    print(f"Reading queries from '{directory}'...")
    if not os.path.isdir(directory):
        print(f"Warning: Directory not found: {directory}")
        return []
    for root, _, files in os.walk(directory):
        for filename in files:
            if filename.endswith(".sql"):
                filepath = Path(root) / filename
                try:
                    with open(filepath, "r") as file:
                        query = file.read().strip()
                        if query:
                            rel_path = str(filepath.relative_to(directory))
                            queries.append((query, rel_path))
                except IOError as e:
                    print(f"Error reading file {filepath}: {e}")
    print(f"Found {len(queries)} queries.")
    return queries

def execute_explain_analyze_alchemy(engine: Engine, query: str) -> dict:
    """Execute EXPLAIN ANALYZE using a SQLAlchemy engine and return the JSON QEP."""
    # Use a 'with' block to check out a connection from the engine's pool
    # and automatically return it when done.
    try:
        with engine.connect() as conn:
            # Use text() to wrap the SQL string, a good practice in SQLAlchemy
            explain_query = text(f"EXPLAIN (ANALYZE, FORMAT JSON) {query}")
            result = conn.execute(explain_query)
            qep_data = result.fetchone()
            return qep_data[0][0] if qep_data else {}
    except Exception as e:
        print(f"  - Error executing EXPLAIN ANALYZE: {e}")
        return {}

def save_json_to_file(data: dict, output_path: Path):
    """Save a dictionary to a JSON file, creating directories if needed."""
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"  - Error saving file to {output_path}: {e}")

import time
from datetime import timedelta
from collections import defaultdict

# --- Main Processing Logic ---

def process_workload(workload_config: dict, engines: Dict[str, Engine]) -> Dict[str, float]:
    """Processes a single workload: reads queries, executes them, and saves results."""
    workload_name = workload_config["name"]
    query_directory = workload_config["query_dir"]
    
    # Define and create output directories for this specific workload
    workload_output_dir = BASE_EXPERIMENT_DIR / workload_name
    train_output_dir = workload_output_dir / "train"
    test_output_dir = workload_output_dir / "test"
    
    train_output_dir.mkdir(parents=True, exist_ok=True)
    test_output_dir.mkdir(parents=True, exist_ok=True)
    
    execution_times = defaultdict(float)

    # --- Read all queries for this workload ---
    queries_with_paths = read_queries_from_directory(query_directory)
    if not queries_with_paths:
        return execution_times

    # --- Process each query ---
    for query_content, query_rel_path in tqdm(queries_with_paths, desc=f"Queries for {workload_name}"):
        
        p_rel_path = Path(query_rel_path)

        # Save original query to the workload-specific training directory
        train_query_path = train_output_dir / query_rel_path
        train_query_path.parent.mkdir(parents=True, exist_ok=True)
        with open(train_query_path, 'w') as f:
            f.write(query_content)

        for runID in range(RUNS):
            run_dir_name = f"run{runID + 1}" if RUNS > 1 else ""
            
            # --- UNIFIED LOGIC: Use a single, consistent output path for all workloads ---
            template_name = p_rel_path.parent.name
            query_id = p_rel_path.stem # e.g., "q3" or "q3__q3-043"
            base_query_output_dir = test_output_dir / run_dir_name / template_name / query_id
            
            # Save query to the structured test output directory
            query_output_path = base_query_output_dir / p_rel_path.name
            query_output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(query_output_path, 'w') as f:
                f.write(query_content)

            # Execute on each database
            for alias, db_name in DATABASES_TO_PROCESS.items():
                engine = engines.get(db_name)
                if not engine:
                    continue
                
                start_time = time.time()                    
                qep_data = execute_explain_analyze_alchemy(engine, query_content)
                elapsed_time = time.time() - start_time
                execution_times[alias] += elapsed_time
                
                if qep_data:
                    # QEP is saved in the same structured directory
                    qep_output_path = base_query_output_dir / f"qep_{alias}.json"
                    save_json_to_file(qep_data, qep_output_path)
    
    return execution_times

def main():
    """Main function to set up engines and process all defined workloads."""
    
    engines: Dict[str, Engine] = {}
    total_execution_times = defaultdict(float)
    
    try:
        # --- Create one engine for each database ---
        print("Creating SQLAlchemy engines...")
        for db_name in DATABASES_TO_PROCESS.values():
            engine = get_alchemy_engine(db_name)
            if engine:
                engines[db_name] = engine
                print(f"  - Engine for '{db_name}' created successfully.")
            else:
                print(f"  - FAILED to create engine for '{db_name}'.")
        
        if not engines:
            print("\nNo database engines could be created. Aborting.")
            return

        # --- Loop through and process each workload ---
        for workload_config in WORKLOADS_TO_RUN:
            workload_name = workload_config["name"]
            print(f"\n{'='*20} STARTING WORKLOAD: {workload_name.upper()} {'='*20}")
            
            workload_times = process_workload(workload_config, engines)
            
            # Aggregate execution times
            for alias, total_seconds in workload_times.items():
                total_execution_times[alias] += total_seconds
            
            print(f"\n--- Workload '{workload_name}' Execution Time Report ---")
            for alias, total_seconds in workload_times.items():
                print(f"  - Total time for '{alias}': {timedelta(seconds=total_seconds)}")
            print(f"{'='*20} FINISHED WORKLOAD: {workload_name.upper()} {'='*20}")

    finally:
        # --- Ensure all engine pools are properly closed ---
        print("\nDisposing of all engine connection pools...")
        for db_name, engine in engines.items():
            if engine:
                engine.dispose()
                print(f"  - Engine pool for '{db_name}' disposed.")

    # --- Report total execution times across all workloads ---
    print("\n--- Final Aggregated Execution Time Report ---")
    for alias, total_seconds in total_execution_times.items():
        print(f"Total execution time for '{alias}': {timedelta(seconds=total_seconds)}")
                
    print("\nProcessing complete.")

if __name__ == "__main__":
    main()