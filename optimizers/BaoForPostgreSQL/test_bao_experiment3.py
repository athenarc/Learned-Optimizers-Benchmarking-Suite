import os
import glob
import shutil
import subprocess
import time
import argparse
import json
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from tqdm import tqdm
import os
from dotenv import load_dotenv

def load_repo_env():
    """Walk up directories until .env is found and load it."""
    current_dir = os.path.abspath(os.path.dirname(__file__))
    while True:
        env_path = os.path.join(current_dir, ".env")
        if os.path.exists(env_path):
            load_dotenv(env_path)
            break
        parent_dir = os.path.dirname(current_dir)
        if parent_dir == current_dir:
            raise FileNotFoundError(".env file not found in any parent directory.")
        current_dir = parent_dir

# Load .env once when the module is imported
load_repo_env()

def pg_connection_string(db_name: str) -> str:
    """
    Return a SQLAlchemy-compatible PostgreSQL connection URL.
    Example:
      postgresql+psycopg2://user:pass@host:port/db_name
    """
    # Load environment variables from repo root
    load_repo_env()

    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")
    host = os.getenv("DB_HOST")
    if (db_name == "imdbload"):
        port = os.getenv("IMDB_PORT", "5432")
    elif (db_name == "tpch"):
        port = os.getenv("TPCH_PORT", "5432")
    elif (db_name == "tpcds"):
        port = os.getenv("TPCDS_PORT", "5432")
    elif (db_name == "ssb"):
        port = os.getenv("SSB_PORT", "5432")
    elif "stack" in db_name:
        port = os.getenv("STACK_PORT", "5432")
    else:
        port = os.getenv("DB_PORT", "5432")

    if not all([user, password, host]):
        raise ValueError("Missing one or more database environment variables (DB_USER, DB_PASS, DB_HOST).")

    # ✅ SQLAlchemy-compatible URL format
    connection_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"
    return connection_url

# --- Global Configurations ---
TIMEOUT_LIMIT = 5 * 60 * 1000  # 3 minutes in ms
NUM_EXECUTIONS = 1
BAO_HOST = '195.251.63.231'
BAO_PORT = 9381
engine = None
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BAO_SERVER_DIR = os.path.join(SCRIPT_DIR, "bao_server")
import socket

# ==============================================================================
# 1. SERVER AND MODEL MANAGEMENT FUNCTIONS
# ==============================================================================

import subprocess
import os
import signal

def kill_process_on_port(port: int) -> bool:
    """Find and kill the process listening on a given TCP port."""
    try:
        # Run ss to get process info
        result = subprocess.run(
            ["ss", "-ltnp"],
            capture_output=True,
            text=True,
            check=True
        )
        lines = [line for line in result.stdout.splitlines() if f":{port} " in line]
        
        if not lines:
            print(f"No process found listening on port {port}")
            return False

        # Extract PID(s) from the line(s)
        killed = False
        for line in lines:
            # Example: users:(("python3",pid=4056450,fd=48))
            if "pid=" in line:
                pid_str = line.split("pid=")[1].split(",")[0]
                pid = int(pid_str)
                try:
                    os.kill(pid, signal.SIGTERM)  # or SIGKILL if you want to force
                    print(f"Killed process {pid} on port {port}")
                    killed = True
                except ProcessLookupError:
                    print(f"Process {pid} not found (already exited?)")
        return killed
    except subprocess.CalledProcessError as e:
        print(f"Error running ss: {e.stderr}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False

from time import time, sleep
def kill_and_clean_bao_server():
    print("--- Terminating BAO Server and Cleaning Up ---")
    try:
        # pkill is system-wide, so no path needed here.
        subprocess.run(['pkill', '-f', 'bao_server/main.py'], check=False)
        sleep(2)

        # Use the explicit BAO_SERVER_DIR path for all file operations.
        shutil.rmtree(os.path.join(BAO_SERVER_DIR, "bao_default_model"), ignore_errors=True)
        if os.path.exists(os.path.join(BAO_SERVER_DIR, "bao.db")):
            os.remove(os.path.join(BAO_SERVER_DIR, "bao.db"))
        
        # Run clean_experience.py using the correct CWD.
        subprocess.run(['python3', 'clean_experience.py'], cwd=BAO_SERVER_DIR, check=False)
        print("BAO server state cleaned.")
    except Exception as e:
        print(f"An error occurred during server cleanup: {e}")

def load_checkpoint_into_server(checkpoint_path):
    """Copies a specific checkpoint model to the server's default location."""
    print(f"--- Loading checkpoint: {os.path.basename(checkpoint_path)} ---")
    kill_and_clean_bao_server() # Ensure a clean state before loading

    dest_model_path = os.path.join(BAO_SERVER_DIR, "bao_default_model")
    if not os.path.isdir(checkpoint_path):
        print(f"Error: Checkpoint path is not a directory: {checkpoint_path}")
        return False
    
    try:
        shutil.copytree(checkpoint_path, dest_model_path)
        print(f"Successfully copied checkpoint to {dest_model_path}")
        return True
    except Exception as e:
        print(f"Error copying checkpoint model: {e}")
        return False

def start_bao_server():
    """Starts the BAO server as a background process after ensuring the port is free."""
    print("--- Starting BAO server ---")
    
    try:
        # We redirect stdout and stderr to DEVNULL to keep the main script's output clean
        server_process = subprocess.Popen(
            ["python3", "main.py"],
            cwd="bao_server",
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        sleep(10)

        if server_process.poll() is None:
            print("BAO server started successfully.")
            return server_process
        else:
            print("Failed to start BAO server. It may have crashed on startup.")
            return None
    except Exception as e:
        print(f"An error occurred while starting the BAO server: {e}")
        return None

# ==============================================================================
# 2. DATABASE AND QUERY EXECUTION FUNCTIONS
# ==============================================================================

def get_alchemy_engine(db_name):
    """Create SQLAlchemy engine with connection pooling"""
    global engine
    if engine is None:
        db_url = pg_connection_string(db_name)
        engine = create_engine(
            db_url,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_recycle=3600,
            pool_pre_ping=True
        )
    return engine

def run_query(sql, db_name):
    """Runs a query against the DB with BAO enabled and returns measurements."""
    measurements = []
    local_engine = get_alchemy_engine(db_name)

    try:
        with local_engine.connect() as conn:
            conn.execute(text(f"""
                SET enable_bao TO on;
                SET bao_host = '{BAO_HOST}';
                SET bao_port = {BAO_PORT};
                SET enable_bao_selection TO on;
                SET enable_bao_rewards TO off; -- No rewards during testing
                SET statement_timeout TO {TIMEOUT_LIMIT};
            """))
            
            for _ in range(NUM_EXECUTIONS):
                result = conn.execute(text(f"EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON) {sql}")).fetchone()
                if result:
                    plan_list = result[0]
                    plan_json = plan_list[-1] # The last element contains the actual plan details
                    measurements.append({
                        'execution_time': plan_json.get('Execution Time', TIMEOUT_LIMIT * 2),
                        'planning_time': plan_json.get('Planning Time', TIMEOUT_LIMIT * 2),
                        'execution_plan': plan_list
                    })
    except Exception as e:
        print(f"Query failed or timed out: {e}")
        # Append failure metrics for all executions if an error occurs
        for _ in range(NUM_EXECUTIONS):
            measurements.append({
                'execution_time': TIMEOUT_LIMIT * 2,
                'planning_time': TIMEOUT_LIMIT * 2,
                'execution_plan': {'Error': str(e)}
            })
    
    return measurements

def fetch_queries(directory_path, query_order_file):
    """
    Discovers all .sql files recursively and yields them, respecting an
    order file if provided.
    Yields: (full_path, filename, sql_content)
    """
    # Step 1: Discover ALL .sql files recursively and create a map.
    print(f"Recursively searching for .sql files in: {directory_path}")
    all_query_full_paths = glob.glob(os.path.join(directory_path, '**', '*.sql'), recursive=True)
    
    # Create a dictionary mapping basename (e.g., "1a.sql") to its full path
    # This allows us to easily find the full path for a file listed in the order file.
    file_map = {os.path.basename(p): p for p in all_query_full_paths}

    if not file_map:
        print(f"Warning: No .sql files were found in {directory_path} or its subdirectories.")
        return # Return an empty generator

    # Step 2: Yield queries in the correct order.
    if query_order_file and os.path.exists(query_order_file):
        print(f"Applying query order from: {query_order_file}")
        with open(query_order_file, 'r') as f:
            query_order = [line.strip() for line in f if line.strip()]
        
        for filename in query_order:
            full_path = file_map.get(filename)
            if full_path:
                with open(full_path, 'r') as sql_file:
                    yield (full_path, filename, sql_file.read())
            else:
                print(f"Warning: Query '{filename}' from order file not found in the directory tree.")
    else:
        # Fallback: Yield all found queries in alphabetical order by their full path.
        print("No query order file provided or found. Executing all found queries alphabetically.")
        for full_path in sorted(all_query_full_paths):
            filename = os.path.basename(full_path)
            with open(full_path, 'r') as sql_file:
                yield (full_path, filename, sql_file.read())

def save_execution_plan(original_query_path, checkpoint_full_path, run_id, plan_json):
    """
    Saves a JSON execution plan to a structured directory alongside the original query.
    Structure: .../query_dir/BAO/<checkpoint_type>/<checkpoint_name>/run_<run_id>/<query_name>_plan.json
    """
    try:
        # --- Extract required components from the paths ---
        query_filename = os.path.basename(original_query_path)
        query_dir = os.path.dirname(original_query_path)
        
        # e.g., 'epoch-010_queries-500_...'
        checkpoint_name = os.path.basename(checkpoint_full_path) 
        
        # e.g., 'epoch_checkpoints'
        checkpoint_type = os.path.basename(os.path.dirname(checkpoint_full_path)) 
        
        # --- Define the structured output path ---
        # .../test_queries/BAO/
        bao_base_dir = os.path.join(query_dir, "BAO")
        
        # .../test_queries/BAO/epoch_checkpoints/
        type_specific_dir = os.path.join(bao_base_dir, checkpoint_type)
        
        # .../test_queries/BAO/epoch_checkpoints/epoch-010_..._model/
        checkpoint_specific_dir = os.path.join(type_specific_dir, checkpoint_name)
        
        if NUM_EXECUTIONS > 1:
            run_specific_dir = os.path.join(checkpoint_specific_dir, f"run_{run_id}")
        else:
            run_specific_dir = checkpoint_specific_dir
        
        os.makedirs(run_specific_dir, exist_ok=True)
        
        # Define the final plan file path
        plan_file_path = os.path.join(run_specific_dir, f"{query_filename.replace('.sql', '')}_plan.json")
        
        # Write the plan to the file
        with open(plan_file_path, 'w') as f:
            json.dump(plan_json, f, indent=2)
            
    except Exception as e:
        print(f"Error saving execution plan for {query_filename}: {e}")

# ==============================================================================
# 3. MAIN WORKFLOW
# ==============================================================================

def evaluate_checkpoint(checkpoint_path, db_name, test_queries_path, query_order_file, output_dir_base):
    """Loads a single checkpoint, runs the test workload, and saves the results."""
    # --- Setup for latency file ---
    checkpoint_name = os.path.basename(checkpoint_path)
    results_dir = os.path.join(output_dir_base, "results")
    os.makedirs(results_dir, exist_ok=True)
    latency_file_path = os.path.join(results_dir, "latencies.csv")

    # --- Load Model and Start Server ---
    if not load_checkpoint_into_server(checkpoint_path):
        print(f"Error: Failed to load checkpoint {os.path.basename(checkpoint_path)}. Skipping.")
        return

    server_process = start_bao_server()
    if not server_process:
        print("Error: BAO server failed to start. Skipping this checkpoint.")
        return

    # --- Run Workload ---
    print(f"--- Executing test workload for checkpoint: {checkpoint_name} ---")
    
    # Write header for latency file if it doesn't exist
    if not os.path.exists(latency_file_path):
        with open(latency_file_path, 'w') as f:
            f.write("checkpoint_name,query_file,run_id,planning_time_ms,execution_time_ms\n")

    ### MODIFICATION: Check for queries BEFORE starting the server ###
    # We convert the generator to a list here to check its length.
    queries_to_run = list(fetch_queries(test_queries_path, query_order_file))
    if not queries_to_run:
        print(f"No queries found for workload '{test_queries_path}'. Skipping evaluation for this checkpoint.")
        return # Exit the function early

    for original_path, filename, sql_query in tqdm(queries_to_run, desc=f"Testing {checkpoint_name}"):
        print(f"Running query: {filename}")
        measurements = run_query(sql_query, db_name)
        
        for i, measurement in enumerate(measurements):
            run_id = i + 1
            # Save latency to the central CSV file
            with open(latency_file_path, 'a') as f:
                f.write(f"{checkpoint_name},{filename},{run_id},{measurement['planning_time']},{measurement['execution_time']}\n")

            ### MODIFICATION: Pass the full checkpoint_path instead of just the name ###
            save_execution_plan(
                original_query_path=original_path,
                checkpoint_full_path=checkpoint_path, # Pass the full path
                run_id=run_id,
                plan_json=measurement['execution_plan']
            )

    # --- Teardown ---
    print("--- Shutting down BAO server ---")
    server_process.terminate()
    try:
        server_process.wait(timeout=5) # Wait up to 5 seconds for it to close
    except subprocess.TimeoutExpired:
        print("Server did not terminate gracefully, killing.")
        server_process.kill()

    print("Terminating any process on port 9381...")
    kill_process_on_port(9381)
        
    kill_and_clean_bao_server()

def main(args):
    """Discovers all checkpoints and evaluates them sequentially."""
    checkpoint_base_dir = args.checkpoint_dir_base
    subdirs_to_check = ["epoch_checkpoints", "query_checkpoints", "loss_checkpoints"]
    
    all_checkpoints = []
    for subdir in subdirs_to_check:
        full_subdir_path = os.path.join(checkpoint_base_dir, subdir)
        if os.path.isdir(full_subdir_path):
            checkpoints_in_subdir = [os.path.join(full_subdir_path, d) for d in os.listdir(full_subdir_path) if os.path.isdir(os.path.join(full_subdir_path, d))]
            all_checkpoints.extend(checkpoints_in_subdir)
    
    if not all_checkpoints:
        print(f"Error: No checkpoint directories found in subdirectories of {checkpoint_base_dir}")
        return

    print(f"Found {len(all_checkpoints)} total checkpoints to evaluate.")
    all_checkpoints.sort()

    for checkpoint_path in all_checkpoints:
        evaluate_checkpoint(
            checkpoint_path,
            args.db_name,
            args.test_queries_dir,
            args.query_order_file,
            checkpoint_base_dir
        )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Systematically evaluate all BAO training checkpoints against a test workload.")
    parser.add_argument("checkpoint_dir_base", type=str,
                        help="The base directory containing the 'epoch_checkpoints', 'query_checkpoints', and 'loss_checkpoints' subdirectories.")
    parser.add_argument("test_queries_dir", type=str,
                        help="Directory path containing the test workload SQL files.")
    parser.add_argument("db_name", type=str,
                        help="Postgres Database name to connect to (e.g., 'imdbload').")
    parser.add_argument('--query_order_file', type=str,
                        help='Optional text file specifying the order of query execution.')
    
    args = parser.parse_args()
    
    main(args)