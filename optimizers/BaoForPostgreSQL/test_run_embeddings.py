import os
import json
import glob
import shutil
import argparse
import subprocess
from time import time, sleep
from datetime import datetime
from tqdm import tqdm
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
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

# --- CONFIGURATION & PATHS ---
# These paths are now defined relative to the script's location.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BAO_SERVER_DIR = os.path.join(SCRIPT_DIR, "bao_server")
TEMP_EMBEDDING_FILE = os.path.join(BAO_SERVER_DIR, "bao_last_embedding.tmp.json")

# --- CONFIGURATION ---
# Database connection string and server details
BAO_HOST = '195.251.63.231'
BAO_PORT = 9381
TIMEOUT_LIMIT_MS = 300000  # 5 minutes in milliseconds
NUM_EXECUTIONS = 1 # Number of times to execute each query phase

engine = None

# --- HELPER FUNCTIONS ---

def get_alchemy_engine(db_name):
    """Creates and returns a SQLAlchemy engine with connection pooling."""
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

def _load_json_safely(file_path):
    if not os.path.exists(file_path): return None
    try:
        with open(file_path, 'r') as f: return json.load(f)
    except: return None

# --- BAO SERVER MANAGEMENT ---

def kill_bao_server():
    print("--- Terminating BAO Server and Cleaning Up ---")
    try:
        # pkill is system-wide, so no path needed here.
        subprocess.run(['pkill', '-f', 'bao_server/main.py'], check=False)
        sleep(2)

        # Use the explicit BAO_SERVER_DIR path for all file operations.
        shutil.rmtree(os.path.join(BAO_SERVER_DIR, "bao_default_model"), ignore_errors=True)
        if os.path.exists(os.path.join(BAO_SERVER_DIR, "bao.db")):
            os.remove(os.path.join(BAO_SERVER_DIR, "bao.db"))
        if os.path.exists(TEMP_EMBEDDING_FILE):
            os.remove(TEMP_EMBEDDING_FILE)
        
        # Run clean_experience.py using the correct CWD.
        subprocess.run(['python3', 'clean_experience.py'], cwd=BAO_SERVER_DIR, check=False)
        print("BAO server state cleaned.")
    except Exception as e:
        print(f"An error occurred during server cleanup: {e}")

def prepare_and_start_bao_server(checkpoint_dir):
    kill_bao_server()
    print("--- Preparing and Starting BAO Server ---")
    if not os.path.exists(checkpoint_dir):
        print(f"FATAL: Final model directory not found at {checkpoint_dir}")
        return False
    try:
        dest_model_path = os.path.join(BAO_SERVER_DIR, "bao_default_model")
        shutil.copytree(checkpoint_dir, dest_model_path)
        print(f"Final model copied to {dest_model_path}")

        # Start the server process by specifying its CWD.
        server_process = subprocess.Popen(
            ["python3", "main.py"],
            cwd=BAO_SERVER_DIR,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        print("Waiting for BAO server to initialize...")
        sleep(30)
        
        if server_process.poll() is None:
            print("BAO server started successfully.")
            return True
        else:
            print("FATAL: Failed to start BAO server.")
            print("STDOUT:", server_process.stdout.read().decode())
            print("STDERR:", server_process.stderr.read().decode())
            return False
    except Exception as e:
        print(f"An error occurred while starting the BAO server: {e}")
        return False

# --- QUERY EXECUTION LOGIC ---

def run_query_and_get_embedding(sql, db_name, bao_select=True, bao_num_arms=5):
    """
    Runs a query, gets the plan, and reads the corresponding embedding from the temp file.
    Returns a dictionary with both the plan and the embedding data.
    """
    result_data = {}
    db_engine = get_alchemy_engine(db_name)
    
    try:
        # Clear the temp file before running to avoid reading stale data
        if os.path.exists(TEMP_EMBEDDING_FILE):
            os.remove(TEMP_EMBEDDING_FILE)

        with db_engine.connect() as conn:
            conn.execute(text(f"""
                SET enable_bao TO on;
                SET bao_host = '{BAO_HOST}';
                SET bao_port = {BAO_PORT};
                SET enable_bao_selection TO {'on' if bao_select else 'off'};
                SET enable_bao_rewards TO 'on';
                SET bao_num_arms TO {bao_num_arms};
                SET statement_timeout TO {TIMEOUT_LIMIT_MS};
            """))
            
            result = conn.execute(text(f"EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON) {sql}")).fetchone()
            
            if result:
                result_data['execution_plan'] = result[0] # The plan is nested
    except Exception as e:
        print(f"An exception or timeout occurred: {e}")
        result_data['execution_plan'] = {'error': str(e)}

    # --- ROBUST FILE WAITING LOGIC ---
    embedding_data = None
    wait_timeout = 2.0  # Wait for a maximum of 2 seconds for the file to appear
    start_wait = time()
    while time() - start_wait < wait_timeout:
        if os.path.exists(TEMP_EMBEDDING_FILE):
            # Attempt to load it. It might be partially written.
            embedding_data = _load_json_safely(TEMP_EMBEDDING_FILE)
            if embedding_data: # If loading was successful, break the loop
                break
        sleep(0.05) # Check every 50ms

    result_data['embedding_data'] = embedding_data
    return result_data

def fetch_queries(directory_path, query_order_file=None):
    """Generator function to fetch queries in order."""
    if query_order_file:
        with open(query_order_file, 'r') as f:
            query_order = [line.strip() for line in f if line.strip()]
        for filename in query_order:
            # Construct the path assuming the format is like '.../1a/1a.sql'
            base_name = os.path.splitext(filename)[0]
            file_path = os.path.join(directory_path, base_name, filename)
            if os.path.exists(file_path):
                yield file_path, filename
            else:
                print(f"Warning: Query file {file_path} from order file not found. Skipping.")
    else:
        for root, _, files in os.walk(directory_path):
            for file in sorted(files):
                if file.endswith('.sql'):
                    yield os.path.join(root, file), file

def execute_queries_in_directory(directory_path, db_name, query_order_file=None, skip_bao_processed=False):
    """Executes the two-phase workload and saves the four files for each query."""
    queries_generator = fetch_queries(directory_path, query_order_file)
    
    for filePath, filename in tqdm(list(queries_generator), desc="Executing Queries"):
        print(f"\nProcessing query: {filename}")

        query_dir = os.path.dirname(filePath)
        bao_analysis_dir = os.path.join(query_dir, "BAO")
        if skip_bao_processed and os.path.exists(bao_analysis_dir):
            print("  - Skipping, BAO directory already exists.")
            continue
        os.makedirs(bao_analysis_dir, exist_ok=True)

        with open(filePath, 'r') as f:
            sql_query = f.read()

        # Run 1: BAO Plan and Embedding
        print("  - Getting BAO Plan and Embedding...")
        bao_result = run_query_and_get_embedding(sql_query, db_name, bao_select=True, bao_num_arms=5)
        
        # Run 2: PostgreSQL Plan and Embedding
        print("  - Getting PostgreSQL Plan and Embedding...")
        pg_result = run_query_and_get_embedding(sql_query, db_name, bao_select=True, bao_num_arms=1)
        
        # Save the four files
        if bao_result.get('execution_plan'):
            with open(os.path.join(bao_analysis_dir, filename.replace('.sql', '_bao_plan.json')), 'w') as f:
                json.dump(bao_result['execution_plan'], f, indent=4)
        if bao_result.get('embedding_data'):
            with open(os.path.join(bao_analysis_dir, filename.replace('.sql', '_bao_embedding.json')), 'w') as f:
                json.dump(bao_result['embedding_data'], f, indent=4)
        else:
            print("  - WARNING: No BAO embedding data found.")
        
        if pg_result.get('execution_plan'):
            with open(os.path.join(bao_analysis_dir, filename.replace('.sql', '_postgres_plan.json')), 'w') as f:
                json.dump(pg_result['execution_plan'], f, indent=4)
        if pg_result.get('embedding_data'):
            with open(os.path.join(bao_analysis_dir, filename.replace('.sql', '_postgres_embedding.json')), 'w') as f:
                json.dump(pg_result['embedding_data'], f, indent=4)
        else:
            print("  - WARNING: No PostgreSQL embedding data found.")

        print(f"  - Saved analysis files to {bao_analysis_dir}")

# --- MAIN EXECUTION ---

def main():
    parser = argparse.ArgumentParser(description="Run workload to generate BAO/PG plans and embeddings.")
    parser.add_argument("workload_directory", help="Directory path containing SQL files.")
    parser.add_argument("db_name", help="PostgreSQL database name.")
    parser.add_argument("--checkpoint_dir", type=str, required=True, help="Path to the trained BAO model directory")    
    parser.add_argument('--query_order_file', help='Text file specifying the order of query execution.')
    parser.add_argument('--skip_bao_processed', action='store_true', help='Skip queries that already have a BAO subdirectory.')
    args = parser.parse_args()

    if not os.path.isdir(args.workload_directory):
        print(f"Error: Workload directory '{args.workload_directory}' not found.")
        return

    if not prepare_and_start_bao_server(args.checkpoint_dir):
        return

    try:
        execute_queries_in_directory(
            args.workload_directory,
            args.db_name,
            args.query_order_file,
            args.skip_bao_processed
        )
    finally:
        print("\nExecution finished or interrupted. Cleaning up.")
        kill_bao_server()
        print("--- Script Finished ---")

if __name__ == "__main__":
    main()