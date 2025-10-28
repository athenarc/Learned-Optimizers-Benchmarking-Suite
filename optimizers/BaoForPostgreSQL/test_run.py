from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from sqlalchemy import text
import os
import argparse
from datetime import datetime
import glob
import subprocess
import shutil
from time import time, sleep
import json
import os
from dotenv import load_dotenv
BAO_HOST = '195.251.63.231'
BAO_PORT = 9381

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

# Database connection string
TIMEOUT_LIMIT = 3 * 60 * 1000
NUM_EXECUTIONS = 3
engine = None

def current_timestamp_str():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

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

from tqdm import tqdm

def fetch_queries(directory_path, query_order_file=None, skip_bao_processed=False):
    queries = []
    if query_order_file:
        # Read the query order from the specified file
        with open(query_order_file, 'r') as f:
            query_order = [line.strip() for line in f.readlines()]
        
        for filename in tqdm(query_order, desc="Processing ordered queries"):
            if not filename.endswith('.sql'):
                print(f"Warning: {filename} is not a .sql file. Skipping.")
                continue
            fileId = filename.split(".")[0]
            file_path = os.path.join(directory_path, fileId, filename)  # Moved here to fix reference
            
            if skip_bao_processed:
                query_dir = os.path.dirname(file_path)
                bao_dir = os.path.join(query_dir, "BAO")
                if os.path.exists(bao_dir):
                    print(f"Skipping {filename} as BAO directory already exists")
                    continue
            
            if os.path.isfile(file_path):
                with open(file_path, 'r') as sql_file:
                    sql_query = sql_file.read()
                yield (file_path, filename, sql_query)
                queries.append((file_path, filename, sql_query))
            else:
                print(f"Warning: {file_path} does not exist. Skipping.")
    else:
        print("No query order file provided. Executing all queries in the directory.")
        all_query_paths = []
        for root, dirs, files in os.walk(directory_path):
            pattern = os.path.join(root, "*.sql")
            all_query_paths.extend(sorted(glob.glob(pattern)))
        
        for query_path in tqdm(all_query_paths, desc="Processing unordered queries"):
            fileId = os.path.basename(query_path).split(".")[0]
            if skip_bao_processed:
                query_dir = os.path.dirname(query_path)
                bao_dir = os.path.join(query_dir, "BAO")
                if os.path.exists(bao_dir):
                    print(f"Skipping {os.path.basename(query_path)} as BAO directory already exists")
                    continue
            
            if os.path.isfile(query_path):
                with open(query_path, 'r') as sql_file:
                    sql_query = sql_file.read()
                yield (query_path, os.path.basename(query_path), sql_query)
                queries.append((query_path, os.path.basename(query_path), sql_query))
            else:
                print(f"Warning: {query_path} does not exist. Skipping.")
    
    return queries

def execute_queries_in_directory(directory_path, db_name, output_file_path, query_order_file=None, skip_bao_processed=False):
    queries = fetch_queries(directory_path, query_order_file, skip_bao_processed)
    # print(f"Found {len(list(queries))} queries to execute in {directory_path}")
    for filePath, filename, sql_query in tqdm(queries, desc="Executing queries", unit="query"):
        print(f"Executing query from file: {filename}")
        use_bao = True
        # Call run_query with the SQL from the file (can set bao_select or bao_reward flags as needed)
        measurements = run_query(sql_query, db_name)

        count = 1        
        for measurement in measurements:
            if measurement['execution_plan'] is None:
                print(f"Warning: Execution plan for {filename} is None. Skipping saving plan.")
                continue
            # Print the best measurement
            output_string = f"{'x' if measurement['hint'] is None else measurement['hint']}, {current_timestamp_str()}, {filename}, {measurement['planning_time']}, {measurement['execution_time']}, {'Bao' if use_bao else 'PG'}"
            print(output_string)
            with open(output_file_path, 'a') as f:
                f.write(output_string)
                f.write(os.linesep)
            
            # From the filepath, get the directory path
            query_dir = os.path.dirname(filePath) 
            # Save the execution plan to a file in a bao directory where the query came from
            bao_dir = os.path.join(query_dir, "BAO")
            
            if NUM_EXECUTIONS > 1:
                run_dir = os.path.join(bao_dir, "run" + str(count))
                os.makedirs(run_dir, exist_ok=True)
                plan_file_path = os.path.join(run_dir ,filename.replace('.sql', '_bao_plan.json'))
            else:
                os.makedirs(bao_dir, exist_ok=True)
                plan_file_path = os.path.join(bao_dir, filename.replace('.sql', '_bao_plan.json'))

            os.makedirs(os.path.dirname(plan_file_path), exist_ok=True)
            with open(plan_file_path, 'w') as plan_file:
                json.dump(measurement['execution_plan'], plan_file, indent=4)
            
            count += 1

import os
import shutil
import subprocess
import signal

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BAO_SERVER_DIR = os.path.join(SCRIPT_DIR, "bao_server")

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
        
        # Run clean_experience.py using the correct CWD.
        subprocess.run(['python3', 'clean_experience.py'], cwd=BAO_SERVER_DIR, check=False)
        print("BAO server state cleaned.")
    except Exception as e:
        print(f"An error occurred during server cleanup: {e}")

def fetch_final_model(model_path: str):
    """Start the bao server with the provided trained model path."""
    # First kill any existing server
    kill_bao_server()
    
    # Check if model directory exists
    if not os.path.exists(model_path):
        print(f"Error: Model directory not found at {model_path}")
        return False
    
    dest_model_path = os.path.join(BAO_SERVER_DIR, "bao_default_model")
    if not os.path.isdir(model_path):
        print(f"Error: Provided model path is not a directory: {model_path}")
        return False
    
    try:
        shutil.copytree(model_path, dest_model_path)
        print(f"Successfully copied model to {dest_model_path}")
        return True
    except Exception as e:
        print(f"Error copying model: {e}")
        return False


def load_model():
    """Instruct the BAO server to load the specified model"""
    model_path = os.path.join(BAO_SERVER_DIR, "bao_default_model")
    print(f"Loading model from {model_path}")
    cmd = f"cd bao_server && python3 baoctl.py --load {model_path}"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"Failed to load model: {result.stderr}")
        return False
    
    print("Model loaded successfully")
    return True

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


def main(workload_directory_path, db_name, output_file_path, query_order, skip_bao_processed, model_path):
    # Fetch the final model and load it
    fetch_final_model(model_path)

    server_process = start_bao_server()
    if not server_process:
        print("Error: BAO server failed to start. Skipping this checkpoint.")
        return

    if not load_model():
        print("Failed to load final model, exiting.")
        kill_bao_server()
        return
        
    # Execute queries and get the path to the actual latencies file
    execute_queries_in_directory(workload_directory_path, db_name, output_file_path, query_order, skip_bao_processed)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Execute SQL queries from files in a directory, record latencies, and calculate Q-error.")
    parser.add_argument("workload_directory", type=str, help="Directory path containing SQL files to execute")
    parser.add_argument("output_file", type=str, default="test_output.txt", help="Path to the output file")
    parser.add_argument("--checkpoint_dir", type=str, required=True, help="Path to the trained BAO model directory")
    parser.add_argument("db_name", type=str, default="imdbload", help="Postgres Database name")
    parser.add_argument('--query_order_file', type=str, help='Text file specifying the order of query files to execute')
    parser.add_argument('--skip_bao_processed', action='store_true', help='Skip queries that already have a BAO directory')
    
    args = parser.parse_args()
    workload_directory_path = args.workload_directory
    db_name = args.db_name
    output_file_path = args.output_file
    query_order = args.query_order_file
    skip_bao_processed = args.skip_bao_processed
    
    # Ensure the provided directory exists
    if not os.path.isdir(workload_directory_path):
        print(f"Error: The directory {workload_directory_path} does not exist.")
    else:
        main(workload_directory_path, db_name, output_file_path, query_order, skip_bao_processed, args.checkpoint_dir)