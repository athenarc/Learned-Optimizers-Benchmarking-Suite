from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from sqlalchemy import text
import os
import random
import shutil
from datetime import datetime
import subprocess
import argparse
import glob
from pathlib import Path
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

USE_BAO = True

def kill_bao_server():
    """Terminate bao_server/main.py using pkill"""
    try:
        result = subprocess.run(
            ['pkill', '-f', 'bao_server/main.py'],
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE
        )
        
        if result.returncode == 0:
            print("Successfully terminated bao_server process")
            return True
        elif result.returncode == 1:
            print("No running bao_server processes found")
            return False
        else:
            print(f"Error terminating bao_server: {result.stderr.decode().strip()}")
            return False
    except Exception as e:
        print(f"Error running pkill: {str(e)}")
        return False
    
def save_final_model_and_history(target_checkpoint_dir):
    """Save the final trained model and all history, then clean original repo"""
    # Configuration
    model_dir = Path(target_checkpoint_dir).joinpath("final_model")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs(model_dir, exist_ok=True)

    print("Terminating bao_server processes...")
    server_was_running = kill_bao_server()

    # Files/directories to transfer
    transfer_items = [
        "bao_server/bao_default_model",
        "bao_server/bao_default_model.metadata.json",
        "training_time.txt",
        "bao_server/model_archive"
    ]

    # 1. Transfer items to central location
    transferred = []
    for src_path in transfer_items:
        if os.path.exists(src_path):
            if src_path == "bao_server/model_archive":
                # Special handling for archive directory
                archive_dest = os.path.join(model_dir, "archive")
                os.makedirs(archive_dest, exist_ok=True)
                
                for item in os.listdir(src_path):
                    item_src = os.path.join(src_path, item)
                    item_dest = os.path.join(archive_dest, item)
                    
                    if os.path.isdir(item_src):
                        shutil.copytree(item_src, item_dest)
                    else:
                        shutil.copy2(item_src, item_dest)
                transferred.append(src_path)
                print(f"Transferred model archive to {archive_dest}")
            else:
                # Handle regular files/directories
                dest_name = f"{timestamp}_{os.path.basename(src_path)}" if "archive" not in src_path else os.path.basename(src_path)
                dest_path = os.path.join(model_dir, dest_name)
                
                if os.path.isdir(src_path):
                    shutil.copytree(src_path, dest_path)
                else:
                    shutil.copy2(src_path, dest_path)
                transferred.append(src_path)
                print(f"Saved {os.path.basename(src_path)} to {dest_path}")
        else:
            print(f"Warning: Source not found - {src_path}")

    # 2. Clean up original repository
    for src_path in transferred:
        try:
            if os.path.isdir(src_path):
                shutil.rmtree(src_path)
            else:
                os.remove(src_path)
            print(f"Cleaned: Removed {src_path} from original location")
        except Exception as e:
            print(f"Warning: Could not remove {src_path} - {str(e)}")

    # 3. Additional cleanup operations
    try:
        # Additional cleanups
        db_paths = ["bao_server/bao.db", "bao_server/bao_previous_model.metadata.json"]
        for db_path in db_paths:
            if os.path.exists(db_path):
                os.remove(db_path)
        
        # Run experience cleaner
        print("Running experience cleaner...")
        os.system("")
        os.system("python3 bao_server/clean_experience.py")
        print("Experience cleaning completed")

    except Exception as e:
        print(f"Error during additional cleanup: {str(e)}")
        
    # 4. Create summary README
    readme_content = f"""BAO Model Training Summary
===========================
Timestamp: {timestamp}
Storage Location: {model_dir}

Contents:
- Current Model: {timestamp}_bao_default_model
- Model Metadata: {timestamp}_bao_default_model.metadata.json
- Execution Time: {timestamp}_training_time.txt
- Archived Models: archive/ directory

Original repository has been cleaned.
"""
    readme_path = os.path.join(model_dir, f"{timestamp}_README.md")
    with open(readme_path, "w") as f:
        f.write(readme_content)

def save_model_checkpoint(c_idx, target_checkpoint_dir):
    """Save Bao model checkpoint after retraining iteration"""
    checkpoint_dir = Path(target_checkpoint_dir).joinpath("checkpoints")
    os.makedirs(checkpoint_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Define files to copy
    checkpoint_files = [
        "bao_server/bao_default_model",
        "bao_server/bao_default_model.metadata.json"
    ]
    
    for file_path in checkpoint_files:
        if os.path.exists(file_path):
            dest_path = os.path.join(checkpoint_dir, f"{timestamp}_chunk{c_idx}_{os.path.basename(file_path)}")
            if os.path.isdir(file_path):
                shutil.copytree(file_path, dest_path)
            else:
                shutil.copy2(file_path, dest_path)
            print(f"Checkpoint saved: {dest_path}")
        else:
            print(f"Warning: {file_path} not found during checkpointing.")

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

USE_BAO = True
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

def run_query(sql, db_name):
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
            
            result = conn.execute(text(f"EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON) {sql}")).fetchone()
            if result:
                plan_list = result[0]
                plan_json = plan_list[-1] # The last element contains the actual plan details
                measurment = {
                    'execution_time': plan_json.get('Execution Time', TIMEOUT_LIMIT * 2),
                    'planning_time': plan_json.get('Planning Time', TIMEOUT_LIMIT * 2),
                    'execution_plan': plan_list
                }

    except Exception as e:
        print(f"Query failed or timed out: {e}")
        # Append failure metrics for all executions if an error occurs
        measurment = {
            'execution_time': TIMEOUT_LIMIT * 2,
            'planning_time': TIMEOUT_LIMIT * 2,
            'execution_plan': {'Error': str(e)}
        }

    return measurment

def current_timestamp_str():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

def write_to_file(file_path, output_string):
    print(output_string)
    with open(file_path, 'a') as f:
        f.write(output_string)
        f.write(os.linesep)

def main(args):
    # Look for .sql files
    if args.query_order_file:
        with open(args.query_order_file, 'r') as f:
            ordered_filenames = [line.strip() for line in f if line.strip()]
        query_paths = [os.path.join(args.query_dir, fname) for fname in ordered_filenames]
    else:
        pattern = os.path.join(args.query_dir, '**/*.sql')
        query_paths = sorted(glob.glob(pattern, recursive=True))
        random.shuffle(query_paths)  # Randomize the order

    print(f"Found {len(query_paths)} queries in {args.query_dir} and its subdirectories.")

    queries = []
    for fp in query_paths:
        with open(fp) as f:
            query = f.read()
        queries.append((fp, query))
    print("Using Bao:", USE_BAO)

    parent_dir = os.path.dirname(args.query_dir)

    db_name = args.database_name
    print("Running against DB:", db_name)

    random.seed(42)
    REPEAT_COUNT = 5
    query_sequence = queries * REPEAT_COUNT
    pg_chunks, *bao_chunks = list(chunks(query_sequence, 25))

    print("Executing queries using PG optimizer for initial training")

    if os.path.exists(args.output_file):
        raise FileExistsError(f"The file {args.output_file} already exists, stopping.")

    for q_idx, (fp, q) in enumerate(pg_chunks):
        # Warm up the cache
        for iteration in range(NUM_EXECUTIONS - 1):
            measurement = run_query(q, db_name=db_name)
            output_string = f"x, {q_idx}, {iteration}, {current_timestamp_str()}, {fp}, {measurement['planning_time']}, {measurement['execution_time']}, PG"
            write_to_file(args.output_file, output_string)
        
        measurement = run_query(q, bao_reward=True, db_name=db_name)
        output_string = f"x, {q_idx}, {NUM_EXECUTIONS-1}, {current_timestamp_str()}, {fp}, {measurement['planning_time']}, {measurement['execution_time']}, PG"
        write_to_file(args.output_file, output_string)

    for c_idx, chunk in enumerate(bao_chunks):
        print("==="*30, flush=True)
        print(f"Iteration over chunk {c_idx + 1}/{len(bao_chunks)}...")
        if USE_BAO:
            print(f"[{current_timestamp_str()}]\t[{c_idx + 1}/{len(bao_chunks)}]\tRetraining Bao...", flush=True)
            os.system("cd bao_server && python3 baoctl.py --retrain")
            os.system("sync")
            save_model_checkpoint(c_idx + 1, parent_dir, args.target_checkpoint_dir)
            print(f"[{current_timestamp_str()}]\t[{c_idx + 1}/{len(bao_chunks)}]\tRetraining done.", flush=True)

        for q_idx, (fp, q) in enumerate(chunk):
            # Warm up the cache
            for iteration in range(NUM_EXECUTIONS - 1):
                measurement = run_query(q, bao_reward=False, bao_select=USE_BAO, db_name=db_name)
                output_string = f"{c_idx}, {q_idx}, {iteration}, {current_timestamp_str()}, {fp}, {measurement['planning_time']}, {measurement['execution_time']}, Bao"
                write_to_file(args.output_file, output_string)

            measurement = run_query(q, bao_reward=USE_BAO, bao_select=USE_BAO, db_name=db_name)
            output_string = f"{c_idx}, {q_idx}, {NUM_EXECUTIONS-1}, {current_timestamp_str()}, {fp}, {measurement['planning_time']}, {measurement['execution_time']}, Bao"
            write_to_file(args.output_file, output_string)

    print("Saving final model and cleaning repository...")
    save_final_model_and_history(args.target_checkpoint_dir)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--database_name', type=str, default='imdbload', help='Database name to query against')
    parser.add_argument('--query_dir', type=str, required=True, help='Directory which contains all the *training* queries')
    parser.add_argument('--output_file', type=str, required=True, help='File in which to store the results')
    parser.add_argument('--query_order_file', type=str, help='Text file specifying the order of query files to execute')
    parser.add_argument('--target_checkpoint_dir', type=str, required=True, help='Directory to save the final model and history')

    args = parser.parse_args()
    main(args)