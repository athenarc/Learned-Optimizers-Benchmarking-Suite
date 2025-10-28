import psycopg2
import os
import sys
import random
from time import time, sleep
import shutil
from datetime import datetime
import subprocess
import argparse
import glob
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
import numpy as np
LOSS_FILE_PATH = "bao_server/last_training_loss.txt"
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
    
def save_final_model_and_history(db_name, checkpoint_dir_base): ### MODIFICATION: Pass base dir
    """Save the final trained model and all history, then clean original repo"""
    # Configuration
    model_dir = os.path.join(checkpoint_dir_base, "final_model") ### MODIFICATION: Use base dir
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
    
def save_model_checkpoint(epoch, total_queries, loss, reason, checkpoint_dir_base, sub_dir):
    """Save Bao model checkpoint into a specific subdirectory based on the trigger reason."""
    checkpoint_dir = os.path.join(checkpoint_dir_base, sub_dir)
    os.makedirs(checkpoint_dir, exist_ok=True)
    
    # Create a descriptive prefix for the checkpoint files
    prefix = f"epoch-{epoch:03d}_queries-{total_queries}_loss-{loss:.4f}_reason-{reason}"

    # Define files to copy
    checkpoint_files = [
        "bao_server/bao_default_model",
        "bao_server/bao_default_model.metadata.json"
    ]
    
    for file_path in checkpoint_files:
        if os.path.exists(file_path):
            dest_path = os.path.join(checkpoint_dir, f"{prefix}_{os.path.basename(file_path)}")
            if os.path.isdir(file_path):
                shutil.copytree(file_path, dest_path)
            else:
                shutil.copy2(file_path, dest_path)
            print(f"  -> Checkpoint saved to '{sub_dir}': {os.path.basename(dest_path)}")
        else:
            print(f"Warning: {file_path} not found during checkpointing.")


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

USE_BAO = True
TIMEOUT_LIMIT = 3 * 60 * 1000
NUM_EXECUTIONS = 3

# SQLAlchemy engine cache
ENGINE_CACHE = {}

def get_engine(db_name: str):
    """Get or create a SQLAlchemy engine with connection pooling"""
    if db_name not in ENGINE_CACHE:
        db_url = pg_connection_string(db_name)
        ENGINE_CACHE[db_name] = create_engine(
            db_url,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_recycle=3600,
            pool_pre_ping=True
        )
    return ENGINE_CACHE[db_name]

def run_query(sql, db_name='imdbload', bao_select=False, bao_reward=False):
    """Execute query using connection pool"""
    engine = get_engine(db_name)
    
    try:
        with engine.connect() as conn:
            # Set all parameters in one execute
            conn.execute(text(f"""
                SET enable_bao TO {'on' if bao_select or bao_reward else 'off'};
                SET bao_host = '195.251.63.231';
                SET bao_port = 9381;
                SET enable_bao_selection TO {'on' if bao_select else 'off'};
                SET enable_bao_rewards TO {'on' if bao_reward else 'off'};
                SET bao_num_arms TO 5;
                SET statement_timeout TO {TIMEOUT_LIMIT};
            """))
            
            # Execute for reward if needed
            if bao_reward:
                conn.execute(text(sql))
            
            # Get execution plan
            result = conn.execute(text(f"EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON) {sql}"))
            plan = result.fetchone()[0][-1]
            
            return {
                'execution_time': plan['Execution Time'],
                'planning_time': plan['Planning Time']
            }
            
    except Exception as e:
        print(f"Query failed: {str(e)}")
        return {
            'execution_time': 2 * TIMEOUT_LIMIT,
            'planning_time': 2 * TIMEOUT_LIMIT
        }

def current_timestamp_str():
    return datetime.now().strftime('%Y-m-d %H:%M:%S.%f')

def write_to_file(file_path, output_string):
    print(output_string)
    with open(file_path, 'a') as f:
        f.write(output_string)
        f.write(os.linesep)

def main(args):
    if args.query_order_file:
        with open(args.query_order_file, 'r') as f:
            ordered_filenames = [line.strip() for line in f if line.strip()]
        query_paths = [os.path.join(args.query_dir, fname) for fname in ordered_filenames]
    else:
        pattern = os.path.join(args.query_dir, '**/*.sql')
        query_paths = sorted(glob.glob(pattern, recursive=True))
    print(f"Loaded {len(query_paths)} queries for the training dataset.")

    queries = []
    for fp in query_paths:
        with open(fp) as f:
            query = f.read()
        queries.append((fp, query))
    
    db_name = args.database_name
    random.seed(42)

    if os.path.exists(args.output_file):
        raise FileExistsError(f"The file {args.output_file} already exists, stopping.")

    # --- State-tracking Variables ---
    total_queries_seen = 0

    ### MODIFICATION: Independent progress trackers for each checkpoint type
    best_actual_loss = float('inf') # Tracker for loss-based checkpoints
    next_epoch_checkpoint_target = args.checkpoint_epoch_interval # Tracker for epoch-based
    next_query_checkpoint_target = args.checkpoint_query_interval # Tracker for query-based
    
    print("\n" + "==="*30)
    print("STARTING EPOCH-BASED TRAINING LOOP")
    print(f"Max Epochs: {args.max_epochs}")
    print(f"Batch Size: {args.batch_size}")
    print(f"Checkpoint base directory: {args.checkpoint_dir_base}")
    print("---"*30 + "\n")

    # ===============================================
    # OUTER LOOP FOR EPOCHS
    # ===============================================
    for epoch in range(1, args.max_epochs + 1):
        print("==="*30, flush=True)
        print(f"STARTING EPOCH {epoch}/{args.max_epochs}...")
        
        random.shuffle(queries)
        all_batches = list(chunks(queries, args.batch_size))
        
        epoch_execution_times = []
        
        use_bao_for_selection = (epoch > 1) and USE_BAO
        use_bao_for_reward = USE_BAO
        
        # ===============================================
        # INNER LOOP FOR BATCHES
        # ===============================================
        for batch_idx, batch in enumerate(all_batches):
            print(f"-- Processing Batch {batch_idx + 1}/{len(all_batches)} of Epoch {epoch} --")
            
            # ... (Query execution logic within the batch is the same) ...
            for q_idx, (fp, q) in enumerate(batch):
                for iteration in range(NUM_EXECUTIONS - 1):
                    run_query(q, bao_reward=False, bao_select=use_bao_for_selection, db_name=db_name)
                measurement = run_query(q, bao_reward=use_bao_for_reward, bao_select=use_bao_for_selection, db_name=db_name)
                output_string = f"{epoch}, {batch_idx}, {q_idx}, {current_timestamp_str()}, {fp}, {measurement['planning_time']}, {measurement['execution_time']}, {'Bao' if use_bao_for_selection else 'PG'}"
                write_to_file(args.output_file, output_string)
                epoch_execution_times.append(measurement['execution_time'])
            
            total_queries_seen += len(batch)

            ### MODIFICATION: Query-based checkpointing (MID-EPOCH)
            # Uses its own independent tracker and saves to a separate directory
            if total_queries_seen >= next_query_checkpoint_target:
                # Use the current epoch-level avg loss as a snapshot value
                current_loss_snapshot = np.mean(epoch_execution_times) if epoch_execution_times else -1.0
                save_model_checkpoint(
                    epoch, total_queries_seen, current_loss_snapshot, "query", 
                    args.checkpoint_dir_base, "query_checkpoints"
                )
                next_query_checkpoint_target += args.checkpoint_query_interval

        # ===============================================
        # END OF EPOCH LOGIC
        # ===============================================
        
        # We still need the average execution time for logging purposes
        avg_epoch_exec_time = np.mean(epoch_execution_times) if epoch_execution_times else -1.0
        print(f"\nEPOCH {epoch} COMPLETE. Total queries seen: {total_queries_seen}. Avg Exec Time: {avg_epoch_exec_time:.4f} ms")

        # --- Retrain BAO ---
        print(f"[{current_timestamp_str()}]\tRetraining Bao model after Epoch {epoch}...", flush=True)
        os.system("cd bao_server && python3 baoctl.py --retrain")
        os.system("sync")
        print(f"[{current_timestamp_str()}]\tRetraining done.", flush=True)

        ### MODIFICATION: Read the actual training loss from the communication file
        current_actual_loss = float('inf') # Default to a high value if reading fails
        try:
            if os.path.exists(LOSS_FILE_PATH):
                with open(LOSS_FILE_PATH, 'r') as f:
                    current_actual_loss = float(f.read().strip())
                print(f"Read model training loss from file: {current_actual_loss:.8f}")
            else:
                print(f"Warning: Loss file '{LOSS_FILE_PATH}' not found after retraining.")
        except (IOError, ValueError) as e:
            print(f"ERROR: Could not read or parse loss file. Error: {e}")
        finally:
            if os.path.exists(LOSS_FILE_PATH):
                os.remove(LOSS_FILE_PATH) # Clean up the file for the next epoch

        # --- Epoch-based checkpointing (using actual loss in the filename) ---
        if epoch >= next_epoch_checkpoint_target:
            save_model_checkpoint(
                epoch, total_queries_seen, current_actual_loss, "epoch", 
                args.checkpoint_dir_base, "epoch_checkpoints"
            )
            next_epoch_checkpoint_target += args.checkpoint_epoch_interval

        # --- Loss-based checkpointing (using actual loss) ---
        if (best_actual_loss - current_actual_loss) > args.loss_improvement_threshold:
            save_model_checkpoint(
                epoch, total_queries_seen, current_actual_loss, "loss", 
                args.checkpoint_dir_base, "loss_checkpoints"
            )
            best_actual_loss = current_actual_loss
        
        # --- Stopping Criterion Check (using actual loss) ---
        condition1_met = (current_actual_loss < args.loss_threshold) and (epoch > args.min_epochs_for_loss_stop)
        
        if condition1_met:
            print("\nSTOPPING CONDITION MET.")
            print(f"Reason: Model Loss ({current_actual_loss:.8f}) is below threshold ({args.loss_threshold}) after epoch {args.min_epochs_for_loss_stop}.")
            break

    print("\nTraining loop finished.")
    print("Saving final model and cleaning repository...")
    save_final_model_and_history(db_name, args.checkpoint_dir_base)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run BAO training with epoch-based logic and actual model loss tracking.")
    # ... (All arguments remain the same as the previous version) ...
    parser.add_argument('--database_name', type=str, default='imdbload', help='Database name to query against')
    parser.add_argument('--query_dir', type=str, required=True, help='Directory which contains all the *training* queries')
    parser.add_argument('--output_file', type=str, required=True, help='File in which to store the results')
    parser.add_argument('--query_order_file', type=str, help='Text file specifying the order of query files to execute')
    parser.add_argument('--batch_size', type=int, default=25, help='Number of queries per training batch.')
    parser.add_argument('--max_epochs', type=int, default=100, help='Maximum number of full passes over the training dataset.')
    parser.add_argument('--min_epochs_for_loss_stop', type=int, default=50, help='Minimum epochs to run before loss-based stopping can occur.')
    parser.add_argument('--loss_threshold', type=float, default=0.001, 
                        help="Model's MSE loss threshold for early stopping. (default: 0.001)")
    parser.add_argument('--loss_improvement_threshold', type=float, default=0.0001, 
                        help="Save a checkpoint if model loss improves by at least this much. (default: 0.0001)")
    parser.add_argument('--checkpoint_dir_base', type=str, required=True, help='Base directory to save all checkpoints and the final model.')
    parser.add_argument('--checkpoint_epoch_interval', type=int, default=10, help='Save a checkpoint every N epochs (at the end of the epoch).')
    parser.add_argument('--checkpoint_query_interval', type=int, default=250, help='Save a checkpoint every N queries processed (can happen mid-epoch).')
    
    args = parser.parse_args()
    main(args)