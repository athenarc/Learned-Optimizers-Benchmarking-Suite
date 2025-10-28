import argparse
import os
import socket
from config import *
from multiprocessing import Pool
import random
import glob
import pickle
import time
import psycopg2
import json
import sys
import shutil
from pathlib import Path
import subprocess
import sqlalchemy
import numpy as np
from utils import * # Assuming a utils.py with do_run_query, create_training_file, etc.
from tqdm import tqdm
from multiprocessing import current_process
import sqlalchemy
import re
from time import sleep, time
import signal

# Global dict to store engines per worker process
_process_engines = {}

def get_engine():
    pid = current_process().pid
    if pid not in _process_engines:
        _process_engines[pid] = sqlalchemy.create_engine(DATABASE_URL, pool_pre_ping=True)
    return _process_engines[pid]

# ==============================================================================
# 1. SERVER MANAGEMENT
# ==============================================================================

# Set the directory where the LERO server script is located, which is the parent of this script
LERO_SERVER_DIR = Path(__file__).resolve().parent.parent

def kill_lero_server():
    """Forcefully terminates any running LERO server process."""
    print("--- Killing LERO server process ---")
    subprocess.run(['pkill', '-9', '-f', 'server.py'], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    sleep(2)

    try:
        result = subprocess.run(
            ["ss", "-ltnp"],
            capture_output=True,
            text=True,
            check=True
        )
        lines = [line for line in result.stdout.splitlines() if f":{LERO_SERVER_PORT} " in line]

        if not lines:
            # Fall back to pkill if ss finds nothing
            subprocess.run(['pkill', '-9', '-f', 'server.py'],
                           check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            print("No process found via ss, attempted pkill fallback.")
            return

        for line in lines:
            if "pid=" in line:
                pid_str = line.split("pid=")[1].split(",")[0]
                pid = int(pid_str)
                try:
                    os.kill(pid, signal.SIGKILL)
                    print(f"Killed LERO server process PID {pid} on port {LERO_SERVER_PORT}")
                except ProcessLookupError:
                    print(f"Process {pid} not found (already exited?)")

    except subprocess.CalledProcessError as e:
        print(f"Error running ss: {e.stderr}")
    except Exception as e:
        print(f"Unexpected error while killing LERO server: {e}")
    
    sleep(2)

def start_lero_server(server_script_dir, model_path=None):
    """
    Starts the LERO server. The server.conf file is always created or overwritten.
    If a model_path is provided, the ModelPath key is set.
    Otherwise, the ModelPath key is commented out.
    """
    config_path = os.path.join(server_script_dir, "server.conf")
    config_content = ""

    if model_path:
        print(f"--- Starting LERO server with model from: {model_path} ---")
        # Create config content with an active ModelPath
        config_content = f"""[lero]
Port = {LERO_SERVER_PORT}
ListenOn = {LERO_SERVER_HOST}
ModelPath = {os.path.abspath(model_path)}
"""
    else:
        print("--- Starting LERO server with no initial model ---")
        # Create config content with the ModelPath commented out
        config_content = f"""[lero]
Port = {LERO_SERVER_PORT}
ListenOn = {LERO_SERVER_HOST}
# ModelPath = (No initial model specified)
"""

    # This block now runs in both cases, ensuring the file is always written.
    try:
        with open(config_path, "w") as f:
            f.write(config_content.strip())
        print(f"Configuration written to {config_path}")
    except IOError as e:
        print(f"!!! CRITICAL ERROR: Could not write to config file {config_path}. Error: {e} !!!")
        return None # Abort if we can't write the config

    print("Using LERO server directory:", server_script_dir)
    with open("server_stdout.log", "a") as out, open("server_stderr.log", "a") as err:
        server_process = subprocess.Popen(
            ["python3", "server.py"],
            cwd=server_script_dir,
            stdout=out,
            stderr=err
        )
    sleep(10)

    if server_process.poll() is None:
        print("LERO server started successfully.")
        return server_process
    else:
        print("Failed to start LERO server.")
        return None

class PolicyEntity:
    def __init__(self, score) -> None:
        self.score = score

    def get_score(self):
        return self.score


class CardinalityGuidedEntity(PolicyEntity):
    def __init__(self, score, card_str) -> None:
        super().__init__(score)
        self.card_str = card_str


class PgHelper():
    def __init__(self, queries, output_query_latency_file) -> None:
        self.queries = queries
        self.output_query_latency_file = output_query_latency_file

    def start(self, pool_num):
        pool = Pool(pool_num)
        for fp, q in self.queries:
            pool.apply_async(do_run_query, args=(q, fp, [], self.output_query_latency_file, True, None, None))
        print('Waiting for all subprocesses done...')
        pool.close()
        pool.join()


class LeroHelper():
    def __init__(self, queries, output_query_latency_file, test_queries, args):
        self.queries = queries
        self.output_query_latency_file = output_query_latency_file
        self.test_queries = test_queries
        self.args = args
        self.topK = args.topK if args.topK else 5
        
        self.lero_server_path = LERO_SERVER_PATH
        self.lero_card_file_path = os.path.join(LERO_SERVER_PATH, LERO_DUMP_CARD_FILE)
        # Empty the card file at start
        with open(self.lero_card_file_path, 'w') as f:
            f.write("")
        print("LERO card file path:", self.lero_card_file_path)
        self.loss_file_path = os.path.join(self.lero_server_path, "last_training_loss.txt")

        # Create structured, independent checkpoint directories
        self.checkpoint_dir_base = Path(args.checkpoint_dir_base)
        self.final_model_dir = self.checkpoint_dir_base / "final_model"
        self.epoch_checkpoint_dir = self.checkpoint_dir_base / "epoch_checkpoints"
        self.query_checkpoint_dir = self.checkpoint_dir_base / "query_checkpoints"
        self.loss_checkpoint_dir = self.checkpoint_dir_base / "loss_checkpoints"
        for d in [self.final_model_dir, self.epoch_checkpoint_dir, self.query_checkpoint_dir, self.loss_checkpoint_dir]:
            d.mkdir(parents=True, exist_ok=True)

    def chunks(self, lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def start(self, pool_num, resume_from_checkpoint=None):
        # State-tracking Variables
        start_epoch = 1
        total_queries_seen = 0
        best_actual_loss = float('inf')

        # --- Logic to handle resuming from a checkpoint ---
        if resume_from_checkpoint:
            print("="*60, flush=True)
            print(f"ATTEMPTING TO RESUME TRAINING FROM: {resume_from_checkpoint}")
            checkpoint_name = os.path.basename(resume_from_checkpoint)

            # Regex to parse epoch, queries, and loss from the directory name
            pattern = re.compile(r"epoch-(\d+)_queries-(\d+)_loss-([\d.]+)_reason-.*")
            match = pattern.search(checkpoint_name)

            if match:
                parsed_epoch, parsed_queries, parsed_loss = match.groups()
                # We start the epoch *after* the one in the checkpoint.
                # The checkpoint is saved at the end of a batch/epoch.
                start_epoch = int(parsed_epoch) 
                total_queries_seen = int(parsed_queries)
                best_actual_loss = float(parsed_loss)

                print(f"  -> Successfully parsed checkpoint metadata.")
                print(f"  -> Resuming from Epoch: {start_epoch}")
                print(f"  -> Total Queries Seen So Far: {total_queries_seen}")
                print(f"  -> Last Known Best Loss: {best_actual_loss}")
                # We must start the *next* epoch, so we increment here.
                start_epoch += 1

            else:
                print(f"!!! CRITICAL: Could not parse metadata from checkpoint name: {checkpoint_name}. Aborting. !!!")
                sys.exit(1)
        
        # Adjust checkpoint targets based on resumed state
        next_query_checkpoint_target = ( (total_queries_seen // self.args.checkpoint_query_interval) + 1) * self.args.checkpoint_query_interval
        next_epoch_checkpoint_target = ( ((start_epoch-1) // self.args.checkpoint_epoch_interval) + 1) * self.args.checkpoint_epoch_interval

        run_args = self.get_run_args()

        for epoch in range(start_epoch, self.args.max_epochs + 1):
            print("="*60, flush=True)
            print(f"STARTING EPOCH {epoch}/{self.args.max_epochs}...")
            
            random.shuffle(self.queries)
            all_batches = list(self.chunks(self.queries, self.args.batch_size))
            
            epoch_losses = [] # To track the average loss for this epoch
            
            for batch_idx, batch in enumerate(tqdm(all_batches, desc=f"Epoch {epoch}", unit="batch")):
                print(f"-- Processing Batch {batch_idx + 1}/{len(all_batches)} of Epoch {epoch} --")

                # <<< CHANGE 1: Define temporary log files for this batch >>>
                batch_latency_file = f"{self.output_query_latency_file}_epoch{epoch}_batch{batch_idx}"
                batch_exploratory_file = f"{batch_latency_file}_exploratory"

                # Ensure they are clean before the batch starts
                if os.path.exists(batch_latency_file): os.remove(batch_latency_file)
                if os.path.exists(batch_exploratory_file): os.remove(batch_exploratory_file)
                
                pool = Pool(pool_num)
                for fp, q in tqdm(
                    batch,
                    desc=f"  Queries in Batch {batch_idx + 1}",
                    unit="query",
                    leave=False,
                ):
                    self.run_pairwise(
                        q, fp, run_args,
                        batch_latency_file,
                        batch_exploratory_file,
                        pool
                    )
                    # print(f"Dispatched query {fp} to pool")
                pool.close()
                pool.join()

                total_queries_seen += len(batch)
                
                # --- BATCH-LEVEL RETRAINING ---
                # model_name = f"{self.args.model_prefix}_epoch_{epoch}_batch_{batch_idx}"
                model_name = f"{self.args.model_prefix}"
                is_first_train = (epoch == 1 and batch_idx == 0)
                if not self.retrain(model_name, batch_latency_file, batch_exploratory_file, is_first_train):
                    print(f"!!! CRITICAL: Retraining failed. Stopping training. !!!")
                    kill_lero_server() # Clean up
                    return # Exit the function
                if os.path.exists(batch_latency_file): os.remove(batch_latency_file)
                if os.path.exists(batch_exploratory_file): os.remove(batch_exploratory_file)

                current_batch_loss = float('inf')
                try:
                    with open(self.loss_file_path, 'r') as f:
                        current_batch_loss = float(f.read().strip())
                    epoch_losses.append(current_batch_loss)
                finally:
                    if os.path.exists(self.loss_file_path): os.remove(self.loss_file_path)

                # --- Query-based checkpointing (happens after each batch) ---
                if total_queries_seen >= next_query_checkpoint_target:
                    self.save_checkpoint(epoch, total_queries_seen, current_batch_loss, "query", self.query_checkpoint_dir, model_name)
                    next_query_checkpoint_target += self.args.checkpoint_query_interval

            # --- END OF EPOCH LOGIC ---
            avg_epoch_loss = np.mean(epoch_losses) if epoch_losses else float('inf')
            print(f"\nEPOCH {epoch} COMPLETE. Queries seen: {total_queries_seen}. Avg Epoch Loss: {avg_epoch_loss:.8f}")

            # The "model_name" for epoch/loss checkpoints is the one from the *last batch* of the epoch.
            
            # Epoch-based checkpointing
            if epoch >= next_epoch_checkpoint_target:
                self.save_checkpoint(epoch, total_queries_seen, avg_epoch_loss, "epoch", self.epoch_checkpoint_dir, model_name)
                next_epoch_checkpoint_target += self.args.checkpoint_epoch_interval

            # Loss-based checkpointing (based on avg epoch loss)
            if (best_actual_loss - avg_epoch_loss) > self.args.loss_improvement_threshold:
                self.save_checkpoint(epoch, total_queries_seen, avg_epoch_loss, "loss", self.loss_checkpoint_dir, model_name)
                best_actual_loss = avg_epoch_loss
            
            # Stopping Criterion
            condition1_met = (avg_epoch_loss < self.args.loss_threshold) and (epoch > self.args.min_epochs_for_loss_stop)
            if condition1_met:
                print(f"\nSTOPPING: Average Epoch Loss ({avg_epoch_loss:.8f}) is below threshold ({self.args.loss_threshold})")
                self.save_checkpoint(epoch, total_queries_seen, avg_epoch_loss, "final_early_stop", self.final_model_dir, model_name)
                return # Exit the function
        
        print("\nTraining loop finished after reaching max epochs.")
        # Final save is the model from the last batch of the last epoch
        final_epoch_loss = np.mean(epoch_losses) if epoch_losses else float('inf')
        final_model_name = f"{self.args.model_prefix}_epoch_{self.args.max_epochs}_batch_{len(all_batches)-1}"
        self.save_checkpoint(self.args.max_epochs, total_queries_seen, final_epoch_loss, "final_max_epochs", self.final_model_dir, final_model_name)

    def save_checkpoint(self, epoch, total_queries, loss, reason, dest_dir, model_name):
        src_model_path = os.path.join(self.lero_server_path, model_name)

        if not os.path.exists(src_model_path):
            print(f"Warning: Model file not found for checkpointing: {src_model_path}")
            return

        prefix = f"epoch-{epoch:03d}_queries-{total_queries}_loss-{loss:.6f}_reason-{reason}"
        dest_model_path = os.path.join(dest_dir, f"{prefix}_{model_name}")
        
        try:
            # If the destination directory already exists, remove it first.
            # This is necessary because copytree() will fail if the destination exists.
            if os.path.exists(dest_model_path):
                shutil.rmtree(dest_model_path)
                
            shutil.copytree(src_model_path, dest_model_path)
            print(f"  -> Checkpoint saved to '{dest_dir.name}': {os.path.basename(dest_model_path)}")
            
        except Exception as e:
            print(f"---!!! ERROR: Failed to copy checkpoint directory from {src_model_path} to {dest_model_path}. Error: {e} !!!---")

    def retrain(self, model_name, current_latency_file, current_exploratory_file, is_first_train):
        training_data_file = self.output_query_latency_file + ".training"
        create_training_file(training_data_file, current_latency_file, current_exploratory_file)
        
        cmd = ["python3", "train.py", "--training_data", os.path.abspath(training_data_file), "--model_name", model_name, "--training_type", "1"]
        if not is_first_train:
            cmd += ["--pretrain_model_name", model_name]

        # print("Running retrain command:", " ".join(cmd))
        with open("train_stdout.log", "a") as out, open("train_stderr.log", "a") as err:
            subprocess.run(cmd, cwd=self.lero_server_path, stdout=out, stderr=err)

        return self.load_model(model_name)

    def load_model(self, model_name):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((LERO_SERVER_HOST, LERO_SERVER_PORT))
            json_str = json.dumps({"msg_type":"load", "model_path": os.path.abspath(LERO_SERVER_PATH + model_name)})
            print("load_model", json_str)

            s.sendall(bytes(json_str + "*LERO_END*", "utf-8"))
            reply_raw = s.recv(1024)
            s.close()

            # 2. Decode the bytes into a UTF-8 string
            reply_str = reply_raw.decode('utf-8')

            # 3. Parse the JSON string into a Python dictionary
            reply_json = json.loads(reply_str)
            print("load_model reply:", reply_json)

            # 4. NOW you can safely check the dictionary
            if reply_json.get("msg_type") == "succ":
                os.system("sync")
                return True # Success
            else:
                print(f"---!!! WARNING: Server reported an error on loading model '{model_name}': {reply_json.get('error')} !!!---")
                return False # Failure

        except json.JSONDecodeError as e:
            print(f"---!!! ERROR: Could not decode JSON response from server: {reply_raw}. Error: {e} !!!---")
            return False
        except Exception as e:
            print(f"---!!! ERROR: An exception occurred during load_model for '{model_name}': {e} !!!---")
            return False

    def test_benchmark(self, output_file):
        run_args = self.get_run_args()
        for (fp, q) in self.test_queries:
            do_run_query(q, fp, run_args, output_file, True, None, None)

    def get_run_args(self):
        run_args = []
        # LERO configuration
        run_args = [
            "SET enable_lero TO True",
            f"SET lero_server_host TO '{LERO_SERVER_HOST}'",
            f"SET lero_server_port TO {LERO_SERVER_PORT}"
        ]        
        return run_args

    def get_card_test_args(self, card_file_name):
        run_args = [
            "SET enable_lero TO True",
            f"SET lero_server_host TO '{LERO_SERVER_HOST}'",
            f"SET lero_server_port TO {LERO_SERVER_PORT}"
        ]
        run_args.append("SET lero_joinest_fname TO '" + CARDINALITY_FILE_REPOSITORY + "/" + card_file_name + "'")
        return run_args

    def write_card_file_via_udf(self, file_name, content):
        """Write cardinality file using PostgreSQL UDF safely with multiprocessing."""
        try:
            engine = get_engine()
            with engine.connect() as conn:
                with conn.begin():
                    result = conn.execute(
                        sqlalchemy.text("SELECT write_lero_card_file(:file_name, :content)"),
                        {"file_name": file_name, "content": content}
                    )
                    success = result.scalar()
                    if not success:
                        raise RuntimeError(f"UDF failed to write card file {file_name}")
        except Exception as e:
            print(f"Error writing card file via UDF: {e}")
            raise

    def run_pairwise(self, q, fp, run_args, output_query_latency_file, exploratory_query_latency_file, pool):
        # When using LERO, it generates candidates and we explore them
        explain_query(q, run_args) # This populates the card file
        policy_entities = []
        with open(self.lero_card_file_path, 'r') as f:
            lines = f.readlines()
            lines = [line.strip().split(";") for line in lines]
            for line in lines:
                policy_entities.append(CardinalityGuidedEntity(float(line[1]), line[0]))

        policy_entities = sorted(policy_entities, key=lambda x: x.get_score())
        policy_entities = policy_entities[:self.topK]
        # print(f"Query {fp} generated {len(policy_entities)} candidate plans for exploration.")
        print("Top candidate scores:", [e.get_score() for e in policy_entities])
        for i, entity in enumerate(policy_entities):
            card_str = "\n".join(entity.card_str.strip().split(" "))
            card_file_name = f"lero_{fp}_{i}.txt"
            self.write_card_file_via_udf(card_file_name, card_str)
            # The first one is the "best" plan, others are for exploration
            output_file = output_query_latency_file if i == 0 else exploratory_query_latency_file
            pool.apply_async(do_run_query, args=(q, fp, self.get_card_test_args(card_file_name), output_file, True, None, None))

    def predict(self, plan):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((LERO_SERVER_HOST, LERO_SERVER_PORT))
        s.sendall(bytes(json.dumps({"msg_type":"predict", "Plan":plan}) + "*LERO_END*", "utf-8"))
        reply_json = json.loads(s.recv(1024))
        assert reply_json['msg_type'] == 'succ'
        s.close()
        # print(reply_json)
        os.system("sync")
        return reply_json['latency']

def load_queries_from_directory(directory_path, test_split=0.2, workload_order_file=None):
    """Load SQL queries from .sql files in a directory and split into train/test sets"""
    query_files = glob.glob(os.path.join(directory_path, "*.sql"))
    
    if workload_order_file:
        # Load ordered queries from the workload order file
        with open(workload_order_file, 'r') as f:
            ordered_files = [line.strip() for line in f if line.strip()]
        
        # Create full paths for ordered files
        train_queries = []
        for filename in ordered_files:
            full_path = os.path.join(directory_path, filename)
            if os.path.exists(full_path):
                with open(full_path, 'r') as qf:
                    query = qf.read().strip()
                    if query:
                        train_queries.append((filename, query))
            else:
                print(f"Warning: Query file {filename} from order file not found in directory")
        
        # All other queries become test queries
        remaining_files = set(os.path.basename(f) for f in query_files) - set(ordered_files)
        test_queries = []
        for filename in remaining_files:
            full_path = os.path.join(directory_path, filename)
            with open(full_path, 'r') as qf:
                query = qf.read().strip()
                if query:
                    test_queries.append((filename, query))
        
        print(f"Loaded {len(train_queries)} training queries (from order file) "
              f"and {len(test_queries)} test queries")
    else:
        # Original random split behavior
        queries = []
        for file_path in query_files:
            with open(file_path, 'r') as f:
                query = f.read().strip()
                if query:
                    file_name = os.path.basename(file_path)
                    queries.append((file_name, query))
        
        random.shuffle(queries)
        split_idx = int(len(queries) * (1 - test_split))
        train_queries = queries[:split_idx]
        test_queries = queries[split_idx:]
        print(f"Randomly split into {len(train_queries)} training queries "
              f"and {len(test_queries)} test queries")
    
    return train_queries, test_queries

if __name__ == "__main__":
    parser = argparse.ArgumentParser("LERO Model training orchestrator with standardized controls")
    # --- Paths and Identifiers ---
    parser.add_argument("--query_dir", required=True, help="Directory containing train .sql files")
    parser.add_argument("--test_split", type=float, default=0.2, help="Fraction for test set (if not using pre-split)")
    parser.add_argument("--output_query_latency_file", required=True, help="Base path for latency logs")
    parser.add_argument("--model_prefix", required=True, help="Prefix for saved model files (e.g., 'lero_job')")
    parser.add_argument('--checkpoint_dir_base', required=True, help='Base directory for all checkpoints and final model.')
    parser.add_argument('--workload_order_file', default=None, help='Optional file listing queries in desired training order.')
    parser.add_argument('--query_num_per_chunk', type=int, default=25, help='Number of queries per chunk for processing.')
    parser.add_argument('--algo', type=str, default='lero', help='Algorithm to use: "lero" or "pg"')

    # --- Training Control ---
    parser.add_argument('--max_epochs', type=int, default=100, help='Max epochs to run.')
    parser.add_argument('--batch_size', type=int, default=25, help='Number of queries per batch to process.')
    parser.add_argument('--min_epochs_for_loss_stop', type=int, default=50, help='Min epochs before early stopping.')
    parser.add_argument('--loss_threshold', type=float, default=0.1, help='BCE loss threshold for early stopping.')
    
    # --- Checkpointing Control ---
    parser.add_argument('--checkpoint_epoch_interval', type=int, default=10, help='Save checkpoint every N epochs.')
    parser.add_argument('--checkpoint_query_interval', type=int, default=250, help='Save checkpoint every N queries.')
    parser.add_argument('--loss_improvement_threshold', type=float, default=0.001, help='Save checkpoint on loss improvement.')

    # --- LERO and DB Specifics ---
    parser.add_argument("--pool_num", type=int, default=2, help="Number of parallel processes for query execution")
    parser.add_argument("--topK", type=int, default=10, help="Number of candidate plans to explore")
    parser.add_argument('--resume_from_checkpoint', default=None, help='Path to a checkpoint directory to resume training from.')

    args = parser.parse_args()
    output_query_latency_file = args.output_query_latency_file
    print("output_query_latency_file:", output_query_latency_file)

    pool_num = 3
    if args.pool_num:
        pool_num = args.pool_num
    print("pool_num:", pool_num)

    ALGO_LIST = ["lero", "pg"]
    algo = "lero"
    if args.algo:
        assert args.algo.lower() in ALGO_LIST
        algo = args.algo.lower()
    print("algo:", algo)

    if not os.path.exists(LOG_PATH):
        os.makedirs(LOG_PATH)

    queries, test_queries = load_queries_from_directory(args.query_dir, args.test_split, args.workload_order_file)
    print(f"Read {len(queries)} training queries and {len(test_queries)} test queries.")

    if algo == "pg":
        helper = PgHelper(queries, output_query_latency_file)
        helper.start(pool_num)
    else:
        print("Read", len(test_queries), "test queries.")

        # --- Setup and Run ---
        server_process = start_lero_server(LERO_SERVER_DIR, model_path=args.resume_from_checkpoint)
        if not server_process:
            print("Could not start LERO server. Aborting.")
            exit(1)

        query_num_per_chunk = args.query_num_per_chunk
        print("query_num_per_chunk:", query_num_per_chunk)

        model_prefix = None
        if args.model_prefix:
            model_prefix = args.model_prefix
        print("model_prefix:", model_prefix)

        topK = 5
        if args.topK is not None:
            topK = args.topK
        print("topK", topK)
        
        try:
            helper = LeroHelper(queries, args.output_query_latency_file, test_queries, args)
            helper.start(args.pool_num, resume_from_checkpoint=args.resume_from_checkpoint)
            
        finally:
            kill_lero_server()
            print("Training finished and LERO server shut down.")