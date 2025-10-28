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
    def __init__(self, queries, query_num_per_chunk, output_query_latency_file, 
                test_queries, model_prefix, topK, checkpoint_dir, max_epochs) -> None:
        self.queries = queries
        self.query_num_per_chunk = query_num_per_chunk
        self.output_query_latency_file = output_query_latency_file
        self.test_queries = test_queries
        self.model_prefix = model_prefix
        self.topK = topK if topK else 5
        self.max_epochs = max_epochs if max_epochs else 100
        self.current_epoch = 0
        
        self.lero_server_path = LERO_SERVER_PATH
        self.lero_card_file_path = os.path.join(LERO_SERVER_PATH, LERO_DUMP_CARD_FILE)
        with open(self.lero_card_file_path, 'w') as f:
            f.write("")

        # Create checkpoint directory
        self.checkpoint_dir = Path(checkpoint_dir)
        self.final_model_dir = self.checkpoint_dir / "final_model"
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        os.makedirs(self.final_model_dir, exist_ok=True)

    def chunks(self, lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def start(self, pool_num, resume_from_checkpoint=None):
        current_epoch = 0
        run_args = self.get_run_args()
        
        if resume_from_checkpoint:
            # Extract epoch number from checkpoint name
            match = re.search(r'epoch(\d+)', resume_from_checkpoint)
            if match:
                current_epoch = int(match.group(1)) + 1
                print(f"Resuming from epoch {current_epoch}")
            else:
                print("Warning: Could not parse epoch number from checkpoint name. Starting from epoch 0.")
        
        for epoch in range(current_epoch, self.max_epochs):
            print("="*60, flush=True)
            print(f"STARTING EPOCH {epoch}/{self.max_epochs}...")
            all_batches = list(self.chunks(self.queries, self.query_num_per_chunk))
            
            for batch_idx, batch in enumerate(tqdm(all_batches, desc=f"Epoch {epoch}", unit="batch")):
                print(f"-- Processing Batch {batch_idx + 1}/{len(all_batches)} of Epoch {epoch} --")            
        
                batch_latency_file = f"{self.output_query_latency_file}_epoch{epoch}_batch{batch_idx}"
                batch_exploratory_file = f"{batch_latency_file}_exploratory"
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
                
                model_name = f"{self.args.model_prefix}"
                is_first_train = (epoch == 1 and batch_idx == 0)
                if not self.retrain(model_name, batch_latency_file, batch_exploratory_file, is_first_train):
                    print(f"!!! CRITICAL: Retraining failed. Stopping training. !!!")
                    kill_lero_server() # Clean up
                    return # Exit the function
                if os.path.exists(batch_latency_file): os.remove(batch_latency_file)
                if os.path.exists(batch_exploratory_file): os.remove(batch_exploratory_file)
            print(f"Completed Epoch {epoch}/{self.max_epochs}.")
            self.save_checkpoint(model_name + f"_epoch{epoch}", self.checkpoint_dir)

        self.save_checkpoint("final_model", self.final_model_dir)

    def save_checkpoint(self, model_name, dest_dir):
        src_model_path = os.path.join(self.lero_server_path, model_name)
        if not os.path.exists(src_model_path):
            print(f"Warning: Model file not found for checkpointing: {src_model_path}")
            return
        
        model_path = os.path.join(dest_dir, model_name)
        os.makedirs(model_path, exist_ok=True)
        try:
            # If the destination directory already exists, remove it first.
            # This is necessary because copytree() will fail if the destination exists.
            if os.path.exists(model_path):
                shutil.rmtree(model_path)
            shutil.copytree(src_model_path, model_path)
            print(f"Checkpoint saved: {model_path}")
        except Exception as e:
            print(f"---!!! ERROR: Failed to copy checkpoint directory from {src_model_path} to {model_path}. Error: {e} !!!---")

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
        # print("Top candidate scores:", [e.get_score() for e in policy_entities])
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
    parser = argparse.ArgumentParser("Model training helper")
    parser.add_argument("--query_dir",
                        metavar="PATH",
                        help="Directory containing SQL query files")
    parser.add_argument("--test_split",
                        type=float,
                        default=0.0,
                        help="Fraction of queries to use for testing (default: 0.0)")
    parser.add_argument("--algo", type=str)
    parser.add_argument("--query_num_per_chunk", type=int, default=25)
    parser.add_argument("--output_query_latency_file", metavar="PATH")
    parser.add_argument("--model_prefix", type=str)
    parser.add_argument("--pool_num", type=int)
    parser.add_argument("--topK", type=int)
    parser.add_argument("--workload_order_file",
                    metavar="PATH",
                    help="Text file specifying the order of training queries")
    parser.add_argument('--resume_from_checkpoint', default=None, help='Path to a checkpoint directory to resume training from.')
    parser.add_argument('--target_checkpoint_dir', default='checkpoints/', help='Directory to save model checkpoints.')
    parser.add_argument('--max_epochs', type=int, default=100, help='Number of training epochs.')
    args = parser.parse_args()

    query_dir = args.query_dir
    test_split = args.test_split
    workload_order_file = args.workload_order_file
    print(f"Load queries from directory {query_dir}, test split = {test_split}")
    if workload_order_file:
        print(f"Using workload order from: {workload_order_file}")
    
    queries, test_queries = load_queries_from_directory(
        query_dir, 
        test_split,
        workload_order_file
    )
    print(f"Read {len(queries)} training queries and {len(test_queries)} test queries.")
    output_query_latency_file = args.output_query_latency_file
    print("output_query_latency_file:", output_query_latency_file)

    pool_num = 10
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

    if algo == "pg":
        helper = PgHelper(queries, output_query_latency_file)
        helper.start(pool_num)
    else:
        server_process = start_lero_server(LERO_SERVER_DIR, model_path=args.resume_from_checkpoint)
        if not server_process:
            print("Could not start LERO server. Aborting.")
            exit(1)        

        query_num_per_chunk = args.query_num_per_chunk

        model_prefix = None
        if args.model_prefix:
            model_prefix = args.model_prefix
        print("model_prefix:", model_prefix)

        topK = 5
        if args.topK is not None:
            topK = args.topK
        print("topK", topK)
        
        try:
            helper = LeroHelper(queries, query_num_per_chunk, output_query_latency_file, test_queries, model_prefix, topK, args.target_checkpoint_dir, args.max_epochs)
            helper.start(pool_num, args.resume_from_checkpoint)

        finally:
            kill_lero_server()
            print("Training finished and LERO server shut down.")