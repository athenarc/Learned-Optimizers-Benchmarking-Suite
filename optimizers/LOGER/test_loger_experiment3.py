import os
import torch
import pickle
import json
from tqdm import tqdm
import pandas as pd
import numpy as np
import argparse
import glob
from pathlib import Path
from lib.log import Logger
from core import database, Sql, Plan
from model.dqn import DeepQNet
from core.oracle import oracle_database
from lib.timer import timer
from lib.cache import HashCache
import typing

# ==============================================================================
# 1. SETUP AND CONFIGURATION
# ==============================================================================

# Global device setting will be determined at runtime
DEVICE = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
NUM_EXECUTIONS = 1  # Standardize to 1 execution per query for testing trajectory
CACHE_INVALID_COUNT = 0
CACHE_BACKUP_INTERVAL = 400
_cache_use_count = 0
DEVICE = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
USE_ORACLE = False
USE_LATENCY = True
SEED = 3407
cache_manager = HashCache()
CACHE_FILE = 'latency_cache.pkl'
# Configuration
FILE_ID = '1'
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
log = Logger(f'log/{FILE_ID}.test.log', buffering=1, stderr=True)
# Performance log file
PERFORMANCE_LOG_PATH = "results/performance_log.txt"

class BaselineCache:
    def __init__(self, sqls=None):
        self.data = {}
        self.timeout = None
        self.max_time = None
        if sqls:
            self.init(sqls)

    def state_dict(self):
        return {
            'data': self.data,
            'timeout': self.timeout,
            'max_time': self.max_time
        }

    def load_state_dict(self, state_dict):
        res = state_dict.get('data', None)
        if res is not None:
            self.data = res
        res = state_dict.get('timeout', NotImplemented)
        if res is not NotImplemented:
            self.timeout = res
        res = state_dict.get('max_time', NotImplemented)
        if res is not NotImplemented:
            self.max_time = res

    def init(self, sqls, verbose=False):
        if verbose:
            sqls = tqdm(sqls)
        costs = []
        for sql in sqls:
            sql : Sql
            _, _cost = cache_latency(sql)
            costs.append(_cost)
            _baseline = sql.baseline.join_order
            baseline = []
            valid = True
            leftdeep = True
            if not _baseline or len(sql.baseline.aliases) != len(sql.aliases):
                # might include subqueries
                if _baseline:
                    print(sql.baseline.aliases, sql.aliases)
                log(f'Warning: Baseline of SQL {sql.filename} is not valid')
                valid = False
            else:
                for index, (left, right) in enumerate(_baseline):
                    if database.config.bushy:
                        if index > 0:
                            if isinstance(right, int):
                                if isinstance(left, int):
                                    leftdeep = False
                                left, right = right, left
                            elif not isinstance(left, int):
                                leftdeep = False
                        baseline.append(((left, right), 0))
                    else:
                        if index > 0:
                            if isinstance(right, int):
                                if isinstance(left, int):
                                    log(f'Warning: Baseline of SQL {sql.filename} is not left-deep')
                                    valid = False
                                    break
                                left, right = right, left
                            elif not isinstance(left, int):
                                log(f'Warning: Baseline of SQL {sql.filename} is not left-deep')
                                valid = False
                                break
                        baseline.append(((left, right), 0))
            if not valid:
                continue
            plan = Plan(sql)
            for left, right in _baseline:
                plan.join(left, right)
            _, plan_cost = cache_latency(plan)
            value = plan_cost / _cost
            self.data[str(sql)] = (value, tuple(baseline), leftdeep)

        self.max_time = max(costs)
        self.timeout = int(database.config.sql_timeout_limit * self.max_time)
        if USE_LATENCY:
            self.set_timeout()

    def set_timeout(self):
        if self.max_time:
            self.timeout = int(database.config.sql_timeout_limit * self.max_time)
        database.statement_timeout = self.timeout
        if USE_ORACLE:
            oracle_database.statement_timeout = self.timeout
        log(f'Set timeout limit to {database.statement_timeout}')

    def update(self, sql, baseline, value):
        s = str(sql)
        prev = self.data.get(s, None)
        if prev is None or value < prev[0]:
            leftdeep = True
            baseline = tuple(baseline)
            for index, ((left, right), join) in enumerate(baseline):
                if index > 0:
                    if isinstance(right, int):
                        if isinstance(left, int):
                            leftdeep = False
                            break
                    elif not isinstance(left, int):
                        leftdeep = False
                        break
            self.data[s] = (value, baseline, leftdeep)

    def get_all(self, sql):
        res = self.data.get(str(sql), None)
        if res is not None:
            return res
        return (None, None, False)

    def get(self, sql):
        res = self.data.get(str(sql), None)
        if res is not None:
            return res[1]
        return None

    def get_cost(self, sql):
        res = self.data.get(str(sql), None)
        if res is not None:
            return res[0]
        return None

def initialize_db(args):
    """Initializes the database connection."""
    try:
        database.setup(
            dbname=args.db_name, user=args.user, password=args.password,
            host=args.host, port=args.port, cache=False
        )
        database.config.bushy = True
    except Exception as e:
        print(f"FATAL: Database connection failed: {e}")
        exit(1)

def cache_latency(sql : typing.Union[Sql, Plan]):
    if isinstance(sql, Plan):
        hash = f'{sql.sql.filename} {sql._hash_str(hint=True)}'
    elif isinstance(sql, Sql):
        hash = f'{sql.filename}$'
    else:
        hash = str(sql)
    cache = cache_manager.get(hash, default=None)
    if cache is not None:
        res, count = cache
        count += 1
        if CACHE_INVALID_COUNT <= 0 or count < CACHE_INVALID_COUNT:
            cache_manager.update(hash, (res, count))
            return res
    else:
        pass
    if USE_ORACLE:
        if isinstance(sql, Plan):
            key = sql.oracle()
        elif isinstance(sql, Sql):
            key = sql.oracle()
        else:
            key = str(sql)
    else:
        key = str(sql)
    _timer = timer()
    with _timer:
        raw_value = cost(key, cache=False)
    value = _timer.time * 1000
    value = (value, raw_value)
    cache_manager.put(key, (value, 0), hash)

    if CACHE_BACKUP_INTERVAL > 0:
        global _cache_use_count
        _cache_use_count += 1
        if _cache_use_count >= CACHE_BACKUP_INTERVAL:
            _cache_use_count = 0
            dic = cache_manager.dump(copy=False)
            with open(CACHE_FILE, 'wb') as f:
                pickle.dump(dic, f)
    return value

def _cost(plan, latency=USE_LATENCY, cache=True):
    if isinstance(plan, Plan):
        if USE_ORACLE:
            _sql = plan.oracle()
        else:
            _sql = str(plan)
        origin = str(plan.sql)
    else:
        if USE_ORACLE and isinstance(plan, Sql):
            _sql = plan.oracle()
        else:
            _sql = str(plan)
        origin = None
    if USE_ORACLE:
        return oracle_database.latency(_sql, origin=origin, cache=cache)
    return database.latency(_sql, origin=origin, cache=cache) if latency else database.cost(_sql, cache=cache)

def cost(plan, latency=USE_LATENCY, cache=True):
    if not cache:
        return _cost(plan, latency=True, cache=False)
    return cache_latency(plan)[1]

# ==============================================================================
# 2. CORE EVALUATION FUNCTIONS
# ==============================================================================

def load_loger_checkpoint(model_path, baseline_path):
    """Loads a specific LOGER checkpoint model and its baseline file."""
    for path in [model_path, baseline_path]:
        if not os.path.exists(path):
            raise FileNotFoundError(f"Required artifact not found: {path}")

    checkpoint = torch.load(model_path, map_location=DEVICE)
    with open(baseline_path, 'rb') as f:
        baseline_data = pickle.load(f)

    # Simplified model initialization for testing
    model = DeepQNet(device=DEVICE, half=200, out_dim=4, num_table_layers=1, use_value_predict=False, restricted_operator=True)
    model.model_recover(checkpoint['model'])
    
    baseline_manager = BaselineCache()
    baseline_manager.data = baseline_data
    if 'baseline' in checkpoint:
        baseline_manager.load_state_dict(checkpoint['baseline'])
    if 'timeout_limit' in checkpoint:
        database.statement_timeout = checkpoint['timeout_limit']
    
    model.eval_mode()
    return model, baseline_manager

def save_plan_and_metrics(test_queries_dir, original_query_filename, checkpoint_name, checkpoint_type, run_id, plan_json, metrics):
    """Saves plan and metrics to .../LOGER/<checkpoint_type>/<checkpoint_name>/..."""
    try:
        # Reconstruct the original directory of the query file
        query_dir = Path(test_queries_dir) / Path(original_query_filename).parent
        query_filename_stem = Path(original_query_filename).stem

        if NUM_EXECUTIONS > 1:
            output_dir = (query_dir / "LOGER" / checkpoint_type / checkpoint_name / f"run_{run_id}")
        else:
            output_dir = (query_dir / "LOGER" / checkpoint_type / checkpoint_name)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save plan
        plan_path = output_dir / f"{query_filename_stem}_plan.json"
        with open(plan_path, 'w') as f:
            json.dump(plan_json, f, indent=2)

        # Save metrics
        metrics_path = output_dir / f"{query_filename_stem}_metrics.json"
        with open(metrics_path, 'w') as f:
            json.dump(metrics, f, indent=2)

    except Exception as e:
        print(f"Error saving plan/metrics for {original_query_filename}: {e}")

def evaluate_checkpoint_workload(model, workload_queries, output_dir_base, checkpoint_full_path, test_queries_dir):
    """Tests a loaded LOGER model against a workload and saves structured results."""
    # Extract names for reporting and directory structures
    checkpoint_name = Path(checkpoint_full_path).stem
    checkpoint_type = Path(checkpoint_full_path).parent.name
    
    # Central latency file setup
    results_dir = Path(output_dir_base) / "results"
    results_dir.mkdir(exist_ok=True)
    latency_file_path = results_dir / "latencies.csv"
    
    if not latency_file_path.exists():
        with open(latency_file_path, 'w') as f:
            f.write("checkpoint_name,query_file,run_id,predicted_latency_ms,actual_latency_ms,q_error\n")

    with torch.no_grad():
        for sql in tqdm(workload_queries, desc=f"Testing {checkpoint_name}"):
            try:
                for run_id in range(1, NUM_EXECUTIONS + 1):
                    sql.to(DEVICE)
                    
                    # Get Prediction & Actual Execution
                    plan, _, _, predicted_latency = model.beam_plan(sql, bushy=True, judge=False, return_prediction=True)
                    predicted_latency_ms = predicted_latency * 1000
                    
                    plan_json = database.plan_latency(str(plan),cache=False)[0][0][0]
                    actual_latency_ms = plan_json.get('Execution Time', -1.0)
                    
                    if actual_latency_ms <= 0:
                        q_error = float('inf')
                    else:
                        q_error = max(predicted_latency_ms / actual_latency_ms, actual_latency_ms / predicted_latency_ms)

                    # Save Latency to Central CSV
                    with open(latency_file_path, 'a') as f:
                        f.write(f"{checkpoint_name},{sql.filename},{run_id},{predicted_latency_ms},{actual_latency_ms},{q_error}\n")

                    # Save Plan and Metrics Alongside Query
                    save_plan_and_metrics(
                        test_queries_dir, sql.filename, checkpoint_name, checkpoint_type, run_id,
                        plan_json,
                        {'predicted_latency_ms': predicted_latency_ms, 'actual_latency_ms': actual_latency_ms, 'q_error': q_error}
                    )
            except Exception as e:
                print(f"Error processing {getattr(sql, 'filename', 'unknown query')}: {e}")
                continue

# ==============================================================================
# 3. MAIN WORKFLOW
# ==============================================================================

def discover_checkpoints(checkpoint_dir_base):
    """Finds all valid checkpoint pairs (.pkl, .pkl baseline) in the subdirectories."""
    print(f"Searching for checkpoints in: {checkpoint_dir_base}")
    subdirs_to_check = ["epoch_checkpoints", "query_checkpoints", "loss_checkpoints"]
    checkpoint_pairs = []
    
    for subdir in subdirs_to_check:
        full_subdir_path = Path(checkpoint_dir_base) / subdir
        if not full_subdir_path.is_dir():
            continue
            
        model_files = glob.glob(str(full_subdir_path / "*.pkl"))
        model_files = [f for f in model_files if '_baseline.pkl' not in f]

        for model_path in model_files:
            # Construct the expected baseline path from the model path
            base_name = Path(model_path).stem
            baseline_path = full_subdir_path / f"{base_name}_baseline.pkl"

            if baseline_path.exists():
                checkpoint_pairs.append((model_path, baseline_path))
            else:
                print(f"Warning: Missing baseline file for model {base_name}, skipping.")
    
    return sorted(checkpoint_pairs)

def main(args):
    """Main orchestration function to discover and evaluate all LOGER checkpoints."""
    initialize_db(args)
    
    # Load the test workload once
    print(f"Loading test workload from: {args.test_queries_dir}")
    # Using a simple recursive glob to find all .sql files for the test set
    test_sql_files = glob.glob(os.path.join(args.test_queries_dir, '**', '*.sql'), recursive=True)
    
    # We need to create Sql objects for LOGER to process
    test_set = []
    for f in tqdm(test_sql_files, desc="Loading test queries"):
        relative_path = os.path.relpath(f, args.test_queries_dir)
        test_set.append(Sql(Path(f).read_text(), database.config.feature_length, filename=relative_path))
    print(f"Loaded {len(test_set)} test queries.")

    checkpoint_pairs = discover_checkpoints(args.checkpoint_dir_base)
    if not checkpoint_pairs:
        print("No complete checkpoints found to evaluate. Exiting.")
        return

    print(f"Found {len(checkpoint_pairs)} total checkpoints to evaluate.")

    for model_path, baseline_path in checkpoint_pairs:
        try:
            print("\n" + "---" * 20)
            print(f"Evaluating checkpoint: {Path(model_path).name}")
            model, _ = load_loger_checkpoint(model_path, baseline_path)
            evaluate_checkpoint_workload(model, test_set, args.checkpoint_dir_base, model_path, args.test_queries_dir)
        except Exception as e:
            print(f"FATAL ERROR while processing {Path(model_path).name}: {e}")
            continue

import os
from dotenv import load_dotenv

def load_repo_env():
    """Find and load the .env file from repo root."""
    current_dir = os.path.abspath(os.path.dirname(__file__))
    while True:
        env_path = os.path.join(current_dir, ".env")
        if os.path.exists(env_path):
            load_dotenv(env_path)
            break
        parent_dir = os.path.dirname(current_dir)
        if parent_dir == current_dir:
            raise FileNotFoundError(".env file not found.")
        current_dir = parent_dir


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Systematically evaluate all LOGER training checkpoints.")
    load_repo_env()
    
    parser.add_argument("checkpoint_dir_base", type=str,
                        help="The base directory containing the 'epoch_checkpoints', etc. subdirectories.")
    parser.add_argument("test_queries_dir", type=str,
                        help="Directory path containing the test workload SQL files.")
    
    # --- DB Arguments ---
    parser.add_argument("--db_name", type=str, default="imdbload", help="Postgres Database name.")
    parser.add_argument('-U', '--user', default=os.getenv("DB_USER"), help='Database user')
    parser.add_argument('-P', '--password', default=os.getenv("DB_PASS"), help='Database password')
    parser.add_argument('--host', default=os.getenv("DB_HOST"), help='Database host')

    default_port = os.getenv("DB_PORT")  # may be None
    db_port_map = {
        "imdbload": os.getenv("IMDB_PORT"),
        "tpch": os.getenv("TPCH_PORT"),
        "tpcds": os.getenv("TPCDS_PORT"),
        "ssb": os.getenv("SSB_PORT"),
    }

    # stack_* databases handled together
    if not default_port:
        if "stack" in os.getenv("DB_NAME", ""):
            default_port = os.getenv("STACK_PORT", "5432")
        else:
            default_port = db_port_map.get(os.getenv("DB_NAME", ""), "5432")

    parser.add_argument('--port', type=int, default=int(default_port or "5432"),
                        help='Database port')
    args = parser.parse_args()

    if not os.getenv("DB_PORT"):  # only adjust if not globally defined
        if args.database in db_port_map and db_port_map[args.database]:
            args.port = int(db_port_map[args.database])
        elif "stack" in args.database.lower():
            args.port = int(os.getenv("STACK_PORT", "5432"))
        else:
            args.port = 5432
    main(args)