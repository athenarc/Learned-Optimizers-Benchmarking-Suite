import os
import torch
import pickle
import json
from tqdm import tqdm
from lib.log import Logger
from core import database, Sql, Plan, load, safe_load
from model.dqn import DeepQNet
from lib.timer import timer
import typing
import pandas as pd
import numpy as np
from lib.cache import HashCache
from model import explorer
from core.oracle import oracle_database
import time
from lib.randomize import seed, get_random_state, set_random_state

DEVICE = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
USE_ORACLE = False
USE_LATENCY = True
SEED = 3407
NUM_EXECUTIONS = 3

cache_manager = HashCache()
CACHE_FILE = 'latency_cache.pkl'
# Configuration
FILE_ID = '1'
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
log = Logger(f'log/{FILE_ID}.test.log', buffering=1, stderr=True)

def find_sql_files(directory, skip_loger_processed=False):
    """Recursively find only .sql files in a directory, ignoring files if LOGER/ exists"""
    sql_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.sql'):
                full_path = os.path.join(root, file)
                if skip_loger_processed:
                    loger_dir = os.path.join(root, "LOGER")
                    if os.path.exists(loger_dir):
                        # print(f"Skipping {full_path} as LOGER directory already exists")
                        continue

                sql_files.append(full_path)
    return sql_files

def load_sql_files(directory, verbose=False, skip_loger_processed=False):
    """Load SQL files and ensure they're on the correct device"""
    sql_files = find_sql_files(directory, skip_loger_processed=skip_loger_processed)
    
    if verbose:
        print(f"Found {len(sql_files)} SQL files")

    sql_files_iter = tqdm(sql_files, desc="Loading SQL files")

    sql_objects = []
    for sql_file in sql_files_iter:
        try:
            with open(sql_file, 'r') as f:
                query = f.read().strip()
            if not query:
                continue
                
            rel_path = os.path.relpath(sql_file, directory)
            sql = Sql(query, database.config.feature_length, filename=rel_path)
            sql.to(device)  # Ensure on correct device
            sql_objects.append(sql)
            
        except Exception as e:
            print(f"Error loading {sql_file}: {str(e)}")
            continue
            
    return sql_objects

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

def load_model_and_artifacts(model_dir):
    """
    Load the trained LOGER model and associated artifacts from the given directory.
    Expects three files:
        - *model*.pkl        → the main model checkpoint
        - *metadata*.json    → metadata file
        - *baseline*.pkl     → baseline cache file
    """
    # List all files in the directory
    files = os.listdir(model_dir)

    # Identify model, metadata, and baseline files by name pattern
    model_candidates = [f for f in files if f.endswith(".pkl") and "baseline" not in f]
    baseline_candidates = [f for f in files if f.endswith("_baseline.pkl")]
    metadata_candidates = [f for f in files if f.endswith("_metadata.json")]

    if not model_candidates:
        raise FileNotFoundError(f"No model checkpoint (*.pkl) found in {model_dir}")
    if not metadata_candidates:
        raise FileNotFoundError(f"No metadata (*.json) found in {model_dir}")
    if not baseline_candidates:
        raise FileNotFoundError(f"No baseline (*.pkl) found in {model_dir}")

    # Pick the first (or only) match of each
    final_model_path = os.path.join(model_dir, model_candidates[0])
    final_metadata_path = os.path.join(model_dir, metadata_candidates[0])
    final_baseline_path = os.path.join(model_dir, baseline_candidates[0])

    print(f"→ Loading model from: {final_model_path}")
    print(f"→ Loading metadata from: {final_metadata_path}")
    print(f"→ Loading baseline from: {final_baseline_path}")

    # Load metadata
    with open(final_metadata_path) as f:
        metadata = json.load(f)

    # Load baseline data
    with open(final_baseline_path, "rb") as f:
        baseline_data = pickle.load(f)

    # Load model checkpoint
    checkpoint = torch.load(final_model_path, map_location=DEVICE)
    
    # Initialize model
    model = DeepQNet(
        device=DEVICE,
        half=200,
        out_dim=4,
        num_table_layers=1,
        use_value_predict=False,
        restricted_operator=True,
        reward_weighting=0.1,
        log_cap=1.0
    )
    model.model_recover(checkpoint['model'])
    
    # Initialize baseline manager
    baseline_manager = BaselineCache()
    baseline_manager.data = baseline_data
    
    # Set additional properties from checkpoint
    if 'baseline' in checkpoint:
        baseline_manager.load_state_dict(checkpoint['baseline'])
    if 'timeout_limit' in checkpoint:
        database.statement_timeout = checkpoint['timeout_limit']
    
    print(database.statement_timeout)
    model.eval_mode()
    return model, baseline_manager, metadata

def initialize_db(args):
    """Initialize database connection with proper error handling"""
    try:
        database.setup(
            dbname=args.database,
            user=args.user,
            password=args.password,
            host=args.host,
            port=args.port,
            cache=False
        )
        database.config.bushy = True
        log("Database connection successful.")
    except Exception as e:
        log(f"Error setting up database: {e}")
        raise RuntimeError("Database connection failed.")

def dataset_generate(path, verbose=False):
    """Generate dataset using our safe loader"""
    try:
        print(f"Loading dataset from {path}")
        return safe_load(database.config, path, device=device, verbose=verbose)
    except Exception as e:
        print(f"Error loading dataset: {str(e)}")
        raise

CACHE_INVALID_COUNT = 0
CACHE_BACKUP_INTERVAL = 400
_cache_use_count = 0

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

def calculate_q_error(predicted, actual):
    """
    Calculate the Q-error between predicted and actual latencies.
    """
    if predicted == 0 or actual == 0:
        return float('inf')  # Handle division by zero
    return max(predicted / actual, actual / predicted)

def log_performance(query_num, predicted_latency, actual_latency, q_error, inference_time):
    """
    Log performance metrics to a file.
    """
    with open(PERFORMANCE_LOG_PATH, "a") as log_file:
        log_file.write(f"{time.time()} - Query #{query_num} - Predicted Latency: {predicted_latency:.4f} - Actual Latency: {actual_latency:.4f} - Q-error: {q_error:.4f} - Inference Time: {inference_time:.4f}\n")

def ensure_loger_directory(sql_file_path):
    """Ensure a LOGER directory exists for the SQL file"""
    dir_path = os.path.dirname(sql_file_path)
    loger_dir = os.path.join(dir_path, "LOGER")
    os.makedirs(loger_dir, exist_ok=True)
    return loger_dir

def save_plan(plan_data, output_path):
    """Save execution plan to a file"""
    with open(output_path, 'w') as f:
        json.dump(plan_data, f, indent=2)

def test_loger_model(model, workload_path, verbose=False, skip_loger_processed=False):
    """Test LOGER model with proper device handling"""
    model.to(device)  # Ensure model is on correct device
    sql_objects = load_sql_files(workload_path, verbose=verbose)
    
    results = []
    q_errors = []

    with torch.no_grad():
        for sql in tqdm(sql_objects, desc="Testing queries"):
            try:
                count = 0
                while count < NUM_EXECUTIONS:                
                    # Ensure SQL object is on correct device
                    sql.to(device)
                    
                    rel_path = getattr(sql, 'filename', "")
                    full_path = os.path.join(workload_path, rel_path)
                    
                    # Get prediction
                    start_time = time.time()
                    plan, _, _, predicted_latency = model.beam_plan(
                        sql.to(device),  # Explicit device placement
                        bushy=False, 
                        judge=False, 
                        return_prediction=True
                    )
                    predicted_latency = predicted_latency * 1000
                    inference_time = time.time() - start_time
                    
                    # Get actual execution
                    # print((database.plan_latency(str(plan))[0][0]))
                    plan_json = database.plan_latency(str(plan),cache=False)[0][0][0]
                    actual_latency = plan_json['Plan']['Actual Total Time']
                    q_error = calculate_q_error(predicted_latency, actual_latency)
                    q_errors.append(q_error)

                    # Save plan
                    loger_dir = os.path.join(os.path.dirname(full_path), "LOGER")
                    if NUM_EXECUTIONS > 1:
                        run_dir = os.path.join(loger_dir, f"run{count+1}")
                        os.makedirs(run_dir, exist_ok=True)
                        plan_path = os.path.join(run_dir, f"{os.path.splitext(os.path.basename(rel_path))[0]}_loger_plan.json")
                    else:
                        os.makedirs(loger_dir, exist_ok=True)
                        plan_path = os.path.join(loger_dir, f"{os.path.splitext(os.path.basename(rel_path))[0]}_loger_plan.json")
                    with open(plan_path, 'w') as f:
                        json.dump(plan_json, f, indent=2)

                    results.append((
                        rel_path,
                        predicted_latency,
                        actual_latency,
                        q_error,
                        inference_time,
                        plan_path
                    ))
                    
                    if NUM_EXECUTIONS > 1:
                        results_path = os.path.join(run_dir, f"{os.path.splitext(os.path.basename(rel_path))[0]}_loger_metrics.json")
                    else:
                        results_path = os.path.join(loger_dir, f"{os.path.splitext(os.path.basename(rel_path))[0]}_loger_metrics.json")
                    with open(results_path, 'w') as f:
                        json.dump({
                            'query': str(plan),
                            'predicted_latency': predicted_latency,
                            'actual_latency': actual_latency,
                            'q_error': q_error,
                            'inference_time': inference_time
                        }, f, indent=2)

                    if verbose:
                        print(f"Processed {rel_path} on {device}: "
                            f"Predicted {predicted_latency:.2f}s, "
                            f"Actual {actual_latency:.2f}s, "
                            f"Q-error {q_error:.2f}")
                    count += 1
            except Exception as e:
                print(f"Error processing {getattr(sql, 'filename', 'unknown')}: {str(e)}")
                continue

    avg_q_error = sum(q_errors)/len(q_errors) if q_errors else float('inf')
    return results, avg_q_error

def test_model(model, sqls):
    results = []
    q_errors = []

    with torch.no_grad():
        for query_num, sql in enumerate(tqdm(sqls, desc="Testing")):
            # Start timer for inference time
            start_time = time.time()

            # Get the best plan and its predicted latency
            plan, _, _, predicted_latency = model.beam_plan(sql, bushy=False, judge=False, return_prediction=True)

            # Calculate inference time
            inference_time = time.time() - start_time

            # Calculate the actual latency
            actual_latency = cost(plan)
            # Convert milliseconds to seconds
            actual_latency /= 1000

            # Calculate the Q-error
            q_error = calculate_q_error(predicted_latency, actual_latency)
            q_errors.append(q_error)

            # Log performance metrics
            log_performance(query_num, predicted_latency, actual_latency, q_error, inference_time)

            # Save results
            results.append((sql.filename, predicted_latency, actual_latency, q_error, inference_time))
            log(f'Query: {sql.filename}, Predicted Latency: {predicted_latency}, Actual Latency: {actual_latency}, Q-error: {q_error}, Inference Time: {inference_time}')

    # Calculate the average Q-error
    average_q_error = sum(q_errors) / len(q_errors)
    log(f'Average Q-error: {average_q_error}')

    return results, average_q_error

# Save results to a CSV file
def save_results(results, average_q_error, output_file):
    df = pd.DataFrame(results, columns=['filename', 'predicted_latency', 'actual_latency', 'q_error', 'inference_time'])
    df.to_csv(output_file, index=False)
    # Save the average Q-error at the end of the output file
    with open(output_file, 'a') as f:
        f.write(f'\nAverage Q-error: {average_q_error}')

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
    import argparse

    load_repo_env()  # Load .env from repo root

    parser = argparse.ArgumentParser(description='Test the trained LOGER model')

    parser.add_argument('workload_path', type=str, help='Path to workload directory')
    parser.add_argument('-o', '--output', type=str, default='test_results.csv',
                        help='Output CSV file path')
    parser.add_argument('--verbose', action='store_true', help='Verbose output')
    parser.add_argument('-D', '--database', default='imdbload', help='Database name')
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
    parser.add_argument('--skip_loger_processed', action='store_true',
                        help='Skip already processed LOGER queries')
    parser.add_argument('--checkpoint_dir', type=str, default='checkpoints',
                        help='Directory to save model checkpoints')
    args = parser.parse_args()

    if not os.getenv("DB_PORT"):  # only adjust if not globally defined
        if args.database in db_port_map and db_port_map[args.database]:
            args.port = int(db_port_map[args.database])
        elif "stack" in args.database.lower():
            args.port = int(os.getenv("STACK_PORT", "5432"))
        else:
            args.port = 5432

    skip_loger_processed = args.skip_loger_processed

    # Initialize database
    initialize_db(args)

    # Load model and artifacts
    try:
        model, baseline_manager, metadata = load_model_and_artifacts(args.checkpoint_dir)
        model.eval_mode()
        log("Successfully loaded model and artifacts")
    except Exception as e:
        log(f"Failed to load model: {e}")
        raise

    # Execute testing
    results, avg_q_error = test_loger_model(
        model,
        args.workload_path,
        verbose=args.verbose,
        skip_loger_processed=skip_loger_processed
    )

    # Save results
    if results:
        df = pd.DataFrame(results, columns=[
            'query_path', 'predicted_latency', 'actual_latency',
            'q_error', 'inference_time', 'plan_path'
        ])
        df.to_csv(args.output, index=False)
        print(f"\nResults saved to {args.output}")
        print(f"Average Q-error: {avg_q_error:.4f}")
        print(f"Processed {len(df)} queries successfully")
    else:
        print("No queries processed successfully")