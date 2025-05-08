import os
import torch
import pickle
from tqdm import tqdm
from lib.log import Logger
from core import database, Sql, Plan, load
from model.dqn import DeepQNet
from lib.timer import timer
import typing
import os
from lib.randomize import seed, get_random_state, set_random_state
import random
from collections.abc import Iterable
import pandas as pd
import math
import numpy as np
from lib.cache import HashCache
from model import explorer
from core.oracle import oracle_database
import time

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

def dataset_generate(path, verbose=False):
    sqls = load(database.config, path, device=device, verbose=verbose)
    return sqls

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
    cache_name = FILE_ID
    CACHE_FILE = f'{args.database}.{cache_name}.pkl'
    if os.path.isfile(CACHE_FILE):
        with open(CACHE_FILE, 'rb') as f:
            dic = pickle.load(f)
            cache_manager.load(dic)

    try:
        # Explicitly pass connection parameters
        database_args = {
            'dbname': args.database,
            'user': args.user,
            'password': args.password,
            'host': args.host,
            'port': args.port
        }
        database.setup(**database_args, cache=False)
        log("Database connection successful.")
    except Exception as e:
        log(f"Error setting up database: {e}")
        raise RuntimeError("Database connection failed. Check connection parameters.")

    database.config.bushy = True

def load_dataset(dataset, device):
    dataset_file = f'temps/{FILE_ID}.dataset.pkl'
    if os.path.isfile(dataset_file):
        # Load the cached workload dataset
        workload_set = torch.load(dataset_file, map_location=device)
        train_set, test_set = workload_set
        for _set in (train_set, test_set):
            for sql in _set:
                sql.to(device)
    else:
        train_path, test_path = dataset

        log('Generating train set')
        train_set = dataset_generate(train_path, verbose=True)
        log('Generating test set')
        test_set = dataset_generate(test_path, verbose=True)

        torch.save([train_set, test_set], dataset_file, _use_new_zipfile_serialization=False)

    return train_set, test_set

# Load the trained model
def load_model(checkpoint_path):
    if not os.path.isfile(checkpoint_path):
        raise FileNotFoundError(f"Checkpoint file not found: {checkpoint_path}")
    dic = torch.load(checkpoint_path, map_location=device)
    model = DeepQNet(device=device, half=200, out_dim=4, num_table_layers=1,
                     use_value_predict=False, restricted_operator=True,
                     reward_weighting=0.1, log_cap=1.0)
    baseline_explorer = explorer.HalfTimeExplorer(0.5, 0.2, 80)    
    model.model_recover(dic['model'])
    start_epoch = dic['epoch'] + 1
    model.schedule(start_epoch)
    baseline_manager = BaselineCache()
    baseline_manager.load_state_dict(dic['baseline'])
    if 'timeout_limit' in dic:
        database.statement_timeout = dic['timeout_limit']
    baseline_explorer.count = dic['baseline_count']
    random_state = dic.get('random_state', None)

    if random_state:
        set_random_state(random_state)
        log('Loaded random state')    
    model.eval_mode()
    return model

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

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--dataset', nargs=2, type=str, default=['dataset/train', 'dataset/test'], help='Training and testing dataset.')
    parser.add_argument('-m', '--model', type=str, default='temps/1.checkpoint.pkl', help='Path to the trained model checkpoint.')
    parser.add_argument('-o', '--output', type=str, default='results/test_results.csv', help='Output CSV file to save results.')
    parser.add_argument('-D', '--database', type=str, default='imdbload', help='PostgreSQL database.')    
    parser.add_argument('-U', '--user', type=str, default='suite_user', help='PostgreSQL user.')
    parser.add_argument('-P', '--password', type=str, default='71Vgfi4mUNPm', help='PostgreSQL user password.')
    parser.add_argument('--port', type=int, default=5469, help='PostgreSQL port.')
    parser.add_argument('-H', '--host', type=str, default='train.darelab.athenarc.gr', help='PostgreSQL host.')
    args = parser.parse_args()

    initialize_db(args)

    train_set, test_set = load_dataset(args.dataset, device)
    sqls = train_set + test_set

    # Load the model
    model = load_model(args.model)

    # Test the model on the workload
    results, average_q_error = test_model(model, sqls)

    # Save the results
    save_results(results, average_q_error, args.output)