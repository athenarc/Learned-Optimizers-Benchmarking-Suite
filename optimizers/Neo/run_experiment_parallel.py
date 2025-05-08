import shutil
import random
import sys
import yaml
import json
from pathlib import Path
import logging
import numpy as np
import torch
import multiprocessing as mp
from database_env import *
from algorithm.neo import *
from model import *

SEED = 123

logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(__name__)

def process_plan(p, env_config, test_set):
    query = p.parts[-1][:-5]
    if query in test_set:
        return None
    plan = Plan()
    plan.load(p)
    env = DataBaseEnv(env_config)
    env.plan = plan
    cost = env.reward()
    return (env.plan, cost, query)

def main():
    mp.set_start_method("spawn")
    np.random.seed(SEED)
    torch.manual_seed(SEED)

    config_path = sys.argv[1]
    env_config_path = sys.argv[2]
    with open(config_path, 'r') as file:
        d = yaml.load(file, Loader=yaml.FullLoader)
    with open(env_config_path, "r") as f:
        env_config = json.load(f)
    Path(d['neo_args']['logdir']).mkdir(parents=True, exist_ok=True)
    shutil.copy(config_path, Path(d['neo_args']['logdir']) / 'config.yaml')
    env_config['return_latency'] = d['neo_args']['latency']

    test_set = env_config['test_queries']
    print(test_set)
    env_config['db_data'] = {
        k: v for k, v in env_config['db_data'].items() if k not in test_set}

    # create_agent
    agent = Agent(TransformerNet(**d['net_args']), collate_fn=collate,
                  device=d['neo_args']['device'])

    # load initial experience
    experience = []
    baseline_plans = {}

    # Parallelize the loading and processing of plans
    path_plan = Path(d['neo_args']['baseline_path'])
    plan_files = list(path_plan.glob("*.sql.json"))

    with mp.Pool(processes=mp.cpu_count()) as pool:
        results = pool.starmap(process_plan, [(p, env_config, test_set) for p in plan_files])

    for result in results:
        if result is not None:
            experience.append(result)
            baseline_plans[result[2]] = result[0]

    print('latency: ', d['neo_args']['latency'] == True)

    torch.manual_seed(SEED)
    random.seed(SEED)
    np.random.seed(SEED)
    alg = Neo(agent, env_config, d['neo_args'],
              d['train_args'], experience, baseline_plans)
    alg.run()

if __name__ == '__main__':
    main()