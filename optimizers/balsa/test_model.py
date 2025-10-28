import os
import argparse

import torch
import numpy as np

import balsa
from balsa.util import plans_lib
from run import BalsaAgent

def main(args, balsa_params):
    print(args)
    print('---')
    print(balsa_params)

    agent = BalsaAgent(balsa_params)
    checkpoint_dir = args.checkpoint_dir
    checkpoint_path = os.path.join(checkpoint_dir, 'checkpoint.pt')
    checkpoint_metadata = os.path.join(checkpoint_dir, 'checkpoint-metadata.txt')
    
    with open(checkpoint_metadata, 'r') as f:
        metadata = f.read().strip()
    agent.curr_value_iter = int(metadata.split(',')[1])

    agent.num_query_execs = 0
    agent.num_total_timeouts = 0
    agent.overall_best_train_latency = np.inf
    agent.overall_best_test_latency = np.inf
    agent.overall_best_test_swa_latency = np.inf
    agent.overall_best_test_ema_latency = np.inf
    agent.train_nodes = plans_lib.FilterScansOrJoins(agent.train_nodes)
    agent.test_nodes = plans_lib.FilterScansOrJoins(agent.test_nodes)

    train_ds, train_loader, _, val_loader = agent._MakeDatasetAndLoader()

    plans_dataset = train_ds.dataset if isinstance(
            train_ds, torch.utils.data.Subset) else train_ds
    
    model = agent._MakeModel(plans_dataset)
    checkpoint = torch.load(checkpoint_path)
    model.load_state_dict(checkpoint, strict=False)
    model = model.model
    print(f"Loaded model checkpoint from {checkpoint_path}")

    agent.prev_optimizer_state_dict = checkpoint.get('optimizer_state_dict', None)
    agent.test_query_dir = args.workload_dir
    planner = agent._MakePlanner(model, plans_dataset)

    # Run TRAIN queries
    # to_execute_test, execution_results_test = agent.PlanAndExecute(
    #     model, planner, is_test=False)
    
    # Run TEST queries
    to_execute_test, execution_results_test = agent.PlanAndExecuteSeq(
        model, planner, is_test=True, save_lqo_plans=True, optimizer='neo')

if __name__ == '__main__':
    # WANDB_MODE = 'disabled'
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--checkpoint_dir', type=str)
    parser.add_argument('--run', type=str)
    parser.add_argument('--workload_dir', type=str, default=None, help='Directory containing the workload files.')
    parser.add_argument('--test_workload_dir', type=str, default=None, help='Directory containing the test workload files.')
    parser.add_argument('--skip_neo_processed', action='store_true', help='Skip queries that already have a NEO directory.')
    parser.add_argument('--skip_balsa_processed', action='store_true', help='Skip queries that already have a Balsa directory.')
    parser.add_argument('--require_loger_dir', action='store_true', help='Require a loger_dir to be included in the parameters.')
    args = parser.parse_args()

    name = args.run
    print(f"Loading Balsa for '{name}' experiment.")
    balsa_params = balsa.params_registry.Get(name)
    balsa_params.use_local_execution = True
    
    # No simulation is used for evaluating
    balsa_params.sim = None

    # Set timeout for training queries to 3 minutes
    balsa_params.initial_timeout_ms = 10 * 60 * 1000
    
    balsa_params.init_experience = ""
    
    if args.workload_dir:
        balsa_params.workload_dir = args.workload_dir

    if args.test_workload_dir:
        balsa_params.test_query_dir = args.test_workload_dir

    if args.skip_neo_processed:
        balsa_params.skip_neo_processed = True
    if args.skip_balsa_processed:
        balsa_params.skip_balsa_processed = True
    if args.require_loger_dir:
        balsa_params.require_loger_dir = True

    main(args, balsa_params)

# python test_model.py --run Balsa_JOBLeakageTest2 --model_checkpoint /app/balsa/wandb/run-20230704_175433-fed9n4a8/files/checkpoint.pt