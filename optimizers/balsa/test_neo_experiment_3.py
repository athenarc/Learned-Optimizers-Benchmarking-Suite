import os
import argparse
import glob
from pathlib import Path
from tqdm import tqdm
import json

import torch
import numpy as np

import balsa
from balsa.util import plans_lib
from train_neo_experiment3 import BalsaAgent, HintStr, ParseExecutionResult, ExecuteSql
import ray

# ==============================================================================
# 1. STANDARDIZED RESULT SAVING
# ==============================================================================

NUM_EXECUTIONS = 1

def save_results(test_queries_dir, query_node, checkpoint_name, checkpoint_type,
                 plan_json, metrics, latency_file_path):
    """
    Saves results in our standard format: central latency CSV and a rich,
    distributed metrics JSON file.
    """
    query_filename = Path(query_node.info['path']).name
    raw_name = query_node.info.get('query_name', Path(query_filename).stem)
    query_name = Path(raw_name).stem
    
    # Run the saving logic for each execution run
    for run_id in range(1, NUM_EXECUTIONS + 1):
        # 1. Save core latencies to the central CSV file
        with open(latency_file_path, 'a') as f:
            f.write(f"{checkpoint_name},{query_filename},{run_id},"
                    f"{metrics.get('planning_time_ms', -1.0)},"
                    f"{metrics.get('actual_latency_ms', -1.0)}\n")

        # 2. Save Plan and all Metrics alongside the original query file
        try:
            query_dir = Path(test_queries_dir) / query_name
            query_filename_stem = Path(query_filename).stem

            # Define the output directory, handling single vs. multiple runs
            if NUM_EXECUTIONS > 1:
                output_dir = (query_dir / "NEO" / checkpoint_type / checkpoint_name / f"run_{run_id}")
            else:
                output_dir = (query_dir / "NEO" / checkpoint_type / checkpoint_name)
            output_dir.mkdir(parents=True, exist_ok=True)
            print(f"Saving results for {query_filename} under {output_dir}")
            
            # Save the execution plan JSON
            plan_path = output_dir / f"{query_filename_stem}_plan.json"
            with open(plan_path, 'w') as f:
                json.dump(plan_json, f, indent=2)

            # Save the rich metrics JSON
            metrics_path = output_dir / f"{query_filename_stem}_metrics.json"
            with open(metrics_path, 'w') as f:
                json.dump(metrics, f, indent=4)
            print(f"  - Saved plan to {plan_path}")
            print(f"  - Saved metrics to {metrics_path}")
        except Exception as e:
            print(f"Error saving plan/metrics for {query_filename}: {e}")

# ==============================================================================
# 2. MODIFIED BalsaAgent.PlanAndExecuteSeq
# ==============================================================================

### We are dynamically replacing the method on the agent instance ###
def modified_PlanAndExecuteSeq(self, model, planner, is_test,
                               checkpoint_name, checkpoint_type,
                               test_queries_dir, latency_file_path):
    """
    A modified version of PlanAndExecuteSeq that saves results immediately
    in our standardized format.
    """
    model.eval()
    nodes = self.test_nodes if is_test else self.train_nodes
    p = self.params

    if p.sim:
        sim = self.GetOrTrainSim()
    
    for i, node in enumerate(tqdm(nodes, desc=f"Testing {checkpoint_name}")):
        # --- 1. Plan and Execute (Original Logic) ---
        planning_time, found_plan, predicted_latency, found_plans = planner.plan(node, self.params.search_method, bushy=self.params.bushy, return_all_found=True)
        predicted_latency, found_plan = self.SelectPlan(found_plans, -1, found_plan, planner, node)
        hint_str = HintStr(found_plan, with_physical_hints=self.params.plan_physical, engine=self.params.engine)
        curr_timeout = self.timeout_controller.GetTimeout(node) if not is_test else 900000

        predicted_costs = sim.Predict(node, [tup[1] for tup in found_plans]) if p.sim else None
        predicted_latency = planner.infer(node, [node])[0]
        node.info['curr_predicted_latency'] = predicted_latency        

        kwarg = {
            'query_name': node.info['query_name'], 'sql_str': node.info['sql_str'],
            'hint_str': hint_str, 'hinted_plan': found_plan, 'query_node': node,
            'predicted_latency': predicted_latency, 'curr_timeout_ms': curr_timeout,
            'found_plans': found_plans, 'predicted_costs': predicted_costs,
            'is_test': is_test, 'use_local_execution': p.use_local_execution,
            'plan_physical': p.plan_physical, 'engine': p.engine,
        }        
        
        task_lambda = lambda: ExecuteSql.options(resources={f'node:{ray.util.get_node_ip_address()}': 1}).remote(**kwarg)
        task = task_lambda()
        result_tup, _ = self._handle_execution_failure(task, task_lambda, kwarg, max_retries=3)
        
        # --- 2. Parse and Save Results (New Logic) ---
        parsed_results = ParseExecutionResult(result_tup, **kwarg)
        real_cost_ms = parsed_results[1]
        
        plan_json = {'Error': 'Execution failed or timed out'}
        planning_time_ms = -1.0
        if real_cost_ms > 0 and parsed_results[0].result:
             plan_json = parsed_results[0].result[0][0][0]
             planning_time_ms = plan_json.get('Planning Time', -1.0)
        
        metrics_to_save = {
            'query_name': node.info['query_name'],
            'sql': node.info['sql_str'],
            'hint_str': hint_str,
            'predicted_latency_ms': predicted_latency, # This is the model's prediction
            'planning_time_ms': planning_time_ms,
            'actual_latency_ms': real_cost_ms
        }
        
        # Call our standardized saving function with the new metrics dict
        save_results(
            test_queries_dir=test_queries_dir,
            query_node=node,
            checkpoint_name=checkpoint_name,
            checkpoint_type=checkpoint_type,
            plan_json=plan_json,
            metrics=metrics_to_save,
            latency_file_path=latency_file_path
        )

# ==============================================================================
# 3. MAIN WORKFLOW
# ==============================================================================

def discover_checkpoints(checkpoint_dir_base):
    """Finds all NEO/Balsa checkpoint.pt files in the subdirectories."""
    subdirs = ["epoch_checkpoints", "query_checkpoints", "loss_checkpoints", "final_model"]
    all_checkpoints = []
    for subdir in subdirs:
        path = Path(checkpoint_dir_base) / subdir
        if path.is_dir():
            # Find all files ending in _checkpoint.pt
            all_checkpoints.extend(glob.glob(str(path / "*_checkpoint.pt")))
    return sorted(all_checkpoints)

def evaluate_checkpoint(checkpoint_path, balsa_params):
    """Loads a single NEO/Balsa checkpoint and runs the test workload against it."""
    checkpoint_name = Path(checkpoint_path).stem.replace("_checkpoint", "")
    checkpoint_type = Path(checkpoint_path).parent.name
    
    print("\n" + "---" * 20)
    print(f"Evaluating checkpoint: {checkpoint_name}")

    # --- 1. Initialize Agent and Load Model State ---
    # The BalsaAgent needs to be re-initialized for each checkpoint to ensure a clean state
    agent = BalsaAgent(balsa_params)
    agent.PlanAndExecuteSeq = modified_PlanAndExecuteSeq.__get__(agent, BalsaAgent)
    agent.num_query_execs = 0
    agent.num_total_timeouts = 0
    agent.overall_best_train_latency = np.inf
    agent.overall_best_test_latency = np.inf
    agent.overall_best_test_swa_latency = np.inf
    agent.overall_best_test_ema_latency = np.inf
    agent.train_nodes = plans_lib.FilterScansOrJoins(agent.train_nodes)
    agent.test_nodes = plans_lib.FilterScansOrJoins(agent.test_nodes)

    # Load the dataset definition, which is needed to build the model
    train_ds, _, _, _ = agent._MakeDatasetAndLoader(log=False)
    plans_dataset = train_ds.dataset if isinstance(train_ds, torch.utils.data.Subset) else train_ds
    
    # Build the model structure and then load the saved weights
    model_pl = agent._MakeModel(plans_dataset)
    state_dict = torch.load(checkpoint_path, map_location=lambda storage, loc: storage)
    model_pl.load_state_dict(state_dict)
    model = model_pl.model
    print(f"Successfully loaded model from {checkpoint_path}")

    # --- 2. Run the Workload ---
    agent.prev_optimizer_state_dict = state_dict.get('optimizer_state_dict', None)
    planner = agent._MakePlanner(model, plans_dataset)
    # The PlanAndExecuteSeq function handles the query-by-query execution
    # It returns a list of results, one for each query in the test set
    results_dir = Path(balsa_params.checkpoint_dir_base) / "results"
    results_dir.mkdir(exist_ok=True)
    latency_file_path = results_dir / "latencies.csv"
    if not latency_file_path.exists():
        with open(latency_file_path, 'w') as f:
            f.write("checkpoint_name,query_file,run_id,planning_time_ms,execution_time_ms\n")

    # Call the modified method
    agent.PlanAndExecuteSeq(
        model, planner, is_test=True,
        checkpoint_name=checkpoint_name,
        checkpoint_type=checkpoint_type,
        test_queries_dir=balsa_params.test_query_dir,
        latency_file_path=latency_file_path
    )

def get_completed_checkpoints(test_queries_dir):
    """
    Scans the test query directory to find which checkpoints have already been
    evaluated by checking for the existence of their output directories.
    This makes the script safe to re-run.
    """
    completed = set()
    # The results are saved under a structure like:
    # <test_queries_dir>/<query_subdir>/NEO/<checkpoint_type>/<checkpoint_name>
    # We can use a glob pattern to find all '<checkpoint_name>' directories.
    glob_pattern = str(Path(test_queries_dir) / "**" / "NEO" / "*" / "*")

    for path_str in glob.glob(glob_pattern):
        path = Path(path_str)
        # Ensure it's a directory, as the results are saved in directories
        if path.is_dir():
            # The checkpoint name is the last component of the path.
            completed.add(path.name)

    return completed

def main(args):
    WANDB_MODE = 'disabled'
    print("Initializing Ray for the test suite...")
    ray.init(ignore_reinit_error=True, resources={'pg': 1})
    
    try:
        # Load the base Balsa parameters from the specified experiment config
        balsa_params = balsa.params_registry.Get(args.run)
        balsa_params.use_local_execution = True
        balsa_params.sim = None # No simulation during testing
        balsa_params.initial_timeout_ms = 15 * 60 * 1000
        balsa_params.init_experience = ""    

        if args.workload_dir:
            balsa_params.workload_dir = args.workload_dir

        if args.test_workload_dir:
            balsa_params.test_query_dir = args.test_workload_dir
            
        balsa_params.checkpoint_dir_base = args.checkpoint_dir_base
        
        all_checkpoints = discover_checkpoints(args.checkpoint_dir_base)
        if not all_checkpoints:
            print(f"No checkpoints found in {args.checkpoint_dir_base}. Exiting.")
            return

        print(f"Found {len(all_checkpoints)} total NEO/Balsa checkpoints to evaluate.")

        # Identify checkpoints that have already been evaluated by checking for output files
        completed_checkpoints = get_completed_checkpoints(balsa_params.test_query_dir)
        if completed_checkpoints:
            print(f"Found results for {len(completed_checkpoints)} previously evaluated checkpoints. These will be skipped.")

        checkpoints_to_run = []
        for checkpoint_path in all_checkpoints:
            checkpoint_name = Path(checkpoint_path).stem.replace("_checkpoint", "")
            if checkpoint_name in completed_checkpoints:
                # Uncomment the line below for verbose logging of skipped checkpoints
                print(f"  - Skipping '{checkpoint_name}' as results already exist.")
                continue
            checkpoints_to_run.append(checkpoint_path)

        print(f"\nProceeding to evaluate {len(checkpoints_to_run)} new checkpoints.")
        if not checkpoints_to_run:
            print("All found checkpoints have already been evaluated.")
            return

        for checkpoint_path in checkpoints_to_run:
            try:
                # We must pass a fresh copy of the params to each evaluation
                # to prevent state from leaking between agent initializations.
                evaluate_checkpoint(checkpoint_path, balsa_params.Copy())
            except Exception as e:
                print(f"FATAL ERROR while processing checkpoint {Path(checkpoint_path).name}: {e}")
                continue
    finally:
        # ========================= MODIFICATION START =========================
        # Shut down Ray cleanly when the script is finished.
        print("Test suite finished. Shutting down Ray.")
        ray.shutdown()
        # ========================== MODIFICATION END ==========================

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Systematically evaluate all NEO/Balsa training checkpoints.")
    
    parser.add_argument("checkpoint_dir_base", type=str,
                        help="The base directory containing the 'epoch_checkpoints', etc., subdirectories.")
    parser.add_argument('--run', type=str)
    parser.add_argument('--workload_dir', type=str, default=None, help='Directory containing the workload files.')
    parser.add_argument('--test_workload_dir', type=str, default=None, help='Directory containing the test workload files.')
    
    args = parser.parse_args()
    main(args)