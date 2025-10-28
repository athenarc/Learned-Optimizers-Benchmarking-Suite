import os
import argparse

import torch
import numpy as np

import balsa
from balsa.util import plans_lib
from run import BalsaAgent

import sys
from balsa.models.treeconv import make_and_featurize_trees

def debug_single_query(agent, model, query_filename):
    """Finds, preprocesses, featurizes, and runs a single query through the model."""
    target_node = None
    # Use agent.all_nodes to find the query before it's split into train/test
    for node in agent.all_nodes:
        if node.info['query_name'] + '.sql' == query_filename:
            target_node = node
            break
    
    if target_node is None:
        print(f"Could not find query '{query_filename}' in the workload.")
        return

    print(f"--- Preparing to debug query: {query_filename} ---")

    # --- THIS IS THE CRITICAL FIX ---
    # Replicate the agent's normal preprocessing step.
    # This removes unary nodes and prevents the IndexError.
    print(f"Original node type: {target_node.node_type}. Filtering scans and joins...")
    target_node = plans_lib.FilterScansOrJoins(target_node)
    print(f"Node type after filtering: {target_node.node_type}.")
    # --- END OF FIX ---

    # 1. Featurize the Query
    query_vec = agent.exp.query_featurizer(target_node)
    query_tensor = torch.from_numpy(query_vec).float().unsqueeze(0)

    # 2. Featurize the (now correctly preprocessed) Plan
    plan_trees, indexes = make_and_featurize_trees([target_node], agent.exp.featurizer)

    # 3. Move tensors to the correct device
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    model.to(device) # Ensure model is on the right device
    query_tensor = query_tensor.to(device)
    plan_trees = plan_trees.to(device)
    indexes = indexes.to(device)

    # 4. Run the forward pass with the debug flag
    with torch.no_grad():
        model.eval()
        prediction = model(query_tensor, plan_trees, indexes, query_name=query_filename, debug=True)

def inspect_model_weights(model):
    """Prints summary statistics for the weights and biases of key modules."""
    print("\n" + "="*25 + " MODEL WEIGHT & BIAS INSPECTION " + "="*25)
    
    # We can inspect both the query MLP and the final output MLP
    sequential_modules = {
        "query_mlp": model.query_mlp,
        "out_mlp": model.out_mlp
    }

    for name, module in sequential_modules.items():
        print(f"\n--- Inspecting {name} ---")
        for i, layer in enumerate(module):
            # We only care about layers that have learnable parameters
            has_params = False
            if hasattr(layer, 'weight'):
                has_params = True
                weights = layer.weight.data
                print(f"  Layer {i} ({layer.__class__.__name__}):")
                print(f"    Weight - Shape: {list(weights.shape)}")
                print(f"    Weight - Stats: mean={weights.mean():.4f}, std={weights.std():.4f}, min={weights.min():.4f}, max={weights.max():.4f}")
                if torch.isnan(weights).any() or torch.isinf(weights).any():
                    print("    !!!!!! WARNING: Weights contain NaN or Inf !!!!!!")


            if hasattr(layer, 'bias') and layer.bias is not None:
                has_params = True
                biases = layer.bias.data
                # If it's the first param for the layer, print the layer info header
                if not hasattr(layer, 'weight'):
                     print(f"  Layer {i} ({layer.__class__.__name__}):")
                print(f"    Bias   - Shape: {list(biases.shape)}")
                print(f"    Bias   - Stats: mean={biases.mean():.4f}, std={biases.std():.4f}, min={biases.min():.4f}, max={biases.max():.4f}")
                if torch.isnan(biases).any() or torch.isinf(biases).any():
                    print("    !!!!!! WARNING: Biases contain NaN or Inf !!!!!!")

            if not has_params:
                print(f"  Layer {i} ({layer.__class__.__name__}): No learnable parameters.")

    print("\n" + "="*70 + "\n")

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

    inspect_model_weights(model)

    agent.prev_optimizer_state_dict = checkpoint.get('optimizer_state_dict', None)
    agent.test_query_dir = args.workload_dir
    planner = agent._MakePlanner(model, plans_dataset)
    
    # === START OF NEW DEBUGGING CODE ===
    
    # We need all nodes for the lookup, not just train/test splits
    agent.all_nodes = agent.workload.Queries(split='all')
    
    print("\n\n>>> STARTING SINGLE QUERY DIAGNOSTICS <<<")
    
    # Pick a few diverse queries from the JOB workload to inspect
    debug_queries = ['2a.sql', '17f.sql', '27a.sql']
    
    for q_file in debug_queries:
        debug_single_query(agent, model, q_file)

    print(">>> DIAGNOSTICS COMPLETE. Exiting script. <<<")
    sys.exit(0) # Stop the script here to avoid running the full workload
    
    # === END OF NEW DEBUGGING CODE ===

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