import os
import argparse
import torch
import numpy as np
import json
from tqdm import tqdm
import balsa
from balsa.util import plans_lib
from balsa.util import postgres
from run import BalsaAgent # We use the agent to set up the environment

def save_json(data, output_path):
    """Safely saves data to a JSON file."""
    try:
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=4)
    except Exception as e:
        print(f"  - WARNING: Failed to save JSON to {output_path}: {e}")

def get_postgres_plan_json(sql_str):
    """Executes a raw SQL string to get the default PostgreSQL EXPLAIN ANALYZE plan."""
    try:
        # We pass no hint_str to get the default plan
        result_tup = postgres.ExplainAnalyzeSql(sql_str, geqo_off=True, remote=False)
        if not result_tup.has_timeout:
            return result_tup.result[0][0][0]
        else:
            return {"error": "PostgreSQL execution timed out."}
    except Exception as e:
        return {"error": f"Failed to get PostgreSQL plan: {e}"}

def get_balsa_plan_json(sql_str, hint_str):
    """Executes a SQL string with a Balsa hint to get its EXPLAIN ANALYZE plan."""
    try:
        result_tup = postgres.ExplainAnalyzeSql(sql_str, comment=hint_str, geqo_off=True, remote=False)
        if not result_tup.has_timeout:
            return result_tup.result[0][0][0]
        else:
            return {"error": "Balsa plan execution timed out."}
    except Exception as e:
        return {"error": f"Failed to execute Balsa plan: {e}"}

def generate_analysis_artifacts(agent, model, planner):
    """
    Main loop to generate the four analysis files for each query.
    """
    print(f"\nProcessing {len(agent.test_nodes)} queries from the test set...")
    postgres.DropBufferCache()
    
    with torch.no_grad():
        for query_node in tqdm(agent.test_nodes, desc="Generating Artifacts"):
            query_name = query_node.info['query_name']
            query_sql = query_node.info['sql_str']
            query_base_dir = os.path.dirname(query_node.info['path'])
            output_dir = os.path.join(query_base_dir, "NEO")
            os.makedirs(output_dir, exist_ok=True)
            
            print(f"\nProcessing: {query_name}")

            try:
                # --- 1. GET BALSA PLAN AND EMBEDDING ---
                print("  - Generating Balsa plan and embedding...")
                # The planner finds the best plan according to the model
                planning_time, balsa_plan_node, predicted_latency, found_plans = planner.plan(
                    query_node,
                    agent.params.search_method,
                    bushy=agent.params.bushy,
                    return_all_found=True
                )

                predicted_latency, balsa_plan_node = agent.SelectPlan(
                    found_plans, predicted_latency, balsa_plan_node, planner, query_node)

                # Immediately after planning, the model's 'final_embedding' attribute is set.
                planner.infer(query_node, [balsa_plan_node])
                balsa_embedding = model.model.final_embedding[0].tolist()
                
                # Execute the Balsa plan to get full timing info
                balsa_hint_str = balsa_plan_node.hint_str(with_physical_hints=agent.params.plan_physical)
                balsa_plan_json = get_balsa_plan_json(query_sql, balsa_hint_str)

                save_json(balsa_plan_json, os.path.join(output_dir, f"{query_name}_neo_plan.json"))
                save_json({"embedding": balsa_embedding}, os.path.join(output_dir, f"{query_name}_neo_embedding.json"))
                print("  - Balsa/NEO files saved.")

                # --- 2. GET POSTGRESQL PLAN AND EMBEDDING ---
                print("  - Generating PostgreSQL plan and embedding...")
                # Get the plan JSON with timing by executing the raw SQL
                postgres_plan_json = get_postgres_plan_json(query_sql)
                
                # To get the embedding for the PG plan, we run inference on the original query_node.
                # The `infer` method triggers a forward pass, which updates the final_embedding.
                planner.infer(query_node, [query_node])
                postgres_embedding = model.model.final_embedding[0].tolist()
                
                save_json(postgres_plan_json, os.path.join(output_dir, f"{query_name}_postgres_plan.json"))
                save_json({"embedding": postgres_embedding}, os.path.join(output_dir, f"{query_name}_postgres_embedding.json"))
                print("  - PostgreSQL files saved.")

            except Exception as e:
                print(f"  - FATAL ERROR processing {query_name}: {e}")
                import traceback
                traceback.print_exc()
                continue

def main(args):
    # Load Balsa parameters for the specified experiment run
    print(f"Loading Balsa parameters for '{args.run}' experiment.")
    balsa_params = balsa.params_registry.Get(args.run)
    balsa_params.use_local_execution = True
    balsa_params.sim = None # No simulation needed for evaluation
    balsa_params.initial_timeout_ms = 3 * 60 * 1000 # Set timeout for training queries to 3 minutes
    balsa_params.init_experience = "" # We are not using pre-collected experience
    balsa_params.explore_visit_counts_sort = False

    # Override workload directory if specified
    if args.workload_dir:
        # balsa_params.query_dir = args.workload_dir
        balsa_params.test_query_dir = args.workload_dir
        # balsa_params.query_glob = ['*.sql']
        # balsa_params.test_query_glob = ['*.sql']

    # --- Setup Balsa Agent and Environment ---
    # The agent handles database connections, workload loading, and featurization
    agent = BalsaAgent(balsa_params)
    agent.curr_value_iter = 0 # Set a default value
    agent.num_query_execs = 0
    agent.num_total_timeouts = 0
    agent.overall_best_train_latency = np.inf
    agent.overall_best_test_latency = np.inf
    agent.overall_best_test_swa_latency = np.inf
    agent.overall_best_test_ema_latency = np.inf

    agent.train_nodes = plans_lib.FilterScansOrJoins(agent.train_nodes)
    agent.test_nodes = plans_lib.FilterScansOrJoins(agent.test_nodes)

    # Create a dummy dataset; we only need its featurizers and stats, not for training
    train_ds, _, _, _ = agent._MakeDatasetAndLoader(log=False)
    plans_dataset = train_ds.dataset if isinstance(
            train_ds, torch.utils.data.Subset) else train_ds
    
    # --- Load the Pre-trained Model ---
    model_checkpoint = os.path.join(args.checkpoint_dir, 'checkpoint.pt')
    checkpoint_metadata = os.path.join(args.checkpoint_dir, 'checkpoint-metadata.txt')
    with open(checkpoint_metadata, 'r') as f:
        metadata = f.read().strip()

    print("Loading pre-trained model...")
    model = agent._MakeModel(plans_dataset)
    ckpt = torch.load(model_checkpoint)
    model.load_state_dict(ckpt)
    agent.model = model
    agent.curr_value_iter = int(metadata.split(',')[1])
    agent.prev_optimizer_state_dict = ckpt.get('optimizer_state_dict', None)
    print(f"Loaded model checkpoint from {model_checkpoint}")

    # --- NEW: WEIGHT VALIDATION BLOCK ---
    print("\n--- Validating Model Weights ---")
    has_nan_weight = False
    for name, param in model.named_parameters():
        print(f"Checking parameter: {name}")
        print(f"  - Shape: {param.shape}, dtype: {param.dtype}")
        print(f"  - NaN count: {torch.isnan(param).sum().item()}, Inf count: {torch.isinf(param).sum().item()}")
        if torch.isnan(param).any():
            print(f"!!! ERROR: NaN detected in model parameter: {name}")
            has_nan_weight = True
        if torch.isinf(param).any():
            print(f"!!! ERROR: Infinity detected in model parameter: {name}")
            has_nan_weight = True
    
    if not has_nan_weight:
        print("Model weights appear to be valid (no NaNs or Infs).")
    else:
        print("!!! CRITICAL ERROR: Model weights are corrupt. Aborting. !!!")
        return # Exit the script if weights are bad
    print("----------------------------\n")
    # --- END OF VALIDATION BLOCK ---

    # The planner links the model to the query featurization logic
    planner = agent._MakePlanner(model, plans_dataset)

    # --- Run the main artifact generation loop ---
    generate_analysis_artifacts(agent, model, planner)
    
    print("\nProcessing complete.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Generate Balsa and PostgreSQL analysis artifacts (plans and embeddings).")
    parser.add_argument('--checkpoint_dir', type=str, required=True, help='Path to the Balsa model checkpoint directory.')
    parser.add_argument('--run', type=str, required=True, help='Name of the balsa experiment config to use (e.g., Balsa_JOBRandSplit).')
    parser.add_argument('--workload_dir', type=str, default=None, help='Directory containing the SQL workload files to process.')
    args = parser.parse_args()

    main(args)