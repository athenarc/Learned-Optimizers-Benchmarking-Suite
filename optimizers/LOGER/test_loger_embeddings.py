import os
import torch
import json
from tqdm import tqdm
from lib.log import Logger
from core import database, Sql, Plan # Assuming 'sql' module contains the Sql class
from model.dqn import DeepQNet
import argparse
import numpy as np
import traceback

# --- Configuration ---
DEVICE = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

# --- Helper Functions (from previous versions) ---
log = Logger(f'log/generate_artifacts.log', buffering=1, stderr=True)

def find_sql_files(directory, skip_loger_processed=False):
    sql_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.sql'):
                if skip_loger_processed and os.path.exists(os.path.join(root, "LOGER")):
                    continue
                sql_files.append(os.path.join(root, file))
    return sql_files

def load_sql_objects(directory, skip_loger_processed=False):
    sql_file_paths = find_sql_files(directory, skip_loger_processed=skip_loger_processed)
    print(f"Found {len(sql_file_paths)} SQL files to process.")
    sql_objects = []
    for sql_file in tqdm(sql_file_paths, desc="Loading SQL files and computing baselines"):
        try:
            with open(sql_file, 'r') as f: query = f.read().strip()
            if not query: continue
            sql = Sql(query, database.config.feature_length, filename=sql_file)
            sql.to(DEVICE)
            sql_objects.append(sql)
        except Exception as e:
            print(f"Error loading {sql_file}: {e}")
    return sql_objects

def initialize_db(args):
    try:
        database.setup(dbname=args.database, user=args.user, password=args.password, host=args.host, port=args.port, cache=False)
        database.config.bushy = True
        log("Database connection successful.")
    except Exception as e:
        log(f"Error setting up database: {e}")
        raise RuntimeError("Database connection failed.")

def load_loger_model(model_path):
    print(f"Loading LOGER model from: {model_path}")
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Model file not found: {model_path}")
    checkpoint = torch.load(model_path, map_location=DEVICE)
    model = DeepQNet(
        device=DEVICE, half=200, out_dim=4, num_table_layers=1,
        use_value_predict=False, restricted_operator=True,
        reward_weighting=0.1, log_cap=1.0
    )
    model.model_recover(checkpoint['model'])
    model.eval_mode()
    print("LOGER model loaded successfully.")
    return model

def get_plan_json(query_string):
    try:
        plan_json = database.plan_latency(query_string, cache=False)
        return plan_json[0][0][0]
    except Exception as e:
        log(f"Failed to get plan JSON for query '{query_string[:70]}...': {e}")
        return {"error": str(e)}

def save_json(data, output_path):
    try:
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=4)
    except Exception as e:
        log(f"Failed to save JSON to {output_path}: {e}")

def get_embedding_for_pg_plan(model: DeepQNet, sql: Sql):
    """
    Definitive, Corrected Implementation. Replicates the exact state context of
    beam_plan's final embedding calculation, including the creation of all
    necessary intermediate embeddings.
    """
    if not hasattr(sql, 'baseline') or not sql.baseline or not sql.baseline.join_order:
        log(f"  - WARNING: No baseline join order found for {sql.filename}.")
        return None

    join_order = sql.baseline.join_order
    join_methods_baseline = sql.baseline.join_methods

    if len(join_order) == 0:
        return None
    if len(join_order) != len(join_methods_baseline):
        log(f"  - WARNING: Mismatch in length for {sql.filename}. Using defaults.")
        join_methods_baseline = [-1] * len(join_order)

    # 1. Initialize a fresh plan
    plan = Plan(sql)
    model.init(plan)

    # 2. Build the plan up to the SECOND-TO-LAST step.
    for i in range(len(join_order) - 1):
        join_action = join_order[i]
        baseline_method_code = join_methods_baseline[i]
        
        plan_join_method = Plan.NEST_LOOP_JOIN
        if baseline_method_code == 1: plan_join_method = Plan.HASH_JOIN
        elif baseline_method_code == 2: plan_join_method = Plan.MERGE_JOIN

        left_alias, right_alias = join_action
        left_emb = plan.root_node_emb(left_alias)
        right_emb = plan.root_node_emb(right_alias)
        parent_emb = model.model_step2(left_emb, right_emb, input=None)
        parent_index = plan.join(left_alias, right_alias, join_method=plan_join_method)
        plan.root_node_emb_set(parent_index, parent_emb)

    # `plan` is now the "almost_complete_plan"
    almost_complete_plan = plan

    # 3. Get the details of the FINAL join action
    final_join_action = join_order[-1]
    final_method_code = join_methods_baseline[-1]
    
    plan_join_method_final = Plan.NEST_LOOP_JOIN
    if final_method_code == 1: plan_join_method_final = Plan.HASH_JOIN
    elif final_method_code == 2: plan_join_method_final = Plan.MERGE_JOIN
        
    last_left_alias, last_right_alias = final_join_action

    # 4. Get the embeddings of the children from the ALMOST complete plan
    left_emb = almost_complete_plan.root_node_emb(last_left_alias)
    right_emb = almost_complete_plan.root_node_emb(last_right_alias)

    # 5. Create a temporary FULLY completed plan AND CALCULATE ITS FINAL EMBEDDING
    temp_complete_plan = almost_complete_plan.clone(deep=True)
    
    # --- THIS IS THE FIX ---
    # a) Calculate the embedding for the final parent node
    final_parent_emb = model.model_step2(left_emb, right_emb, input=None)
    # b) Perform the final join
    final_parent_index = temp_complete_plan.join(last_left_alias, last_right_alias, join_method=plan_join_method_final)
    # c) Store the final parent's embedding in the temporary plan
    temp_complete_plan.root_node_emb_set(final_parent_index, final_parent_emb)
    # --- END FIX ---

    # 6. Assemble the inputs for model_tail, sourcing from the correct states
    
    # These now come from the `temp_complete_plan` which has all embeddings populated
    global_emb_from_complete = temp_complete_plan.root_node_emb_all_hidden()
    state_join_encodings = temp_complete_plan.join_encodings()
    
    # This is re-calculated from the almost-complete children, mimicking beam_plan
    parent_emb_for_tail = model.model_step2(left_emb, right_emb, input=None)
    parent_emb_for_tail = parent_emb_for_tail.chunk(2, dim=-1)[0]
    
    _len = len(sql.from_tables)
    global_emb_agg = model.model_tail.aggregate(global_emb_from_complete)

    # 7. Call model_tail with the correctly sourced and complete components
    _, final_embedding = model.model_tail(
        global_emb_agg.unsqueeze(0),
        torch.tensor([_len], device=DEVICE),
        parent_emb_for_tail.unsqueeze(0),
        left_emb.chunk(2, dim=-1)[0].unsqueeze(0),
        right_emb.chunk(2, dim=-1)[0].unsqueeze(0),
        state_join_encodings.unsqueeze(0),
        return_embedding=True
    )
    
    return final_embedding.squeeze(0)

# --- MAIN EXECUTION LOGIC ---

def generate_analysis_artifacts(model, sql_objects):
    """
    Main loop to generate the four analysis files for each query.
    """
    with torch.no_grad():
        for sql in tqdm(sql_objects, desc="Generating Analysis Artifacts"):
            sql_path = sql.filename
            query_name_base = os.path.splitext(os.path.basename(sql_path))[0]
            loger_dir = os.path.join(os.path.dirname(sql_path), "LOGER")
            os.makedirs(loger_dir, exist_ok=True)
            
            print(f"\nProcessing: {sql.filename}")

            try:
                
                # --- 1. GET POSTGRESQL PLAN AND ITS EMBEDDING ---
                print("  - Generating PostgreSQL plan and embedding...")
                postgres_plan_json = get_plan_json(sql.sql)
                
                # Use our new helper to get the embedding for the PG plan
                pg_embedding = get_embedding_for_pg_plan(model, sql)

                if pg_embedding is not None:
                    pg_embedding_data = {"embedding": pg_embedding.cpu().numpy().tolist()}
                else:
                    print("  - WARNING: Could not generate PG embedding. Using placeholder.")
                    pg_embedding_data = {"embedding": None, "error": "Failed to reconstruct PG plan or get baseline."}
                
                # --- 2. GET LOGER's BEST PLAN AND EMBEDDING ---
                print("  - Generating LOGER plan and embedding...")
                loger_plan_obj, _, _, _, loger_embedding = model.beam_plan(
                    sql, bushy=False, judge=False, return_prediction=True, return_embedding=True
                )
                loger_plan_json = get_plan_json(str(loger_plan_obj))
                
                save_json(loger_plan_json, os.path.join(loger_dir, f"{query_name_base}_loger_plan.json"))
                save_json({"embedding": loger_embedding.cpu().numpy().tolist()}, 
                          os.path.join(loger_dir, f"{query_name_base}_loger_embedding.json"))
                print("  - LOGER files saved.")

                save_json(postgres_plan_json, os.path.join(loger_dir, f"{query_name_base}_postgres_plan.json"))
                save_json(pg_embedding_data, os.path.join(loger_dir, f"{query_name_base}_postgres_embedding.json"))
                print("  - PostgreSQL files saved.")

            except Exception as e:
                print(f"  - FATAL ERROR processing {sql.filename}: {e}")
                traceback.print_exc()
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
    parser = argparse.ArgumentParser(description='Generate LOGER and PostgreSQL analysis artifacts (plans and embeddings).')
    parser.add_argument('workload_path', type=str, help='Path to workload directory with .sql files')
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
    parser.add_argument('--checkpoint_path', type=str, required=True,
                        help='Path to the trained LOGER model file (.pkl)')
    args = parser.parse_args()

    if not os.getenv("DB_PORT"):  # only adjust if not globally defined
        if args.database in db_port_map and db_port_map[args.database]:
            args.port = int(db_port_map[args.database])
        elif "stack" in args.database.lower():
            args.port = int(os.getenv("STACK_PORT", "5432"))
        else:
            args.port = 5432

    initialize_db(args)
    model = load_loger_model(args.model_path)
    sql_objects = load_sql_objects(args.workload_path, skip_loger_processed=args.skip_loger_processed)
    generate_analysis_artifacts(model, sql_objects)
    print("\nProcessing complete.")