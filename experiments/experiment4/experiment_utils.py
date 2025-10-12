import os
import json
import numpy as np
from collections import defaultdict

OPTIMIZERS = ["NEO", "BAO", "LERO", "LOGER", "FASTgres"]
# Folders to ignore when parsing path for query/join info
IGNORE_PATH_PARTS = [
    "4.1", "4.2", "4.3", "4.4", ".ipynb_checkpoints",
    "synthetic", "added_index", "classic_qep.json"
] + OPTIMIZERS

# --- Core Helper Functions ---

def _parse_path(dirpath):
    """
    Parses a directory path to extract join level, query name, and selectivity.
    Returns a dictionary with the extracted info, or None if invalid.
    """
    parts = dirpath.split(os.sep)
    info = {'join_level': None, 'query_name': None, 'selectivity': None}
    
    is_synthetic = "synthetic" in parts
    
    for part in parts:
        if part.endswith("_joins"):
            try:
                info['join_level'] = int(part.split("_")[0])
            except (ValueError, IndexError):
                continue
        elif part.endswith("%"):
            info['selectivity'] = part
        # A part is the query name if it's not a known, structured directory
        elif part not in IGNORE_PATH_PARTS:
            info['query_name'] = part

    # Synthetic benchmark has a fixed query name pattern, not parsed from the path
    if is_synthetic and info['join_level']:
        info['query_name'] = f"movie_company_{info['join_level']-1}"
            
    if info['join_level'] and info['selectivity'] and info['query_name']:
        return info
    return None

def _load_json_safely(file_path):
    """Loads a JSON file, returning None on error."""
    if not os.path.exists(file_path):
        return None
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError, TypeError) as e:
        # print(f"Warning: Could not process {file_path}. Error: {e}")
        return None

def _get_plan_file_path(base_path, opt_name, query_name, selectivity):
    """
    Handles the inconsistent and messy logic of finding the correct plan file.
    This is the core fix for the bug in the previous version.
    """
    sel_val = selectivity[:-1]
    
    # Logic for synthetic benchmark (exp 4.1)
    if "synthetic" in base_path:
        if opt_name == "NEO":
            return os.path.join(base_path, opt_name, f"{query_name}_{sel_val}.sql_neo_qep.json")
        elif opt_name == "FASTgres":
            return os.path.join(base_path, opt_name, f"{query_name}_{sel_val}.sql_fastgres_plan.json")
        else: # BAO, LERO, LOGER
            return os.path.join(base_path, opt_name, f"{query_name}_{sel_val}_{opt_name.lower()}_plan.json")
            
    # Logic for added_index benchmark (exp 4.1)
    elif "added_index" in base_path:
        if opt_name == "FASTgres":
            return os.path.join(base_path, opt_name, f"{query_name}_{sel_val}.sql_fastgres_plan.json")
        elif opt_name == "NEO":
            # NEO has two different files for plan vs. execution time in this experiment
            # We check for both, preferring the one with execution time (_qep).
            qep_path = os.path.join(base_path, opt_name, f"{query_name}_{sel_val}.sql_neo_qep.json")
            plan_path = os.path.join(base_path, opt_name, f"{query_name}_{sel_val}.sql_neo_plan.json")
            return qep_path if os.path.exists(qep_path) else plan_path
        else: # BAO, LERO, LOGER
            return os.path.join(base_path, opt_name, f"{query_name}_{sel_val}_{opt_name.lower()}_plan.json")
    
    # Logic for operator composition (exp 4.2)
    elif "4.2" in base_path:
        if opt_name == "PostgreSQL":
             return os.path.join(base_path, "classic_qep.json")
        elif opt_name == "FASTgres":
            return os.path.join(base_path, opt_name, f"{query_name}.sql_fastgres_plan.json")
        elif opt_name == "NEO":
            return os.path.join(base_path, opt_name, f"{query_name}.sql_neo_plan.json")
        else: # BAO, LERO, LOGER
            return os.path.join(base_path, opt_name, f"{query_name}_{opt_name.lower()}_plan.json")
            
    return None

def _get_execution_time(data, optimizer=None):
    """Extracts execution time from various JSON plan structures."""
    if not data:
        return None
        
    # Handle BAO's specific format [prediction_info, plan_info]
    if optimizer == "BAO" and isinstance(data, list) and len(data) > 1:
        data = data[1]

    if isinstance(data, list) and data and isinstance(data[0], dict):
        return data[0].get("Execution Time")
    elif isinstance(data, dict):
        return data.get("Execution Time")
    return None

def count_join_operators(plan_data):
    join_counts = defaultdict(int)
    def traverse_plan(node):
        if not isinstance(node, dict): return
        node_type = node.get("Node Type", "")
        if "Nested Loop" in node_type: join_counts["Nested Loop"] += 1
        elif "Hash Join" in node_type: join_counts["Hash Join"] += 1
        elif "Merge Join" in node_type: join_counts["Merge Join"] += 1
        if "Plans" in node:
            for child in node["Plans"]: traverse_plan(child)

    def traverse_neo_plan(execution_plan):
        """Helper to traverse NEO's execution plan format"""
        for join_step in execution_plan:
            if isinstance(join_step, list) and len(join_step) >= 2:
                join_info = join_step[1]
                if isinstance(join_info, dict) and "join_type" in join_info:
                    join_type = join_info["join_type"]
                    # Standardize join type names to match PostgreSQL style
                    if "Hash" in join_type:
                        join_counts["Hash Join"] += 1
                    elif "Merge" in join_type:
                        join_counts["Merge Join"] += 1
                    elif "Nested" in join_type or "Loop" in join_type:
                        join_counts["Nested Loop"] += 1
                    else:
                        join_counts["Hash Join"] += 1
    
    plan_to_traverse = None
    if isinstance(plan_data, list) and plan_data: 
        if isinstance(plan_data[0], dict) and "Plan" in plan_data[0]:
            plan_to_traverse = plan_data[0].get("Plan", {})
        elif isinstance(plan_data[0], list):
            # NEO's execution plan is a list of steps
            traverse_neo_plan(plan_data)
            return dict(join_counts)
    elif isinstance(plan_data, dict): plan_to_traverse = plan_data.get("Plan", {})
    
    if plan_to_traverse:
        traverse_plan(plan_to_traverse)
        
    return dict(join_counts)

# --- Main Data Extraction Functions ---

def extract_scan_latencies(root_dir):
    """Extracts ground-truth latencies for index vs. sequential scans."""
    latencies = defaultdict(lambda: defaultdict(dict))
    
    for dirpath, _, filenames in os.walk(root_dir):
        path_info = _parse_path(dirpath)
        if not path_info:
            continue
            
        key = f"{path_info['join_level']}_joins_{path_info['query_name']}"
        selectivity = path_info['selectivity']

        for filename in ["index_scan_qep.json", "seq_scan_qep.json"]:
            if filename in filenames:
                scan_type = "index_scan" if "index" in filename else "seq_scan"
                file_path = os.path.join(dirpath, filename)
                data = _load_json_safely(file_path)
                exec_time = _get_execution_time(data)
                if exec_time is not None:
                    latencies[key][selectivity][scan_type] = exec_time
    return json.loads(json.dumps(latencies)) # Convert to regular dict

def _find_scan_in_plan(plan_node, relation_name, alias, column_name):
    """Recursively search for a target scan type in a query plan."""
    if isinstance(plan_node, dict):
        # Check if the current node is the one we're looking for
        is_target = (plan_node.get("Relation Name") == relation_name and 
                     plan_node.get("Alias") == alias)
        
        # For Bitmap Scans, look at the child node
        if plan_node.get("Node Type") == "Bitmap Heap Scan":
            return "index_scan" if is_target else None
            
        if is_target:
            # Check filter or index condition for the column name
            for cond_key in ["Filter", "Index Cond"]:
                if cond_key in plan_node:
                    condition = plan_node[cond_key]
                    if isinstance(condition, str) and column_name in condition:
                        node_type = plan_node.get("Node Type", "unknown").lower()
                        return "index_scan" if "index" in node_type or "bitmap" in node_type else "seq_scan"

        # Recurse into child plans
        if "Plans" in plan_node:
            for subplan in plan_node.get("Plans", []):
                result = _find_scan_in_plan(subplan, relation_name, alias, column_name)
                if result:
                    return result
    elif isinstance(plan_node, list):
         for item in plan_node:
            result = _find_scan_in_plan(item, relation_name, alias, column_name)
            if result:
                return result
    return None

def extract_optimizer_data(root_dir, scan_target=None):
    results = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))

    for dirpath, dirnames, filenames in os.walk(root_dir):
        path_info = _parse_path(dirpath)
        if not path_info:
            continue

        key = f"{path_info['join_level']}_joins_{path_info['query_name']}"

        # --- Add PostgreSQL ---
        if "classic_qep.json" in filenames:
            pg_file = os.path.join(dirpath, "classic_qep.json")
            pg_data = _load_json_safely(pg_file)
            if isinstance(pg_data, list):
                pg_data = pg_data[0]
            if pg_data:
                exec_time = _get_execution_time(pg_data)
                if exec_time is not None:
                    results[key]["PostgreSQL"][path_info['selectivity']]['exec_time'] = exec_time

                if scan_target:
                    plan_data = pg_data.get("Plan", pg_data)
                    scan_type = _find_scan_in_plan(plan_data, **scan_target)
                    results[key]["PostgreSQL"][path_info['selectivity']]['scan_decision'] = scan_type or "unknown"

        for opt_name in dirnames:
            if opt_name not in OPTIMIZERS:
                continue

            plan_file = _get_plan_file_path(dirpath, opt_name, path_info['query_name'], path_info['selectivity'])
            data = _load_json_safely(plan_file)
            if not data:
                continue
                
            # Store execution time
            exec_time = _get_execution_time(data, optimizer=opt_name)
            if exec_time is not None:
                results[key][opt_name][path_info['selectivity']]['exec_time'] = exec_time

            # Store scan decision if a target is provided
            if scan_target:
                plan_data = data[1] if opt_name == "BAO" and isinstance(data, list) else data
                # Get the first plan node to search for the scan
                if isinstance(plan_data, list) and len(plan_data) == 1:
                    plan_data = plan_data[0] if isinstance(plan_data[0], dict) else {}
                    
                if isinstance(plan_data, dict) and "Plan" in plan_data:
                    plan_data = plan_data["Plan"]
                scan_type = _find_scan_in_plan(plan_data, **scan_target)
                results[key][opt_name][path_info['selectivity']]['scan_decision'] = scan_type or "unknown"

    return json.loads(json.dumps(results))

def analyze_join_operators(root_dir, optimal_plans_dir=None):
    op_counts = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    exec_times = defaultdict(lambda: defaultdict(float))
    all_optimizers = {"PostgreSQL": "classic_qep.json", **{opt: opt for opt in OPTIMIZERS}}

    # This map is crucial for linking an optimal plan back to its join level.
    query_to_join_level = {}

    # --- First Pass: Process the main experiment data (as before) ---
    print("Processing main experiment directory...")
    for join_dir in os.listdir(root_dir):
        if not join_dir.endswith("_joins"): continue
        join_path = os.path.join(root_dir, join_dir)
        if not os.path.isdir(join_path): continue
        join_level = int(join_dir.split('_')[0])

        for query_dir in os.listdir(join_path):
            query_path = os.path.join(join_path, query_dir)
            if not os.path.isdir(query_path): continue
            
            # Populate our mapping
            query_to_join_level[query_dir] = join_level
            
            for opt_name, opt_dir in all_optimizers.items():
                plan_file = None
                if opt_name == "PostgreSQL":
                    plan_file = os.path.join(query_path, "classic_qep.json")
                elif os.path.isdir(os.path.join(query_path, opt_dir)):
                    if opt_name == "FASTgres": plan_file = os.path.join(query_path, opt_dir, f"{query_dir}.sql_fastgres_plan.json")
                    elif opt_name == "NEO": plan_file = os.path.join(query_path, opt_dir, f"{query_dir}.sql_neo_plan.json")
                    else: plan_file = os.path.join(query_path, opt_dir, f"{query_dir}_{opt_dir.lower()}_plan.json")
                    if not os.path.exists(plan_file):
                        print(f"Warning: Plan file does not exist: {plan_file}")
                        continue
                
                data = _load_json_safely(plan_file)
                if data:
                    plan_for_counting = data[1:] if opt_name == "BAO" and isinstance(data, list) else data
                    counts = count_join_operators(plan_for_counting)
                    for join_type, count in counts.items():
                        op_counts[join_level][opt_name][join_type] += count
                    
                    exec_time = _get_execution_time(data, optimizer=opt_name)
                    if exec_time is not None:
                        exec_times[(join_level, query_dir)][opt_name] = exec_time
    
    # --- Second Pass: Process the optimal plans directory (if provided) ---
    if optimal_plans_dir and os.path.isdir(optimal_plans_dir):
        print(f"Processing optimal plans directory: {optimal_plans_dir}")
        for dirpath, _, filenames in os.walk(optimal_plans_dir):
            for filename in filenames:
                if filename.endswith("_optimal_plan.json"):
                    query_name = filename.replace("_optimal_plan.json", "")
                    
                    # Use our map to find the correct join_level for this query
                    join_level = query_to_join_level.get(query_name)
                    if join_level is None:
                        # print(f"Warning: Could not find join level for optimal plan '{query_name}'. Skipping.")
                        continue
                        
                    plan_file = os.path.join(dirpath, filename)
                    data = _load_json_safely(plan_file)
                    
                    if data:
                        # Add counts for the 'Optimal' category
                        counts = count_join_operators(data)
                        for join_type, count in counts.items():
                            op_counts[join_level]['Optimal'][join_type] += count
                        
                        # Add execution time for the 'Optimal' category
                        exec_time = _get_execution_time(data)
                        if exec_time is not None:
                            exec_times[(join_level, query_name)]['Optimal'] = exec_time
    
    return json.loads(json.dumps(op_counts)), exec_times    

import pandas as pd

def collect_prediction_vs_actual_metrics(root_dir):
    """
    Collects predicted vs. actual latency metrics for Experiment 4.3.
    Navigates the structure: root_dir -> join_type -> join_level -> query.
    Returns a pandas DataFrame with all collected data.
    """
    data_list = []
    
    # Expected subdirectories for forced join types
    for join_type in ['hashjoin', 'mergejoin', 'nestloop']:
        join_type_dir = os.path.join(root_dir, join_type)
        if not os.path.isdir(join_type_dir):
            continue
            
        for join_level_dir in os.listdir(join_type_dir):
            if not join_level_dir.endswith('_joins'):
                continue
            
            join_level = int(join_level_dir.split('_')[0])
            join_level_path = os.path.join(join_type_dir, join_level_dir)
            
            for query_dir in os.listdir(join_level_path):
                query_path = os.path.join(join_level_path, query_dir)
                if not os.path.isdir(query_path):
                    continue
                
                metrics = {
                    'join_type': join_type,
                    'join_level': join_level,
                    'query': query_dir,
                    'latency': None, 'bao_latency': None, 'loger_latency': None,
                    'bao_prediction': None, 'loger_prediction': None, 'neo_prediction': None
                }
                
                # 1. Get classic PostgreSQL metrics (actual latency)
                classic_file = os.path.join(query_path, 'classic_qep.json')
                classic_data = _load_json_safely(classic_file)
                metrics['latency'] = _get_execution_time(classic_data)
                
                # 2. Get BAO metrics (predicted and actual latency)
                bao_file = os.path.join(query_path, 'BAO', f'{query_dir}_bao_plan.json')
                bao_data = _load_json_safely(bao_file)
                if isinstance(bao_data, list) and len(bao_data) > 1:
                    if isinstance(bao_data[0], dict):
                        metrics['bao_prediction'] = bao_data[0].get('Bao', {}).get('Bao prediction')
                    metrics['bao_latency'] = _get_execution_time(bao_data, optimizer='BAO')

                # 3. Get LOGER metrics (predicted and actual latency are in different files)
                loger_metrics_file = os.path.join(query_path, 'LOGER', f'{query_dir}_loger_metrics.json')
                loger_plan_file = os.path.join(query_path, 'LOGER', f'{query_dir}_loger_plan.json')
                
                loger_metrics_data = _load_json_safely(loger_metrics_file)
                if isinstance(loger_metrics_data, dict):
                    metrics['loger_prediction'] = loger_metrics_data.get('predicted_latency')

                loger_plan_data = _load_json_safely(loger_plan_file)
                metrics['loger_latency'] = _get_execution_time(loger_plan_data)

                # 4. Get NEO metrics (predicted latency)
                neo_metrics_file = os.path.join(query_path, 'NEO', f'{query_dir}.sql_neo_metrics.json')
                neo_data = _load_json_safely(neo_metrics_file)
                if isinstance(neo_data, list) and len(neo_data) > 0 and isinstance(neo_data[0], dict):
                    metrics['neo_prediction'] = neo_data[0].get('predicted_latency')
                elif isinstance(neo_data, dict):
                    metrics['neo_prediction'] = neo_data.get('predicted_latency')
                    
                # Convert all ms values to seconds for consistency (NEO is already in seconds)
                for key in ['latency', 'bao_latency', 'loger_latency', 'bao_prediction', 'loger_prediction']:
                    if metrics[key] is not None:
                        try:
                            metrics[key] = float(metrics[key]) / 1000
                        except (ValueError, TypeError):
                            metrics[key] = None # In case of bad data

                data_list.append(metrics)
    
    return pd.DataFrame(data_list)

import numpy as np

def _calculate_q_error(predicted, actual):
    """Internal helper to calculate Q-error, handling None and zero values."""
    if predicted is None or actual is None or actual == 0 or predicted == 0:
        return None
    try:
        # Ensure values are float for division
        predicted, actual = float(predicted), float(actual)
        return max(predicted / actual, actual / predicted)
    except (ValueError, TypeError):
        return None

def collect_latency_q_errors(root_dir):
    """
    Collects Q-errors for latency prediction vs. actual execution for Experiment 4.4.
    NOTE: The data source for this experiment is the directory structure from Experiment 4.2.
    
    Args:
        root_dir (str): The path to the experiment directory (e.g., .../experiment4/4.2).
        
    Returns:
        A dictionary of Q-error values: {join_level: {optimizer: [q_error1, ...]}}
    """
    q_errors = defaultdict(lambda: defaultdict(list))
    
    for join_level_dir in os.listdir(root_dir):
        if not join_level_dir.endswith('_joins'):
            continue
            
        join_level = int(join_level_dir.split('_')[0])
        if join_level > 16:
            continue
            
        join_level_path = os.path.join(root_dir, join_level_dir)
        
        for query_dir in os.listdir(join_level_path):
            query_path = os.path.join(join_level_path, query_dir)
            if not os.path.isdir(query_path):
                continue

            # --- Process BAO predictions ---
            bao_file = os.path.join(query_path, 'BAO', f'{query_dir}_bao_plan.json')
            bao_data = _load_json_safely(bao_file)
            if isinstance(bao_data, list) and len(bao_data) > 1:
                bao_pred = bao_data[0].get('Bao', {}).get('Bao prediction')
                bao_actual = _get_execution_time(bao_data, optimizer='BAO')
                q_error = _calculate_q_error(bao_pred, bao_actual)
                if q_error: q_errors[join_level]['BAO'].append(q_error)

            # --- Process LOGER predictions ---
            loger_metrics_file = os.path.join(query_path, 'LOGER', f'{query_dir}_loger_metrics.json')
            loger_plan_file = os.path.join(query_path, 'LOGER', f'{query_dir}_loger_plan.json')
            loger_pred_data = _load_json_safely(loger_metrics_file)
            loger_actual_data = _load_json_safely(loger_plan_file)
            if loger_pred_data and loger_actual_data:
                loger_pred = loger_pred_data.get('predicted_latency')
                loger_actual = _get_execution_time(loger_actual_data)
                q_error = _calculate_q_error(loger_pred, loger_actual)
                if q_error: q_errors[join_level]['LOGER'].append(q_error)
            
            # --- Process NEO predictions ---
            neo_metrics_file = os.path.join(query_path, 'NEO', f'{query_dir}.sql_neo_metrics.json')
            neo_data = _load_json_safely(neo_metrics_file)
            if neo_data:
                neo_pred = neo_data.get('predicted_latency')
                # NEO's prediction is in seconds, while actual is in ms. Must convert.
                if neo_pred is not None: neo_pred *= 1000
                neo_actual = neo_data.get('actual_latency')
                q_error = _calculate_q_error(neo_pred, neo_actual)
                if q_error: q_errors[join_level]['NEO'].append(q_error)
    
    return json.loads(json.dumps(q_errors))

from scipy.spatial.distance import cosine
import re

def _parse_path_for_analysis(path):
    """
    Parses a directory path to extract the run ID and query name.
    Example path: '.../run1/1a'
    """
    info = {'run_id': 'unknown', 'query_name': 'unknown'}
    parts = path.split(os.sep)
    
    # Find 'runX'
    run_match = re.search(r'run(\d+)', path)
    if run_match:
        info['run_id'] = run_match.group(0)
    
    # Query name is typically the last meaningful directory
    if len(parts) > 0:
        info['query_name'] = parts[-1]
        
    return info

from tqdm import tqdm

def load_embedding_analysis_data(workload_dir):
    """
    Walks the workload directory recursively to find all BAO/ subdirectories and
    loads the four generated files for each query to calculate divergence metrics.
    
    Args:
        workload_dir (str): The root directory of the workload.

    Returns:
        pd.DataFrame: A DataFrame with query_name, embedding_distance, and speedup_factor.
    """
    analysis_results = []
    
    # Use os.walk to find all subdirectories named "BAO". This is robust.
    bao_dirs_to_process = []
    for root, dirs, files in os.walk(workload_dir):
        if "BAO" in dirs:
            # 'root' is the query's parent directory (e.g., '.../run1/1a')
            bao_dir_path = os.path.join(root, "BAO")
            bao_dirs_to_process.append((root, bao_dir_path, files))
            
    if not bao_dirs_to_process:
        print(f"Error: No 'BAO' subdirectories found under '{workload_dir}'. Please check the path.")
        return pd.DataFrame()

    print(f"Found {len(bao_dirs_to_process)} queries with BAO results to analyze.")
    
    for root, bao_dir, files in tqdm(bao_dirs_to_process, desc="Processing Queries"):
        # Find the original .sql file in the parent directory
        query_file = next((f for f in files if f.endswith('.sql')), None)
        if not query_file:
            continue
        
        query_name_base = query_file.replace('.sql', '')
        
        # Parse path for context like run_id and query_name
        path_info = _parse_path_for_analysis(root)

        # Load the four essential files
        bao_plan = _load_json_safely(os.path.join(bao_dir, f'{query_name_base}_bao_plan.json'))
        bao_emb = _load_json_safely(os.path.join(bao_dir, f'{query_name_base}_bao_embedding.json'))
        pg_plan = _load_json_safely(os.path.join(bao_dir, f'{query_name_base}_postgres_plan.json'))
        pg_emb = _load_json_safely(os.path.join(bao_dir, f'{query_name_base}_postgres_embedding.json'))
        
        if not all([bao_plan, bao_emb, pg_plan, pg_emb]):
            continue

        # Extract data and calculate metrics
        bao_vec = np.array(bao_emb['embedding'])
        pg_vec = np.array(pg_emb['embedding'])

        if bao_vec.ndim == 0 or pg_vec.ndim == 0: continue
            
        embedding_distance = cosine(bao_vec, pg_vec)
        bao_total_time = _get_execution_time(bao_plan, optimizer='BAO')
        pg_total_time = _get_execution_time(pg_plan, optimizer='BAO')

        speedup = (pg_total_time / bao_total_time) if bao_total_time and pg_total_time and bao_total_time > 0 else None

        analysis_results.append({
            'query_name': path_info['query_name'],
            'run_id': path_info['run_id'],
            'embedding_distance': embedding_distance,
            'speedup_factor': speedup,
            'bao_time_ms': bao_total_time,
            'pg_time_ms': pg_total_time
        })
            
    return pd.DataFrame(analysis_results)

def load_all_optimizer_data(root_dir):
    """
    Recursively finds all optimizer subdirectories (BAO, LOGER, etc.),
    loads their four analysis files, and computes divergence metrics.
    """
    all_results = []

    print(f"Starting recursive search for optimizer data in: {root_dir}")
    
    for optimizer_name in OPTIMIZERS:
        print(f"\n--- Searching for {optimizer_name} results... ---")
        
        # Find all subdirectories with the current optimizer's name
        optimizer_dirs = [os.path.join(root, optimizer_name) 
                          for root, dirs, _ in os.walk(root_dir) if optimizer_name in dirs]
        
        if not optimizer_dirs:
            print(f"No '{optimizer_name}' subdirectories found. Skipping.")
            continue
        
        print(f"Found {len(optimizer_dirs)} queries with {optimizer_name} results to analyze.")

        for opt_dir in tqdm(optimizer_dirs, desc=f"Processing {optimizer_name}"):
            query_dir = os.path.dirname(opt_dir)
            query_file = next((f for f in os.listdir(query_dir) if f.endswith('.sql')), None)
            if not query_file: continue
            
            query_name_base = query_file.replace('.sql', '')
            path_info = _parse_path_for_analysis(query_dir)

            # --- Load the four essential files for this optimizer ---
            opt_plan = _load_json_safely(os.path.join(opt_dir, f'{query_name_base}_{optimizer_name.lower()}_plan.json'))
            opt_emb = _load_json_safely(os.path.join(opt_dir, f'{query_name_base}_{optimizer_name.lower()}_embedding.json'))
            pg_plan = _load_json_safely(os.path.join(opt_dir, f'{query_name_base}_postgres_plan.json'))
            pg_emb = _load_json_safely(os.path.join(opt_dir, f'{query_name_base}_postgres_embedding.json'))
            
            if not all([opt_plan, opt_emb, pg_plan, pg_emb]): continue
            
            # --- Extract data and compute metrics ---
            opt_vec_data = opt_emb.get('embedding')
            pg_vec_data = pg_emb.get('embedding')

            if opt_vec_data is None or pg_vec_data is None: continue
            
            opt_vec = np.array(opt_vec_data)
            pg_vec = np.array(pg_vec_data)

            if opt_vec.ndim == 0 or pg_vec.ndim == 0: continue
            
            embedding_distance = cosine(opt_vec, pg_vec) if opt_vec.shape == pg_vec.shape else np.nan
            opt_total_time = _get_execution_time(opt_plan, optimizer=optimizer_name)
            pg_total_time = _get_execution_time(pg_plan, optimizer=optimizer_name)

            speedup = (pg_total_time / opt_total_time) if opt_total_time and pg_total_time and opt_total_time > 0 else None

            all_results.append({
                'optimizer': optimizer_name,
                'query_name': path_info['query_name'],
                'run_id': path_info['run_id'],
                'embedding_distance': embedding_distance,
                'speedup_factor': speedup,
                'opt_time_ms': opt_total_time,
                'pg_time_ms': pg_total_time
            })
            
    return pd.DataFrame(all_results)

from sklearn.manifold import TSNE # <--- NEW IMPORT

def load_and_transform_for_tsne(root_dir, perplexity=30, n_iter=1000):
    """
    Loads all embeddings for all optimizers, runs t-SNE, and returns a
    DataFrame ready for plotting.
    """
    all_embeddings_data = []

    for optimizer_name in OPTIMIZERS:
        optimizer_dirs = [os.path.join(root, optimizer_name) 
                          for root, dirs, _ in os.walk(root_dir) if optimizer_name in dirs]
        
        if not optimizer_dirs: continue

        for opt_dir in tqdm(optimizer_dirs, desc=f"Loading {optimizer_name} embeddings"):
            query_dir = os.path.dirname(opt_dir)
            query_file = next((f for f in os.listdir(query_dir) if f.endswith('.sql')), None)
            if not query_file: continue
            
            query_name_base = query_file.replace('.sql', '')
            
            # Load the optimizer's embedding and the PG embedding
            opt_emb_data = _load_json_safely(os.path.join(opt_dir, f'{query_name_base}_{optimizer_name.lower()}_embedding.json'))
            pg_emb_data = _load_json_safely(os.path.join(opt_dir, f'{query_name_base}_postgres_embedding.json'))
            opt_plan_data = _load_json_safely(os.path.join(opt_dir, f'{query_name_base}_{optimizer_name.lower()}_plan.json'))
            pg_plan_data = _load_json_safely(os.path.join(opt_dir, f'{query_name_base}_postgres_plan.json'))

            if not all([opt_emb_data, pg_emb_data, opt_plan_data, pg_plan_data]): continue
            
            # Calculate speedup to use for coloring the points
            opt_time = _get_execution_time(opt_plan_data, optimizer=optimizer_name)
            pg_time = _get_execution_time(pg_plan_data, optimizer=optimizer_name)
            speedup = (pg_time / opt_time) if opt_time and pg_time and opt_time > 0 else 1.0

            if opt_emb_data.get('embedding'):
                all_embeddings_data.append({
                    'optimizer': optimizer_name,
                    'plan_type': optimizer_name,
                    'embedding': opt_emb_data['embedding'],
                    'speedup': speedup,
                    'query_name': query_name_base
                })
            if pg_emb_data.get('embedding'):
                all_embeddings_data.append({
                    'optimizer': optimizer_name, # Grouped with the optimizer it was evaluated by
                    'plan_type': 'PostgreSQL',
                    'embedding': pg_emb_data['embedding'],
                    'speedup': speedup, # The PG plan doesn't have a speedup, but we use the query's result for context
                    'query_name': query_name_base
                })

    if not all_embeddings_data:
        print("No valid embeddings found to process for t-SNE.")
        return pd.DataFrame()

    df_emb = pd.DataFrame(all_embeddings_data)
    
    # --- Run t-SNE Transformation ---
    # We run t-SNE separately for each optimizer's embedding space
    tsne_results = []
    for optimizer_name, group in df_emb.groupby('optimizer'):
        print(f"\nRunning t-SNE for {optimizer_name}...")
        
        # Check for consistent embedding dimensions within the group
        dim = len(group['embedding'].iloc[0])
        if not all(len(emb) == dim for emb in group['embedding']):
            print(f"  - WARNING: Inconsistent embedding dimensions for {optimizer_name}. Skipping t-SNE.")
            continue
        
        # Filter out FASTgres as its vectors are not comparable embeddings
        if optimizer_name == 'FASTgres':
            print(f"  - Skipping t-SNE for FASTgres as its vectors are feature inputs, not learned embeddings.")
            continue
            
        embeddings_matrix = np.array(group['embedding'].tolist())
        
        tsne = TSNE(n_components=2, perplexity=min(perplexity, len(embeddings_matrix) - 1), 
                    n_iter=n_iter, random_state=42, init='pca', learning_rate='auto')
        
        tsne_coords = tsne.fit_transform(embeddings_matrix)
        
        # Add coordinates back to the group DataFrame
        group['tsne_x'] = tsne_coords[:, 0]
        group['tsne_y'] = tsne_coords[:, 1]
        tsne_results.append(group)

    if not tsne_results:
        return pd.DataFrame()
        
    return pd.concat(tsne_results, ignore_index=True)

