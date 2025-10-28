import argparse
import os
import socket
from config import *
from multiprocessing import Pool
import json
from pathlib import Path
import subprocess
from utils import * # Assuming a utils.py with do_run_query, create_training_file, etc.
from tqdm import tqdm
from time import sleep, time
import signal
import socket

# Set the directory where the LERO server script is located, which is the parent of this script
LERO_SERVER_DIR = Path(__file__).resolve().parent.parent

def kill_lero_server():
    """Forcefully terminates any running LERO server process."""
    print("--- Killing LERO server process ---")
    subprocess.run(['pkill', '-9', '-f', 'server.py'], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    sleep(2)

    try:
        result = subprocess.run(
            ["ss", "-ltnp"],
            capture_output=True,
            text=True,
            check=True
        )
        lines = [line for line in result.stdout.splitlines() if f":{LERO_SERVER_PORT} " in line]

        if not lines:
            # Fall back to pkill if ss finds nothing
            subprocess.run(['pkill', '-9', '-f', 'server.py'],
                           check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            print("No process found via ss, attempted pkill fallback.")
            return

        for line in lines:
            if "pid=" in line:
                pid_str = line.split("pid=")[1].split(",")[0]
                pid = int(pid_str)
                try:
                    os.kill(pid, signal.SIGKILL)
                    print(f"Killed LERO server process PID {pid} on port {LERO_SERVER_PORT}")
                except ProcessLookupError:
                    print(f"Process {pid} not found (already exited?)")

    except subprocess.CalledProcessError as e:
        print(f"Error running ss: {e.stderr}")
    except Exception as e:
        print(f"Unexpected error while killing LERO server: {e}")
    
    sleep(2)

def start_lero_server(server_script_dir, model_path=None):
    """
    Starts the LERO server. The server.conf file is always created or overwritten.
    If a model_path is provided, the ModelPath key is set.
    Otherwise, the ModelPath key is commented out.
    """
    config_path = os.path.join(server_script_dir, "server.conf")
    config_content = ""

    if model_path:
        print(f"--- Starting LERO server with model from: {model_path} ---")
        # Create config content with an active ModelPath
        config_content = f"""[lero]
Port = {LERO_SERVER_PORT}
ListenOn = {LERO_SERVER_HOST}
ModelPath = {os.path.abspath(model_path)}
"""
    else:
        print("--- Starting LERO server with no initial model ---")
        # Create config content with the ModelPath commented out
        config_content = f"""[lero]
Port = {LERO_SERVER_PORT}
ListenOn = {LERO_SERVER_HOST}
# ModelPath = (No initial model specified)
"""

    # This block now runs in both cases, ensuring the file is always written.
    try:
        with open(config_path, "w") as f:
            f.write(config_content.strip())
        print(f"Configuration written to {config_path}")
    except IOError as e:
        print(f"!!! CRITICAL ERROR: Could not write to config file {config_path}. Error: {e} !!!")
        return None # Abort if we can't write the config

    print("Using LERO server directory:", server_script_dir)
    with open("server_stdout.log", "a") as out, open("server_stderr.log", "a") as err:
        server_process = subprocess.Popen(
            ["python3", "server.py"],
            cwd=server_script_dir,
            stdout=out,
            stderr=err
        )
    sleep(10)

    if server_process.poll() is None:
        print("LERO server started successfully.")
        return server_process
    else:
        print("Failed to start LERO server.")
        return None

def save_metrics_from_plan(query_name, sql_text, plan_json, predicted_latency, output_path):
    """
    Save query metrics (planning time, actual latency, predicted latency) to JSON.
    
    Args:
        query_name (str): Identifier of the query.
        sql_text (str): Full SQL query text.
        plan_json (list): Execution plan JSON returned by PostgreSQL.
        predicted_latency (float): Predicted latency from the model.
        output_path (str): Path to save the metrics JSON.
    """
    # Extract planning time and actual total execution time
    if plan_json and isinstance(plan_json, list) and "Planning Time" in plan_json[0]:
        planning_time = plan_json[0]["Planning Time"]
        actual_latency = plan_json[0]["Execution Time"]
    else:
        print(f"WARNING: Could not find planning/execution times in plan for {query_name}.")
        planning_time = None
        actual_latency = None

    metrics = {
        "query_name": query_name,
        "sql": sql_text,
        "planning_time": planning_time,
        "predicted_latency": predicted_latency,
        "actual_latency": actual_latency
    }

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    try:
        with open(output_path, 'w') as f:
            json.dump(metrics, f, indent=4)
        print(f"Metrics saved to {output_path}")
    except Exception as e:
        print(f"ERROR saving metrics for {query_name}: {e}")

def ensure_lero_directory(sql_file_path):
    """Ensure a LERO directory exists for the SQL file"""
    dir_path = os.path.dirname(sql_file_path)
    lero_dir = os.path.join(dir_path, "LERO")
    os.makedirs(lero_dir, exist_ok=True)
    return lero_dir

def save_plan(plan, output_path):
    """Save execution plan to a file"""
    with open(output_path, 'w') as f:
        json.dump(plan, f, indent=2)

def find_sql_files(directory, skip_lero_processed=False):
    """Recursively find all .sql files in a directory"""
    sql_files = []
    file_paths = []
    queryIDs = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.sql'):
                full_path = os.path.join(root, file)
                if skip_lero_processed:
                    lero_dir = os.path.join(os.path.dirname(full_path), "LERO")
                    if os.path.exists(lero_dir):
                        print(f"Skipping {file} as LERO directory already exists.")
                        continue
                file_paths.append(full_path)
                queryIDs.append(file[:-4])  # Remove .sql extension for queryID
                with open(full_path, 'r') as f:
                    sql_files.append(f.read())
    return [queryIDs, file_paths, sql_files]

def get_predicted_latency_from_server(plan_json):
    """Send plan to Lero server and get predicted latency."""
    if not plan_json or 'Plan' not in plan_json[0]:
        print("  - ERROR: Invalid plan JSON provided for latency prediction.")
        return None

    message_data = {"msg_type": "predict", "Plan": plan_json[0]['Plan']}

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((LERO_SERVER_HOST, LERO_SERVER_PORT))
            sock.sendall(bytes(json.dumps(message_data) + "*LERO_END*", "utf-8"))
            response = sock.recv(8192).decode("utf-8")
            reply = json.loads(response)
            latency = reply.get("latency")
    except Exception as e:
        print(f"  - ERROR: Could not connect to Lero server for latency prediction: {e}")
        return None

NUM_EXECUTIONS = 1
# python test.py --query_path ../reproduce/test_query/stats.txt --output_query_latency_file stats.test
from tqdm import tqdm

if __name__ == "__main__":
    parser = argparse.ArgumentParser("Model training helper")
    parser.add_argument("--query_path",
                        metavar="PATH",
                        help="Load the queries")
    parser.add_argument("--output_query_latency_file", metavar="PATH")
    parser.add_argument("--skip_lero_processed", action="store_true", help="Skip queries that already have a LERO directory")
    parser.add_argument("--checkpoint_dir", metavar="PATH", required=True, help="Path to the trained model checkpoint directory")

    args = parser.parse_args()
    server_process = start_lero_server(LERO_SERVER_DIR, model_path=args.checkpoint_dir)
    if not server_process:
        print("Could not start LERO server. Aborting.")
        exit(1)

    try: 
        test_queries = find_sql_files(args.query_path, skip_lero_processed=args.skip_lero_processed)
        # --- The main loop that processes queries ---
        for i in tqdm(range(len(test_queries[0])), desc="Processing queries"):
            queryID = test_queries[0][i]
            fp = test_queries[1][i]
            q = test_queries[2][i]
            count = 0
            lero_dir = ensure_lero_directory(fp)
            # Loop for multiple executions per query
            while count < NUM_EXECUTIONS:
                print(f"\nExecuting {queryID} (Run {count + 1}/{NUM_EXECUTIONS})")
                query_plan = None
                try:
                    # Execute the query
                    query_plan = test_query(q, fp, ["SET enable_lero TO True"], args.output_query_latency_file, True, None, None)
                except Exception as e:
                    print(f"ERROR executing query {queryID}: {e}")
                    print("Attempting to restart database and retry after a delay...")
                    # If execution fails, still try to restart the DB to recover
                    clear_cache()
                    continue # Skip to next execution attempt

                if query_plan is not None:
                    # Save the successful plan
                    if NUM_EXECUTIONS > 1:
                        output_path = os.path.join(lero_dir, f"run{count+1}", f"{queryID}_lero_plan.json")
                    else:
                        output_path = os.path.join(lero_dir, f"{queryID}_lero_plan.json")
                    os.makedirs(os.path.dirname(output_path), exist_ok=True)
                    save_plan(query_plan, output_path)
                    print(f"Execution plan for {queryID} saved to {output_path}.")

                    # Get predicted latency from Lero server
                    predicted_latency = get_predicted_latency_from_server(query_plan)
                    if predicted_latency is not None:
                        print(f"Predicted latency for {queryID}: {predicted_latency} ms")
                        if NUM_EXECUTIONS > 1:
                            metrics_path = os.path.join(lero_dir, f"run{count+1}", f"{queryID}_lero_metrics.json")
                        else:
                            metrics_path = os.path.join(lero_dir, f"{queryID}_lero_metrics.json")
                        os.makedirs(os.path.dirname(output_path), exist_ok=True)
                        save_metrics_from_plan(
                            query_name=queryID,
                            sql_text=q,
                            plan_json=query_plan,
                            predicted_latency=predicted_latency,
                            output_path=metrics_path
                        )
                else:
                    print(f"Failed to get an execution plan for query {queryID}.")
                count += 1
    finally:
        kill_lero_server()