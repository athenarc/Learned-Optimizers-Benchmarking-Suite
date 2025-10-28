import argparse

from utils import *
import glob
import os
import random
from utils import clear_cache
import os
import json
from tqdm import tqdm
import subprocess
from time import time,sleep
import socket
from pathlib import Path
import shutil
from utils import do_run_query
import signal

# ==============================================================================
# 1. CONFIGURATION
# ==============================================================================
NUM_EXECUTIONS = 1
LERO_SERVER_HOST = '195.251.63.231' # As per your original config
LERO_SERVER_PORT = 14567
# Set the directory where the LERO server script is located, which is the parent of this script
LERO_SERVER_DIR = Path(__file__).resolve().parent.parent
LERO_SERVER_SCRIPT = LERO_SERVER_DIR / "server.py"

# ==============================================================================
# 2. SERVER MANAGEMENT
# ==============================================================================
def is_port_free(port, host):
    """Checks if a TCP port is free by trying to bind to it."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind((host, port))
            return True
        except OSError:
            return False

def kill_lero_server():
    """Forcefully terminates any running LERO server process."""
    print("--- Killing LERO server process ---")
    subprocess.run(
        ['fuser', '-k', '-n', 'tcp', str(LERO_SERVER_PORT)],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    sleep(1) # Give OS time to release port

    # Step 1: Try pkill with full script path (safer than just 'server.py')
    subprocess.run(
        ['pkill', '-9', '-f', str(LERO_SERVER_SCRIPT)],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    sleep(1)

    # Step 2: Kill any process bound to the port via fuser (all PIDs)
    subprocess.run(
        ['fuser', '-k', f'{LERO_SERVER_PORT}/tcp'],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    sleep(1)

    # Step 3: Look for any process still bound to the port
    result = subprocess.run(
        ["ss", "-lptn", f"sport = :{LERO_SERVER_PORT}"],
        capture_output=True,
        text=True,
        check=False
    )

    if not result.stdout.strip():
        print(f"No process is listening on port {LERO_SERVER_PORT}")
        return

    # Step 4: Kill by PID from ss output
    for line in result.stdout.splitlines():
        if "pid=" in line:
            pid_str = line.split("pid=")[1].split(",")[0]
            pid = int(pid_str)
            try:
                os.kill(pid, signal.SIGKILL)
                print(f"Killed LERO server process PID {pid} on port {LERO_SERVER_PORT}")
            except ProcessLookupError:
                print(f"Process {pid} already gone")

    timeout = 65
    # Use tqdm to create a visual progress bar for the wait time
    with tqdm(total=timeout, bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt}s') as pbar:
        start_time = time()
        while time() - start_time < timeout:
            if is_port_free(LERO_SERVER_PORT, LERO_SERVER_HOST):
                pbar.update(timeout) # Instantly fill the progress bar
                print(f"Port {LERO_SERVER_PORT} is now free.")
                return
            
            sleep(1)
            pbar.update(1)

    # If the loop finishes without returning, the port is still blocked.
    raise RuntimeError(
        f"FATAL: Port {LERO_SERVER_PORT} could not be freed after {timeout} seconds. "
        "Please check for persistent processes or system issues."
    )            

def start_lero_server(model_path, server_script_dir):
    """Starts the LERO server with a specific model by creating a temporary config."""
    print(f"--- Starting LERO server with model: {Path(model_path).name} ---")
    
    # Dynamically create the config file for this run
    config_content = f"""
[lero]
Port = {LERO_SERVER_PORT}
ListenOn = {LERO_SERVER_HOST}
ModelPath = {os.path.abspath(model_path)}
"""
    config_path = os.path.join(server_script_dir, "server.conf")
    with open(config_path, "w") as f:
        f.write(config_content)

    print("Using LERO server directory:", server_script_dir)
    with open("server_stdout.log", "a") as out, open("server_stderr.log", "a") as err:
        server_process = subprocess.Popen(
            ["python3", "server.py"],
            cwd=server_script_dir,
            stdout=out,
            stderr=err
        )
    sleep(10) # Give server time to load the model

    if server_process.poll() is None:
        print("LERO server started successfully.")
        return server_process
    else:
        print("Failed to start LERO server.")
        return None

# ==============================================================================
# 3. QUERY EXECUTION AND RESULT SAVING
# ==============================================================================

def get_predicted_latency(plan_json):
    """Sends a plan to the running LERO server to get a prediction."""
    if not plan_json or not isinstance(plan_json, list) or 'Plan' not in plan_json[0]:
        return -1.0
    
    message_data = {"msg_type": "predict", "Plan": plan_json[0]['Plan']}
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((LERO_SERVER_HOST, LERO_SERVER_PORT))
            sock.sendall(bytes(json.dumps(message_data) + "*LERO_END*", "utf-8"))
            response = sock.recv(8192).decode("utf-8")
            # The prediction is in seconds, convert to ms
            return json.loads(response).get("latency", -1.0) * 1000
    except Exception as e:
        print(f"  - Prediction failed: {e}")
        return -1.0

def save_plan_and_metrics(test_queries_dir, original_query_filename, checkpoint_name, checkpoint_type, run_id, plan_json, metrics):
    """Saves plan and metrics to .../LERO/<checkpoint_type>/<checkpoint_name>/..."""
    try:
        query_dir = Path(test_queries_dir) / Path(original_query_filename).parent
        query_filename_stem = Path(original_query_filename).stem

        if NUM_EXECUTIONS > 1:
            output_dir = (query_dir / "LERO" / checkpoint_type / checkpoint_name / f"run_{run_id}")
        else:
            output_dir = (query_dir / "LERO" / checkpoint_type / checkpoint_name)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        plan_path = output_dir / f"{query_filename_stem}_plan.json"
        with open(plan_path, 'w') as f: json.dump(plan_json, f, indent=2)

        metrics_path = output_dir / f"{query_filename_stem}_metrics.json"
        with open(metrics_path, 'w') as f: json.dump(metrics, f, indent=2)
    except Exception as e:
        print(f"Error saving results for {original_query_filename}: {e}")

# ==============================================================================
# 4. MAIN WORKFLOW
# ==============================================================================

def discover_checkpoints(checkpoint_dir_base):
    """Finds all LERO model files in the subdirectories."""
    subdirs = ["epoch_checkpoints", "query_checkpoints", "loss_checkpoints"]
    all_checkpoints = []
    for subdir in subdirs:
        path = Path(checkpoint_dir_base) / subdir
        if path.is_dir():
            # LERO models are directories, not single files
            all_checkpoints.extend([os.path.join(path, d) for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))])
    return sorted(all_checkpoints)

def get_completed_checkpoints(test_queries_dir):
    """
    Scans the test query directory to find which LERO checkpoints have already
    been evaluated by checking for their output directories.
    """
    completed = set()
    glob_pattern = str(Path(test_queries_dir) / "**" / "LERO" / "*" / "*")

    for path_str in glob.glob(glob_pattern):
        path = Path(path_str)
        if path.is_dir():
            completed.add(path.name)
    return completed

def main(args):
    # Load test queries once
    test_queries = []
    test_sql_files = glob.glob(os.path.join(args.test_queries_dir, '**', '*.sql'), recursive=True)
    for f in test_sql_files:
        relative_path = os.path.relpath(f, args.test_queries_dir)
        test_queries.append((relative_path, Path(f).read_text()))
    
    all_checkpoints = discover_checkpoints(args.checkpoint_dir_base)
    print(f"Found {len(all_checkpoints)} total LERO checkpoints to evaluate.")

    completed_checkpoints = get_completed_checkpoints(args.test_queries_dir)
    if completed_checkpoints:
        print(f"Found results for {len(completed_checkpoints)} previously evaluated checkpoints. These will be skipped.")

    checkpoints_to_run = []
    for checkpoint_path in all_checkpoints:
        checkpoint_name = Path(checkpoint_path).name
        if checkpoint_name in completed_checkpoints:
            continue
        checkpoints_to_run.append(checkpoint_path)

    print(f"\nProceeding to evaluate {len(checkpoints_to_run)} new checkpoints.")
    if not checkpoints_to_run:
        print("All found checkpoints have already been evaluated.")
        return

    results_dir = Path(args.checkpoint_dir_base) / "results"
    results_dir.mkdir(exist_ok=True)
    latency_file_path = results_dir / "latencies.csv"
    if not latency_file_path.exists():
        with open(latency_file_path, 'w') as f:
            f.write("checkpoint_name,query_file,run_id,predicted_latency_ms,actual_latency_ms,q_error\n")

    # Add progress tracking with tqdm
    for checkpoint_path in tqdm(checkpoints_to_run, desc="Overall Progress"):
        server_process = start_lero_server(checkpoint_path, LERO_SERVER_DIR)
        if not server_process:
            print(f"Skipping checkpoint {Path(checkpoint_path).name} due to server start failure.")
            continue

        checkpoint_name = Path(checkpoint_path).name
        checkpoint_type = Path(checkpoint_path).parent.name
        
        try:
            for query_filename, sql in tqdm(test_queries, desc=f"Testing {checkpoint_name}"):
                for run_id in range(1, NUM_EXECUTIONS + 1):
                    plan_json = None
                    actual_latency_ms = -1.0
                    
                    try:
                        # Use the test_query function from utils.py
                        plan_json = test_query(sql, query_filename, ["SET enable_lero TO True", f"SET lero_server_host TO '{LERO_SERVER_HOST}'", f"SET lero_server_port TO {LERO_SERVER_PORT}"], None, write_latency_file=False)
                        if plan_json:
                            actual_latency_ms = plan_json[0]['Execution Time']
                    except Exception as e:
                        print(f"test_query failed for {query_filename}: {e}")
                        plan_json = {'Error': str(e)}

                    predicted_latency_ms = get_predicted_latency(plan_json)

                    q_error = float('inf')
                    if actual_latency_ms > 0 and predicted_latency_ms > 0:
                        q_error = max(predicted_latency_ms / actual_latency_ms, actual_latency_ms / predicted_latency_ms)

                    with open(latency_file_path, 'a') as f:
                        f.write(f"{checkpoint_name},{query_filename},{run_id},{predicted_latency_ms},{actual_latency_ms},{q_error}\n")

                    save_plan_and_metrics(
                        args.test_queries_dir, query_filename, checkpoint_name, checkpoint_type, run_id,
                        plan_json,
                        {'predicted_latency_ms': predicted_latency_ms, 'actual_latency_ms': actual_latency_ms, 'q_error': q_error}
                    )
        finally:
            kill_lero_server()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Systematically evaluate all LERO training checkpoints.")
    parser.add_argument("checkpoint_dir_base", help="Base directory of checkpoints ('.../exp3/LERO/').")
    parser.add_argument("test_queries_dir", help="Directory of test SQL files.")
    
    args = parser.parse_args()
    main(args)