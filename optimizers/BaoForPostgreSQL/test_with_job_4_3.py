import os
import psycopg2
import sys
import random
import json
from time import time, sleep
from datetime import datetime
import subprocess
import shutil
import re

# Configuration
USE_BAO = True
PG_CONNECTION_STR = "dbname=imdbload user=suite_user host=train.darelab.athenarc.gr port=5469 password=71Vgfi4mUNPm"
FINAL_MODEL_DIR = "/data/hdd1/users/kmparmp/models/bao/job/20250324_111844_bao_default_model"
EXPLAIN_ANALYZE_MODE = True  # Set to False to do timing measurements instead
SAVE_PG_PLANS = False  # Set to False to only save BAO plans
PERFORMANCE_LOG_PATH = "/data/hdd1/users/kmparmp/BaoForPostgreSQL/bao_server/performance_log.txt"

def find_sql_files(directory):
    """Recursively find all .sql files in a directory"""
    sql_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.sql'):
                full_path = os.path.join(root, file)
                sql_files.append(full_path)
    return sql_files

def ensure_bao_directory(sql_file_path):
    """Ensure a BAO directory exists for the SQL file"""
    dir_path = os.path.dirname(sql_file_path)
    bao_dir = os.path.join(dir_path, "BAO")
    os.makedirs(bao_dir, exist_ok=True)
    return bao_dir

def save_json(data, output_path):
    """Save data to a JSON file"""
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)

# Constants
def run_query(sql, bao_select=False, bao_reward=False):
    """Run query and return either time or JSON explain analyze plan"""
    start = time()
    while True:
        try:
            conn = psycopg2.connect(PG_CONNECTION_STR)
            cur = conn.cursor()
            cur.execute(f"SET enable_bao TO {bao_select or bao_reward}")
            cur.execute(f"SET bao_host = '195.251.63.231'")
            cur.execute(f"SET bao_port = 9381")
            cur.execute(f"SET enable_bao_selection TO {bao_select}")
            cur.execute(f"SET enable_bao_rewards TO {bao_reward}")
            cur.execute("SET bao_num_arms TO 27")
            
            if EXPLAIN_ANALYZE_MODE:
                # Get plan in JSON format
                cur.execute("EXPLAIN (ANALYZE, FORMAT JSON) " + sql)
                plan = cur.fetchone()[0]
                conn.close()
                return plan
            else:
                cur.execute(sql)
                cur.fetchall()
                conn.close()
                break
                
        except psycopg2.errors.QueryCanceled:
            print("Query was canceled due to statement timeout")
            conn.close()
            return None  # or raise an exception if you prefer
        except Exception as e:
            print(f"Query failed: {str(e)}. Retrying...")
            sleep(1)
            continue
    
    if not EXPLAIN_ANALYZE_MODE:
        stop = time()
        return stop - start
    return None

def kill_bao_server():
    """Terminate bao_server/main.py using pkill"""
    try:
        result = subprocess.run(
            ['pkill', '-f', 'bao_server/main.py'],
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE
        )
        
        # Delete the bao_default_model and bao.db from bao_server directory
        bao_default_model_path = os.path.join("bao_server", "bao_default_model")
        bao_db_path = os.path.join("bao_server", "bao.db")
        if os.path.exists(bao_default_model_path):
            shutil.rmtree(bao_default_model_path)
        if os.path.exists(bao_db_path):
            os.remove(bao_db_path)
        
        # Run the clean_experience.py script
        subprocess.run(
            ['python3', 'bao_server/clean_experience.py'],
            cwd="bao_server",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        if result.returncode == 0:
            print("Successfully terminated bao_server process")
            return True
        elif result.returncode == 1:
            print("No running bao_server processes found")
            return False
        else:
            print(f"Error terminating bao_server: {result.stderr.decode().strip()}")
            return False
    except Exception as e:
        print(f"Error running pkill: {str(e)}")
        return False

def start_bao_server_with_final_model():
    """Start the bao server with the final trained model"""
    # First kill any existing server
    kill_bao_server()
    
    # Check if model directory exists
    if not os.path.exists(FINAL_MODEL_DIR):
        print(f"Error: Final model directory not found at {FINAL_MODEL_DIR}")
        return False
    
    try:
        # Create bao_server directory if it doesn't exist
        os.makedirs("bao_server/bao_default_model", exist_ok=True)
        
        # Copy all model files to the bao_server directory
        for item in os.listdir(FINAL_MODEL_DIR):
            print(f"Copying {item} to bao_server directory...")
            src_path = os.path.join(FINAL_MODEL_DIR, item)
            dest_path = os.path.join("bao_server/bao_default_model", item)
            
            if os.path.isdir(src_path):
                if os.path.exists(dest_path):
                    shutil.rmtree(dest_path)
                shutil.copytree(src_path, dest_path)
            else:
                shutil.copy2(src_path, dest_path)
                
        # Start the server in the bao_server directory
        server_process = subprocess.Popen(
            ["python3", "main.py", "--log-performance", "--log-file-path", "performance_log.txt"],
            cwd="bao_server",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Give it some time to start
        sleep(10)
        
        # Check if it's running
        if server_process.poll() is None:
            print("BAO server started successfully with final model")
            return True
        else:
            print("Failed to start BAO server")
            print("STDOUT:", server_process.stdout.read().decode())
            print("STDERR:", server_process.stderr.read().decode())
            return False
            
    except Exception as e:
        print(f"Error starting BAO server: {str(e)}")
        return False

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def parse_performance_log():
    """Parse the performance log to get predicted latencies and inference times"""
    performance_data = {}
    if not os.path.exists(PERFORMANCE_LOG_PATH):
        return performance_data
        
    # Regular expression pattern to match the log line format
    pattern = re.compile(
        r"(?P<timestamp>\d+\.\d+) - Query #(?P<query_num>\d+) - "
        r"Best Predicted Latency: (?P<latency>\d+\.\d+) - "
        r"Inference Time: (?P<inference_time>\d+\.\d+)"
    )
    
    with open(PERFORMANCE_LOG_PATH, 'r') as f:
        for line in f:
            match = pattern.match(line.strip())
            if match:
                try:
                    query_num = int(match.group('query_num'))
                    performance_data[query_num] = {
                        'timestamp': float(match.group('timestamp')),
                        'predicted_latency': float(match.group('latency')),
                        'inference_time': float(match.group('inference_time'))
                    }
                except ValueError as e:
                    print(f"Error parsing values in line: {line.strip()}")
                    continue
            else:
                print(f"Skipping malformed line: {line.strip()}")
    
    return performance_data

def load_model():
    """Instruct the BAO server to load the specified model"""
    model_path = os.path.join("/data/hdd1/users/kmparmp/BaoForPostgreSQL/bao_server", "bao_default_model")
    print(f"Loading model from {model_path}")
    cmd = f"cd bao_server && python3 baoctl.py --load {model_path}"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"Failed to load model: {result.stderr}")
        return False
    
    print("Model loaded successfully")
    return True

def main():
    if len(sys.argv) < 2:
        print("Usage: python script.py <directory_path>")
        return
    
    directory_path = sys.argv[1]
    sql_files = find_sql_files(directory_path)
    
    if not sql_files:
        print(f"No SQL files found in directory: {directory_path}")
        return

    # Clear previous performance log
    if os.path.exists(PERFORMANCE_LOG_PATH):
        os.remove(PERFORMANCE_LOG_PATH)
    
    queries = []
    for fp in sql_files:
        with open(fp) as f:
            query = f.read()
        queries.append((fp, query))
    print("Found", len(queries), "SQL files to execute.")
    
    if not start_bao_server_with_final_model():
        print("Failed to start BAO server, exiting.")
        return

    # Explicitly load the final model
    if not load_model():
        print("Failed to load final model, exiting.")
        kill_bao_server()
        return

    random.seed(42)
    random.shuffle(queries)

    # Process each query
    for query_num, (fp, q) in enumerate(queries, 1):
        print(f"\nProcessing query #{query_num}: {fp}")
        
        # Create BAO directory
        bao_dir = ensure_bao_directory(fp)
        query_name = os.path.splitext(os.path.basename(fp))[0]
        
        if SAVE_PG_PLANS:
            # Get and save PostgreSQL plan
            pg_plan = run_query(q, bao_select=False, bao_reward=False)
            pg_plan_path = os.path.join(bao_dir, f"{query_name}_pg_plan.json")
            save_json(pg_plan, pg_plan_path)
            print(f"Saved PG plan to {pg_plan_path}")
        
        # Get and save BAO plan
        bao_plan = run_query(q, bao_select=True, bao_reward=True)
        bao_plan_path = os.path.join(bao_dir, f"{query_name}_bao_plan.json")
        save_json(bao_plan, bao_plan_path)
        print(f"Saved BAO plan to {bao_plan_path}")
    
    print("\nAll query plans saved successfully.")
    
    # Parse performance log for predicted latencies and inference times
    performance_data = parse_performance_log()
    
    # Save performance metrics to corresponding files
    for query_num, (fp, _) in enumerate(queries, 1):
        if query_num in performance_data:
            bao_dir = ensure_bao_directory(fp)
            query_name = os.path.splitext(os.path.basename(fp))[0]
            metrics_path = os.path.join(bao_dir, f"{query_name}_bao_metrics.json")
            
            # Save both latency and inference time
            save_json({
                "predicted_latency": performance_data[query_num]['predicted_latency'],
                "inference_time": performance_data[query_num]['inference_time']
            }, metrics_path)
            
            print(f"Saved BAO metrics to {metrics_path}")
    
    # Clean up
    kill_bao_server()

if __name__ == "__main__":
    main()