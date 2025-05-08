import os
import time
import psycopg2
import argparse
import re

# Database connection string
PG_CONNECTION_STR = "dbname=imdbload user=suite_user host=train.darelab.athenarc.gr port=5469 password=71Vgfi4mUNPm"

def run_query(sql, bao_select=True, bao_reward=True):
    start = time.time()
    while True:
        try:
            conn = psycopg2.connect(PG_CONNECTION_STR)
            cur = conn.cursor()
            cur.execute(f"SET enable_bao TO {bao_select or bao_reward}")
            cur.execute(f"SET bao_host = '195.251.63.231'")
            cur.execute(f"SET bao_port = 9381")
            cur.execute(f"SET enable_bao_selection TO {bao_select}")
            cur.execute(f"SET enable_bao_rewards TO {bao_reward}")
            cur.execute("SET bao_num_arms TO 5")
            cur.execute("SET statement_timeout TO 300000")
            cur.execute(sql)
            cur.fetchall()
            cur.execute("SELECT \"clear_cache\"();")
            conn.close()
            break
        except Exception as e:
            print(f"Error executing query: {e}")
            time.sleep(1)
            continue
    stop = time.time()
    return stop - start

def execute_queries_in_directory(directory_path):
    latencies = []
    cumulative_latency = 0  # Variable to accumulate latency
    query_counter = 1  # Initialize query counter    
    # Iterate over all files in the given directory
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)

        # Only process .sql files
        if os.path.isfile(file_path) and filename.endswith('.sql'):
            with open(file_path, 'r') as sql_file:
                sql_query = sql_file.read()

            # Call run_query with the SQL from the file (can set bao_select or bao_reward flags as needed)
            latency = run_query(sql_query)
            latencies.append((query_counter, filename, latency))
            cumulative_latency += latency  # Add the latency to cumulative total
            query_counter += 1  # Increment query counter            
            print(f"Query in {filename} executed in {latency:.6f} seconds")

    # Save the latencies to a file for further analysis
    actual_latencies_file_path = "query_real_latencies.txt"
    with open(actual_latencies_file_path, "w") as latencies_file:
        for query_counter, filename, latency in latencies:
            latencies_file.write(f"Query #{query_counter} - {filename}: {latency:.6f} seconds\n")
    
    # Output the cumulative latency
    print(f"Cumulative latency for all queries: {cumulative_latency:.6f} seconds")

    return actual_latencies_file_path

def read_performance_log(performance_log_path):
    predicted_latencies = {}
    inference_times = {}
    
    with open(performance_log_path, "r") as file:
        for line in file:
            match = re.match(r"(\d+\.\d+) - Query #(\d+) - Best Predicted Latency: (\d+\.\d+) - Inference Time: (\d+\.\d+)", line.strip())
            if match:
                timestamp = float(match.group(1))  # Timestamp
                query_num = int(match.group(2))  # Query Number
                predicted_latency = float(match.group(3))  # Predicted Latency (in milliseconds)
                inference_time = float(match.group(4))  # Inference Time (in seconds)
                predicted_latencies[query_num] = predicted_latency
                inference_times[query_num] = inference_time
                
    return predicted_latencies, inference_times

def read_actual_latencies(actual_file_path):
    actual_latencies = {}
    query_names = {}
    
    with open(actual_file_path, "r") as file:
        for line in file:
            # Match the query number, query name, and actual latency
            match = re.match(r"Query #(\d+) - (.+\.sql): (\d+\.\d+) seconds", line.strip())
            if match:
                query_num = int(match.group(1))  # Query Number
                query_name = match.group(2)  # Query Name (e.g., "query1.sql")
                actual_latency = float(match.group(3)) * 1000  # Convert seconds to milliseconds
                
                # Store the actual latency and query name
                actual_latencies[query_num] = actual_latency
                query_names[query_num] = query_name
                
    return actual_latencies, query_names

def calculate_q_error(predicted_latencies, actual_latencies):
    q_errors = {}
    total_q_error = 0
    num_queries = 0
    
    # Iterate over each query to calculate the Q-error
    for query_num in predicted_latencies:
        if query_num in actual_latencies:
            predicted_latency = predicted_latencies[query_num]
            actual_latency = actual_latencies[query_num]
            
            # Calculate Q-error: max(predicted/actual, actual/predicted)
            q_error = max(predicted_latency / actual_latency, actual_latency / predicted_latency)
            
            q_errors[query_num] = q_error
            total_q_error += q_error
            num_queries += 1
    
    return q_errors, total_q_error / num_queries if num_queries > 0 else 0

import shutil
import subprocess
FINAL_MODEL_DIR = "/data/hdd1/users/kmparmp/models/bao/job/20250324_111844_bao_default_model"

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


def fetch_final_model():
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
                
        return True
            
    except Exception as e:
        print(f"Error starting BAO server: {str(e)}")
        return False

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

def main(workload_directory_path, performance_log_path, output_file_path):
    # Fetch the final model and load it
    fetch_final_model()
    load_model()
    
    # Execute queries and get the path to the actual latencies file
    actual_latencies_file_path = execute_queries_in_directory(workload_directory_path)
    
    # Read the predicted latencies and inference times from the performance log
    predicted_latencies, inference_times = read_performance_log(performance_log_path)
    actual_latencies, query_names = read_actual_latencies(actual_latencies_file_path)
    
    # Calculate Q-error
    q_errors, avg_q_error = calculate_q_error(predicted_latencies, actual_latencies)
    
    # Write the statistics to the output file
    with open(output_file_path, "w") as output_file:
        for query_num, q_error in q_errors.items():
            inference_time = inference_times.get(query_num, 0)
            query_name = query_names.get(query_num, "Unknown")
            predicted_latency = predicted_latencies.get(query_num, 0)
            actual_latency = actual_latencies.get(query_num, 0)
            output_file.write(f"{query_num} - {query_name} - {inference_time:.6f} - {predicted_latency:.6f} - {actual_latency:.6f} - {q_error:.6f}\n")    
    
    print(f"Statistics saved to {output_file_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Execute SQL queries from files in a directory, record latencies, and calculate Q-error.")
    parser.add_argument("workload_directory", type=str, help="Directory path containing SQL files to execute")
    parser.add_argument("output_file", type=str, default="test_output.txt", help="Path to the output file")
    parser.add_argument("performance_log", type=str, default="/data/hdd1/users/kmparmp/BaoForPostgreSQL/bao_server/performance_log.txt", help="Path to the file containing predicted latencies and inference times")
    
    args = parser.parse_args()
    workload_directory_path = args.workload_directory
    performance_log_path = args.performance_log
    output_file_path = args.output_file

    # Ensure the provided directory exists
    if not os.path.isdir(workload_directory_path):
        print(f"Error: The directory {workload_directory_path} does not exist.")
    else:
        main(workload_directory_path, performance_log_path, output_file_path)