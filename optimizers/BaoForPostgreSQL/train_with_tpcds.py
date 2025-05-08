import psycopg2
import os
import sys
import random
from time import time, sleep
import shutil
from datetime import datetime
import subprocess

USE_BAO = True
PG_CONNECTION_STR = "dbname=tpcds user=suite_user host=train.darelab.athenarc.gr port=5469 password=71Vgfi4mUNPm"

def kill_bao_server():
    """Terminate bao_server/main.py using pkill"""
    try:
        result = subprocess.run(
            ['pkill', '-f', 'bao_server/main.py'],
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE
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
    
def save_final_model_and_history():
    """Save the final trained model and all history, then clean original repo"""
    # Configuration
    model_dir = "/data/hdd1/users/kmparmp/models/bao/tpc-ds"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs(model_dir, exist_ok=True)

    print("Terminating bao_server processes...")
    server_was_running = kill_bao_server()

    # Files/directories to transfer
    transfer_items = [
        "bao_server/bao_default_model",
        "bao_server/bao_default_model.metadata.json",
        "bao_server/bao_previous_model.metadata.json",
        "training_time.txt",
        "bao_server/model_archive"
    ]

    # 1. Transfer items to central location
    transferred = []
    for src_path in transfer_items:
        if os.path.exists(src_path):
            if src_path == "bao_server/model_archive":
                # Special handling for archive directory
                archive_dest = os.path.join(model_dir, "archive")
                os.makedirs(archive_dest, exist_ok=True)
                
                for item in os.listdir(src_path):
                    item_src = os.path.join(src_path, item)
                    item_dest = os.path.join(archive_dest, item)
                    
                    if os.path.isdir(item_src):
                        shutil.copytree(item_src, item_dest)
                    else:
                        shutil.copy2(item_src, item_dest)
                transferred.append(src_path)
                print(f"Transferred model archive to {archive_dest}")
            else:
                # Handle regular files/directories
                dest_name = f"{timestamp}_{os.path.basename(src_path)}" if "archive" not in src_path else os.path.basename(src_path)
                dest_path = os.path.join(model_dir, dest_name)
                
                if os.path.isdir(src_path):
                    shutil.copytree(src_path, dest_path)
                else:
                    shutil.copy2(src_path, dest_path)
                transferred.append(src_path)
                print(f"Saved {os.path.basename(src_path)} to {dest_path}")
        else:
            print(f"Warning: Source not found - {src_path}")

    # 2. Clean up original repository
    for src_path in transferred:
        try:
            if os.path.isdir(src_path):
                shutil.rmtree(src_path)
            else:
                os.remove(src_path)
            print(f"Cleaned: Removed {src_path} from original location")
        except Exception as e:
            print(f"Warning: Could not remove {src_path} - {str(e)}")

    # 3. Additional cleanup operations
    try:
        # Additional cleanups
        db_paths = ["bao_server/bao.db", "bao_server/bao_previous_model.metadata.json"]
        for db_path in db_paths:
            if os.path.exists(db_path):
                os.remove(db_path)
        
        # Run experience cleaner
        print("Running experience cleaner...")
        os.system("")
        os.system("python3 bao_server/clean_experience.py")
        print("Experience cleaning completed")

    except Exception as e:
        print(f"Error during additional cleanup: {str(e)}")
        
    # 3. Create summary README
    readme_content = f"""BAO Model Training Summary
===========================
Timestamp: {timestamp}
Storage Location: {model_dir}

Contents:
- Current Model: {timestamp}_bao_default_model
- Model Metadata: {timestamp}_bao_default_model.metadata.json
- Execution Time: {timestamp}_training_time.txt
- Archived Models: archive/ directory

Original repository has been cleaned.
"""
    readme_path = os.path.join(model_dir, f"{timestamp}_README.md")
    with open(readme_path, "w") as f:
        f.write(readme_content)
          
def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def run_query(sql, bao_select=False, bao_reward=False):
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
            cur.execute("SET bao_num_arms TO 5")
            cur.execute("SET statement_timeout TO 300000")
            cur.execute(sql)
            cur.fetchall()
            conn.close()
            break
        except:
            sleep(1)
            continue
    stop = time()
    return stop - start

# Start timer
start_time = time()

query_paths = sys.argv[1:]
queries = []
for fp in query_paths:
    with open(fp) as f:
        query = f.read()
    queries.append((fp, query))
print("Read", len(queries), "queries.")
print("Using Bao:", USE_BAO)

random.seed(42)
query_sequence = random.choices(queries, k=500)
pg_chunks, *bao_chunks = list(chunks(query_sequence, 25))

print("Executing queries using PG optimizer for initial training")

for fp, q in pg_chunks:
    pg_time = run_query(q, bao_reward=True)
    print("x", "x", time(), fp, pg_time, "PG", flush=True)

for c_idx, chunk in enumerate(bao_chunks):
    if USE_BAO:
        os.system("cd bao_server && python3 baoctl.py --retrain")
        os.system("sync")
    for q_idx, (fp, q) in enumerate(chunk):
        q_time = run_query(q, bao_reward=USE_BAO, bao_select=USE_BAO)
        print(c_idx, q_idx, time(), fp, q_time, flush=True)

# Stop timer
end_time = time()
total_time = end_time - start_time

# Store execution time in a file
with open("training_time.txt", "w") as f:
    f.write(f"Total training time: {total_time:.2f} seconds\n")

print(f"Total training time: {total_time:.2f} seconds")
save_final_model_and_history()
