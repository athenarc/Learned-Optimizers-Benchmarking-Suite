import psycopg2
import os
import sys
import random
from time import time, sleep
import os
from dotenv import load_dotenv

def load_repo_env():
    """Walk up directories until .env is found and load it."""
    current_dir = os.path.abspath(os.path.dirname(__file__))
    while True:
        env_path = os.path.join(current_dir, ".env")
        if os.path.exists(env_path):
            load_dotenv(env_path)
            break
        parent_dir = os.path.dirname(current_dir)
        if parent_dir == current_dir:
            raise FileNotFoundError(".env file not found in any parent directory.")
        current_dir = parent_dir

# Load .env once when the module is imported
load_repo_env()

def pg_connection_string(db_name: str) -> str:
    """Constructs a PostgreSQL connection string from environment variables."""
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT", "5432")  # default port fallback

    if not all([db_user, db_pass, db_host]):
        raise ValueError("Missing one or more database environment variables (DB_USER, DB_PASS, DB_HOST).")

    return f"dbname={db_name} user={db_user} host={db_host} port={db_port} password={db_pass}"

USE_BAO = True
PG_CONNECTION_STR = pg_connection_string("imdbload")

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