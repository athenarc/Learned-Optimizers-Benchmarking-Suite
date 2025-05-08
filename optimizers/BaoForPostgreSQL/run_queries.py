import psycopg2
import os
import sys
import random
from time import time, sleep

USE_BAO = True
PG_CONNECTION_STR = "dbname=imdbload user=suite_user host=train.darelab.athenarc.gr port=5469 password=71Vgfi4mUNPm"

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