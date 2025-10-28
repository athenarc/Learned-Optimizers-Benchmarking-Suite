import hashlib
import json
import os
from time import sleep, time
from config import *
import fcntl
import psycopg2
import logging
import sqlalchemy
from sqlalchemy import text, event
from sqlalchemy.exc import OperationalError
from multiprocessing import current_process

# We need the specific psycopg2 error to check for it robustly.
try:
    from psycopg2 import errors as psycopg2_errors
except ImportError:
    # Create a dummy class if psycopg2 is not installed, so the code doesn't crash.
    class _DummyPsycopg2Errors:
        class DiskFull: pass
    psycopg2_errors = _DummyPsycopg2Errors()

# --- Basic Logging ---
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - PID:%(process)d - %(levelname)s - %(message)s'
)

# --- Engines managed per process ---
_process_engines = {}

def get_engine():
    """
    Creates and returns a SQLAlchemy engine, cached per process.
    Uses an event listener to configure session variables for each new connection.
    """
    pid = current_process().pid
    if pid not in _process_engines:
        logging.info(f"Creating a new engine for process PID: {pid}")
        engine = sqlalchemy.create_engine(
            DATABASE_URL,
            pool_size=1,        # Recommended for worker processes to not hold idle connections
            max_overflow=0,
            pool_pre_ping=True, # Proactively checks connection liveness
        )

        # Hook: configure LERO/PG vars once per new connection in the pool
        @event.listens_for(engine, "connect")
        def set_session_settings(dbapi_connection, connection_record):
            # This function runs every time a *new* physical connection is made.
            cursor = dbapi_connection.cursor()
            logging.info(f"PID: {pid} - Configuring new connection with session variables.")
            # Set your desired variables here
            cursor.execute(f"SET search_path TO public")
            cursor.execute(f"SET statement_timeout TO {TIMEOUT}")
            cursor.execute(f"SET enable_lero TO True")
            cursor.execute(f"SET lero_server_host TO '{LERO_SERVER_HOST}'")
            cursor.execute(f"SET lero_server_port TO {LERO_SERVER_PORT}") 
            cursor.close()

        _process_engines[pid] = engine
        
    return _process_engines[pid]

def check_server_health():
    """
    Actively checks if the database server is responsive using the process-local engine.

    Returns:
        tuple[bool, str]: A tuple containing (is_healthy, message).
    """
    try:
        engine = get_engine() # Reuses the existing engine for this process
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True, "Server is healthy and responsive."
    except OperationalError as e:
        return False, f"Server health check failed. Reason: {e}"
    except Exception as e:
        return False, f"An unexpected error occurred during health check: {e}"

def clear_cache():
    """
    Executes the remote restart UDF and waits for the server
    to be fully out of recovery mode and ready to accept queries.
    """
    engine = get_engine()
    print("\n" + "="*25 + " [TRIGGERING REMOTE RESTART] " + "="*25)
    try:
        with engine.connect() as connection:
            with connection.begin():
                print(f"--> Calling remote UDF: SELECT {RESTART_UDF_NAME}();")
                connection.execute(text(f"SELECT {RESTART_UDF_NAME}();"))
    except sqlalchemy.exc.OperationalError:
        print("--> Database disconnected as expected during restart. This is normal.")
    except Exception as e:
        print(f"--> Unexpected error while triggering restart UDF: {e}")

    # Poll until recovery is over
    print("--> Waiting for server to become fully operational...")
    max_wait_time = 300
    start_wait = time()

    while time() - start_wait < max_wait_time:
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT pg_is_in_recovery();"))
                is_in_recovery = result.scalar()
                if not is_in_recovery:
                    print("--> SUCCESS: Server is online and out of recovery mode.")
                    print("=" * 75 + "\n")
                    return
                else:
                    print("--> Still in recovery. Waiting 5s...")
                    sleep(5)
        except sqlalchemy.exc.OperationalError:
            print("--> Server not ready. Waiting 5s...")
            sleep(5)

    print(f"FATAL: Server did not exit recovery within {max_wait_time} seconds.")
    exit(1)


def encode_str(s):
    md5 = hashlib.md5()
    md5.update(s.encode('utf-8'))
    return md5.hexdigest()
        
def run_query(q, max_retries=3, delay_seconds=10):
    """
    Executes a SQL query with timing, retries, and health checks.

    Args:
        q (str): The SQL query string to execute.
        max_retries (int): The number of times to retry upon failure.
        delay_seconds (int): The number of seconds to wait between retries.

    Returns:
        tuple[float, list | str]: 
        - On success: (duration_in_seconds, list_of_rows)
        - On handled failure: (-1, failure_reason_string)
        
    Raises:
        OperationalError: If the query fails after all retry attempts.
    """
    engine = get_engine()
    last_exception = None

    for attempt in range(len(max_retries)):
        try:
            with engine.connect() as conn:
                # Start timer right before execution
                start = time()
                
                result = conn.execute(text(q)).fetchall()
                
                # Stop timer immediately after
                stop = time()
                
                # Success: return duration and result
                return stop - start, result
        
        except OperationalError as e:
            last_exception = e
            logging.warning(
                f"Query failed on attempt {attempt + 1}/{max_retries}. Error: {e}"
            )

            if isinstance(getattr(e, 'orig', None), psycopg2_errors.DiskFull):
                logging.error("Failure due to 'Disk Full'. Aborting retries.")
                return -1, "FAILURE_DISK_FULL"

            if attempt == max_retries - 1:
                logging.error("All retry attempts failed.")
                break

            logging.info("--> Checking database server health before retrying...")
            is_healthy, message = check_server_health()
            logging.info(f"--> Health Check Result: {message}")
            
            if not is_healthy:
                logging.error("Server is not healthy. Aborting retries.")
                return -1, "FAILURE_SERVER_UNHEALTHY"
            
            logging.info(f"Server is healthy. Retrying in {delay_seconds} seconds...")
            time.sleep(delay_seconds)

    # If the loop completes without success, raise the last known exception.
    raise last_exception

# # --- MODIFIED: run_query now uses the SQLAlchemy pool ---
# def run_query(q, run_args):
#     start = time()
#     conn = psycopg2.connect(CONNECTION_STR)
#     conn.set_client_encoding('UTF8')
#     result = None
#     try:
#         cur = conn.cursor()
#         cur.execute("SET lero_server_host TO '" + LERO_SERVER_HOST + "'")
#         cur.execute("SET lero_server_port TO " + str(LERO_SERVER_PORT))
#         if run_args is not None and len(run_args) > 0:
#             for arg in run_args:
#                 cur.execute(arg)
#         cur.execute("SET statement_timeout TO " + str(TIMEOUT))
#         # print(run_args)
#         # print(q)
#         cur.execute(q)
#         result = cur.fetchall()
#     finally:
        
#         conn.close()
#     # except Exception as e:
#     #     conn.close()
#     #     raise e
    
#     stop = time()
#     return stop - start, result


def get_history(encoded_q_str, plan_str, encoded_plan_str):
    history_path = os.path.join(LOG_PATH, encoded_q_str, encoded_plan_str)
    if not os.path.exists(history_path):
        return None
    
    with open(os.path.join(history_path, "check_plan"), "r") as f:
        history_plan_str = f.read().strip()
        if plan_str != history_plan_str:
            print("there is a hash conflict between two plans:", history_path)
            print("given", plan_str)
            print("wanted", history_plan_str)
            return None
    
    with open(os.path.join(history_path, "plan"), "r") as f:
        return f.read().strip()
    
def save_history(q, encoded_q_str, plan_str, encoded_plan_str, latency_str):
    history_q_path = os.path.join(LOG_PATH, encoded_q_str)
    os.makedirs(history_q_path, exist_ok=True)
    query_file_path = os.path.join(history_q_path, "query")
    if not os.path.exists(query_file_path):
        with open(query_file_path, "w") as f:
            f.write(q)
    else:
        with open(query_file_path, "r") as f:
            history_q = f.read()
            if q != history_q:
                # print("there is a hash conflict between two queries:", history_q_path)
                return    
    
    history_plan_path = os.path.join(history_q_path, encoded_plan_str)
    os.makedirs(history_plan_path, exist_ok=True)
    plan_file_path = os.path.join(history_plan_path, "plan")
    if os.path.exists(plan_file_path):
        return
        
    with open(os.path.join(history_plan_path, "check_plan"), "w") as f:
        f.write(plan_str)
    with open(plan_file_path, "w") as f:
        f.write(latency_str)

def explain_query(q, run_args, contains_cost = False):
    q = "EXPLAIN (COSTS " + ("" if contains_cost else "False") + ", FORMAT JSON, SUMMARY) " + (q.strip().replace("\n", " ").replace("\t", " "))
    _, plan_json = run_query(q, run_args)
    plan_json = plan_json[0][0]
    if len(plan_json) == 2:
        # remove bao's prediction
        plan_json = [plan_json[1]]
    return plan_json

def explain_analyze_query(q, run_args):
    q = "EXPLAIN (ANALYZE, FORMAT JSON) " + (q.strip().replace("\n", " ").replace("\t", " "))
    _, plan_json = run_query(q, run_args)
    plan_json = plan_json[0][0]
    if len(plan_json) == 2:
        # remove bao's prediction
        plan_json = [plan_json[1]]
    return plan_json

def create_training_file(training_data_file, *latency_files):
    lines = []
    for file in latency_files:
        with open(file, 'r') as f:
            lines += f.readlines()

    pair_dict = {}

    for line in lines:
        arr = line.strip().split(SEP)
        if arr[0] not in pair_dict:
            pair_dict[arr[0]] = []
        pair_dict[arr[0]].append(arr[1])

    pair_str = []
    for k in pair_dict:
        if len(pair_dict[k]) > 1:
            candidate_list = pair_dict[k]
            pair_str.append(SEP.join(candidate_list))
    str = "\n".join(pair_str)

    with open(training_data_file, 'w') as f2:
        f2.write(str)

def do_run_query(sql, query_name, run_args, latency_file, write_latency_file = True, manager_dict = None, manager_lock = None):
    sql = sql.strip().replace("\n", " ").replace("\t", " ")

    # 1. run query with pg hint
    _, plan_json = run_query("EXPLAIN (COSTS FALSE, FORMAT JSON, SUMMARY) " + sql, run_args)
    plan_json = plan_json[0][0]
    if len(plan_json) == 2:
        # remove bao's prediction
        plan_json = [plan_json[1]]
    planning_time = plan_json[0]['Planning Time']
    
    cur_plan_str = json.dumps(plan_json[0]['Plan'])
    try:
        # 2. get previous running result
        latency_json = None
        encoded_plan_str = encode_str(cur_plan_str)
        encoded_q_str = encode_str(sql)
        previous_result = get_history(encoded_q_str, cur_plan_str, encoded_plan_str)
        if previous_result is not None:
            latency_json = json.loads(previous_result)
        else:
            if manager_dict is not None and manager_lock is not None:
                manager_lock.acquire()
                if cur_plan_str in manager_dict:
                    manager_lock.release()
                    print("another process will run this plan:", cur_plan_str)
                    return
                else:
                    manager_dict[cur_plan_str] = 1
                    manager_lock.release()

            # 3. run current query 
            run_start = time()
            try:
                _, latency_json = run_query("EXPLAIN (ANALYZE, TIMING, VERBOSE, COSTS, SUMMARY, FORMAT JSON) " + sql, run_args)
                latency_json = latency_json[0][0]
                if len(latency_json) == 2:
                    # remove bao's prediction
                    latency_json = [latency_json[1]]
            except Exception as e:
                if  time() - run_start > (TIMEOUT / 1000 * 0.9):
                    # Execution timeout
                    _, latency_json = run_query("EXPLAIN (VERBOSE, COSTS, FORMAT JSON, SUMMARY) " + sql, run_args)
                    latency_json = latency_json[0][0]
                    if len(latency_json) == 2:
                        # remove bao's prediction
                        latency_json = [latency_json[1]]
                    latency_json[0]["Execution Time"] = TIMEOUT
                else:
                    raise e

            latency_str = json.dumps(latency_json)
            save_history(sql, encoded_q_str, cur_plan_str, encoded_plan_str, latency_str)

        # 4. save latency
        latency_json[0]['Planning Time'] = planning_time
        if write_latency_file:
            with open(latency_file, "a+") as f:
                fcntl.flock(f, fcntl.LOCK_EX)
                f.write(query_name + SEP + json.dumps(latency_json) + "\n")
                fcntl.flock(f, fcntl.LOCK_UN)

        exec_time = latency_json[0]["Execution Time"]
        print(time(), query_name, exec_time, flush=True)
        return latency_json

    except Exception as e:
        with open(latency_file + "_error", "a+") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            f.write(query_name + "\n")
            f.write(str(e).strip() + "\n")
            fcntl.flock(f, fcntl.LOCK_UN)

def test_query(sql, query_name, run_args, latency_file, write_latency_file = True, manager_dict = None, manager_lock = None):
    sql = sql.strip().replace("\n", " ").replace("\t", " ")

    # 1. run query with pg hint
    _, plan_json = run_query("EXPLAIN (COSTS FALSE, FORMAT JSON, SUMMARY) " + sql, run_args)
    plan_json = plan_json[0][0]
    if len(plan_json) == 2:
        # remove bao's prediction
        plan_json = [plan_json[1]]
    planning_time = plan_json[0]['Planning Time']
    
    cur_plan_str = json.dumps(plan_json[0]['Plan'])
    try:
        if manager_dict is not None and manager_lock is not None:
            manager_lock.acquire()
            if cur_plan_str in manager_dict:
                manager_lock.release()
                print("another process will run this plan:", cur_plan_str)
                return
            else:
                manager_dict[cur_plan_str] = 1
                manager_lock.release()

        # 3. run current query 
        run_start = time()
        # print("Running query:", sql)
        # print("Query name:", query_name)
        # print("Run args:", run_args)
        # print("Planning time:", planning_time)
        try:
            _, latency_json = run_query("EXPLAIN (ANALYZE, TIMING, VERBOSE, COSTS, SUMMARY, FORMAT JSON) " + sql, run_args)
            latency_json = latency_json[0][0]
            if len(latency_json) == 2:
                # remove bao's prediction
                latency_json = [latency_json[1]]
        except Exception as e:
            if  time() - run_start > (TIMEOUT / 1000 * 0.9):
                # Execution timeout
                _, latency_json = run_query("EXPLAIN (VERBOSE, COSTS, FORMAT JSON, SUMMARY) " + sql, run_args)
                latency_json = latency_json[0][0]
                if len(latency_json) == 2:
                    # remove bao's prediction
                    latency_json = [latency_json[1]]
                latency_json[0]["Execution Time"] = TIMEOUT
            else:
                raise e

        latency_str = json.dumps(latency_json)

        # 4. save latency
        latency_json[0]['Planning Time'] = planning_time
        if write_latency_file:
            with open(latency_file, "a+") as f:
                fcntl.flock(f, fcntl.LOCK_EX)
                f.write(query_name + SEP + json.dumps(latency_json) + "\n")
                fcntl.flock(f, fcntl.LOCK_UN)

        exec_time = latency_json[0]["Execution Time"]
        # print(time(), query_name, exec_time, flush=True)
        return latency_json

    except Exception as e:
        with open(latency_file + "_error", "a+") as f:
            raise e
            # fcntl.flock(f, fcntl.LOCK_EX)
            # f.write(query_name + "\n")
            # f.write(str(e).strip() + "\n")
            # fcntl.flock(f, fcntl.LOCK_UN)
