import os
from dotenv import load_dotenv

def load_repo_env():
    """Load .env from the repo root, even if this file is deeply nested."""
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

load_repo_env()

# ✅ Database name (default to imdbload unless overridden)
DB = "imdbload"
HOST = os.getenv("DB_HOST", "localhost")
USER = os.getenv("DB_USER", "postgres")
PASSWORD = os.getenv("DB_PASS", "")

db_lower = DB.lower()
if "imdb" in db_lower:
    PORT = int(os.getenv("IMDB_PORT", 5471))
elif "tpch" in db_lower or "tpc_h" in db_lower:
    PORT = int(os.getenv("TPCH_PORT", 5471))
elif "tpcds" in db_lower or "tpc_ds" in db_lower:
    PORT = int(os.getenv("TPCDS_PORT", 5471))
elif "ssb" in db_lower:
    PORT = int(os.getenv("SSB_PORT", 5468))
elif "stack" in db_lower:
    PORT = int(os.getenv("STACK_PORT", 5471))
else:
    PORT = 5432  # fallback

# DB = "tpcds"
CONNECTION_STR = "dbname=" + DB + " user=" + USER + " password=" + PASSWORD + " host=" + HOST + " port=" + str(PORT)
DATABASE_URL = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}"
RESTART_UDF_NAME = "clear_cache"
TIMEOUT = 30000000
# [important]
# the data directory of your Postgres in which the database data will live 
# you can execute "show data_directory" in psql to get it
# Please ensure this path is correct, 
# because the program needs to write cardinality files to it 
# to make the optimizer generate some specific execution plans of each query.
PG_DB_PATH = "/app/db"
CARDINALITY_FILE_REPOSITORY = "/home/postgres/lero_files"
# Rap conf (No modification is required by default)
LERO_SERVER_PORT = 14567
LERO_SERVER_HOST = "195.251.63.231"
LERO_SERVER_PATH = "../"
LERO_DUMP_CARD_FILE = "dump_card_with_score.txt"

# Test conf (No modification is required by default)
LOG_PATH = "./log/query_latency"
SEP = "#####"