import psycopg2
import configparser
import linecache
from itertools import chain
import threading
import json
import os
from dotenv import load_dotenv
from pathlib import Path

def load_repo_env():
    """Load .env from the repo root, even if this file is deeply nested."""
    current_dir = Path(__file__).resolve().parent
    while True:
        env_path = current_dir / ".env"
        if env_path.exists():
            load_dotenv(env_path)
            break
        if current_dir.parent == current_dir:
            raise FileNotFoundError(".env file not found in any parent directory.")
        current_dir = current_dir.parent

load_repo_env()

DB_CONFIG = {
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASS", ""),
    "dbname": os.getenv("DB_NAME", "postgres"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
}

class Conn:
    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG, client_encoding="utf-8")
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM pg_stats WHERE schemaname = 'public';")
        self.stats = cursor.fetchall()

    def reconnect(self):
        self.conn.close()
        self.conn = psycopg2.connect(**DB_CONFIG, client_encoding="utf-8")


class Connection:
    _instance_lock = threading.Lock()

    def __init__(self):
        pass

    def __new__(cls, *args, **kwargs):
        if not hasattr(Connection, "_instance"):
            with Connection._instance_lock:
                if not hasattr(Connection, "_instance"):
                    Connection._instance = object.__new__(cls)
        return Connection._instance

    @staticmethod
    def get_connection():
        """Return a new PostgreSQL connection."""
        return psycopg2.connect(**DB_CONFIG, client_encoding="utf-8")

    # -------------------------------------------------------------------------
    # JSON caching utilities (stored relative to repo root)
    # -------------------------------------------------------------------------
    @staticmethod
    def _get_cache_path(filename: str) -> Path:
        repo_root = Path(__file__).resolve().parents[1]  # one level above Connection.py
        cache_dir = repo_root / "cache"
        cache_dir.mkdir(exist_ok=True)
        return cache_dir / filename

    @staticmethod
    def get_all_attribute_name():
        cache_path = Connection._get_cache_path("all_attribute.json")
        if cache_path.exists():
            return json.load(open(cache_path, encoding="utf-8"))

        conn = Connection.get_connection()
        cur = conn.cursor()
        sql_command = "SELECT table_name, column_name FROM information_schema.columns WHERE table_schema='public';"
        cur.execute(sql_command)
        all_tables = cur.fetchall()

        total_attributes, simple_attributes = {}, {}
        for count, (table, col) in enumerate(all_tables):
            total_attributes[f"{table}.{col}"] = count
            simple_attributes[col] = [table, count]

        with open(cache_path, "w") as f:
            json.dump([total_attributes, simple_attributes], f)

        conn.close()
        return [total_attributes, simple_attributes]

    @staticmethod
    def get_all_tables():
        cache_path = Connection._get_cache_path("all_tables.json")
        if cache_path.exists():
            return json.load(open(cache_path, encoding="utf-8"))

        conn = Connection.get_connection()
        cur = conn.cursor()
        sql_command = "SELECT tablename FROM pg_tables WHERE tablename NOT LIKE 'pg%' ORDER BY tablename;"
        cur.execute(sql_command)
        all_tables = [r[0] for r in cur.fetchall()]
        dict_tables = {name: idx for idx, name in enumerate(all_tables)}

        with open(cache_path, "w") as f:
            json.dump(dict_tables, f)

        conn.close()
        return dict_tables

    @staticmethod
    def get_all_tables_rows_num():
        cache_path = Connection._get_cache_path("all_tables_rows_num.json")
        if cache_path.exists():
            return json.load(open(cache_path, encoding="utf-8"))

        conn = Connection.get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT relname, reltuples 
            FROM pg_class r 
            JOIN pg_namespace n ON relnamespace = n.oid 
            WHERE relkind = 'r' AND n.nspname = 'public';
        """)
        all_tables_rows_num_dic = {name: rows for name, rows in cur.fetchall()}

        with open(cache_path, "w") as f:
            json.dump(all_tables_rows_num_dic, f)

        conn.close()
        return all_tables_rows_num_dic


# -----------------------------------------------------------------------------
# Initialize and preload metadata
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    conn = Conn()
    all_attribute_name = Connection.get_all_attribute_name()
    print("All attribute names:", len(all_attribute_name[0]))
    all_tables = Connection.get_all_tables()
    all_tables_rows_num = Connection.get_all_tables_rows_num()
    print("Tables:", list(all_tables.keys())[:5], "...")