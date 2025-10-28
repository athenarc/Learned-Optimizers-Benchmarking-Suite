# config.py
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from pathlib import Path
import os

# Database schema (for reference)
TABLES = {
    "aka_name": ["id", "person_id", "name", "imdb_index", "name_pcode_cf", "name_pcode_nf", "surname_pcode", "md5sum"],
    "aka_title": ["id", "movie_id", "title", "imdb_index", "kind_id", "production_year", "phonetic_code", "episode_of_id", "season_nr", "episode_nr", "note", "md5sum"],
    "cast_info": ["id", "person_id", "movie_id", "person_role_id", "note", "nr_order", "role_id"],
    "char_name": ["id", "name", "imdb_index", "imdb_id", "name_pcode_nf", "surname_pcode", "md5sum"],
    "comp_cast_type": ["id", "kind"],
    "company_name": ["id", "name", "country_code", "imdb_id", "name_pcode_nf", "name_pcode_sf", "md5sum"],
    "company_type": ["id", "kind"],
    "complete_cast": ["id", "movie_id", "subject_id", "status_id"],
    "info_type": ["id", "info"],
    "keyword": ["id", "keyword", "phonetic_code"],
    "kind_type": ["id", "kind"],
    "link_type": ["id", "link"],
    "movie_companies": ["id", "movie_id", "company_id", "company_type_id", "note"],
    "movie_info": ["id", "movie_id", "info_type_id", "info", "note"],
    "movie_info_idx": ["id", "movie_id", "info_type_id", "info", "note"],
    "movie_keyword": ["id", "movie_id", "keyword_id"],
    "movie_link": ["id", "movie_id", "linked_movie_id", "link_type_id"],
    "name": ["id", "name", "imdb_index", "imdb_id", "gender", "name_pcode_cf", "name_pcode_nf", "surname_pcode", "md5sum"],
    "person_info": ["id", "person_id", "info_type_id", "info", "note"],
    "role_type": ["id", "role"],
    "title": ["id", "title", "imdb_index", "kind_id", "production_year", "imdb_id", "phonetic_code", "episode_of_id", "season_nr", "episode_nr", "series_years", "md5sum"]
}

from dotenv import load_dotenv

def load_repo_env():
    """Load .env from repo root, even if this script is nested deeply."""
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
    'host': os.getenv("DB_HOST", "localhost"),
    'port': int(os.getenv("DB_PORT", 5469)),
    'user': os.getenv("DB_USER", "suite_user"),
    'password': os.getenv("DB_PASS", "")
}

# Optional: dataset-specific port logic
def get_db_port(db_name: str) -> int:
    db_lower = db_name.lower()
    port_map = {
        "imdb": os.getenv("IMDB_PORT"),
        "tpch": os.getenv("TPCH_PORT"),
        "tpcds": os.getenv("TPCDS_PORT"),
        "ssb": os.getenv("SSB_PORT"),
        "stack": os.getenv("STACK_PORT")
    }
    return int(port_map.get(db_lower, DB_CONFIG['port']))

query_directory = "../workloads/imdb_pg_dataset/job"

def connect_to_db():
    """Connect to the PostgreSQL database using SQLAlchemy."""
    try:
        db_url = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"        
        engine = create_engine(db_url)
        conn = engine.connect()
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

from sqlalchemy.pool import QueuePool

def get_alchemy_engine(db_name: str, pool_size=5, max_overflow=2):
    """Connect to the PostgreSQL database using SQLAlchemy with connection pooling.
    
    Args:
        pool_size: Number of permanent connections in the pool
        max_overflow: Number of connections allowed beyond pool_size
        pool_recycle: Recycle connections after this many seconds
        
    Returns:
        SQLAlchemy engine instance or None if connection fails
    """
    try:
        db_url = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{db_name}"
        
        engine = create_engine(
            db_url,
            poolclass=QueuePool,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_recycle=3600,
            pool_pre_ping=True  # Test connections for liveness before use
        )
            
        return engine
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None


def get_schema():
    """Return the database schema."""
    return TABLES