# config.py
from sqlalchemy import create_engine
import pandas as pd

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

# Database connection parameters
DB_CONFIG = {
    'dbname': 'imdbload',
    'user': 'suite_user',
    'password': '71Vgfi4mUNPm',
    'host': 'train.darelab.athenarc.gr',
    'port': 5469
}

query_directory = "/data/hdd1/users/kmparmp/workloads/job"

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

def get_alchemy_engine():
    """Connect to the PostgreSQL database using SQLAlchemy."""
    try:
        db_url = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"        
        engine = create_engine(db_url)
        return engine
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None


def get_schema():
    """Return the database schema."""
    return TABLES

DB_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"

def get_database_schema(conn):
    """Extract the complete database schema from PostgreSQL"""
    schema = {}
    
    # Query to get all tables and their columns
    query = """
    SELECT 
        table_name, 
        column_name,
        data_type,
        is_nullable,
        column_default
    FROM 
        information_schema.columns
    WHERE 
        table_schema = 'public'
    ORDER BY 
        table_name, 
        ordinal_position;
    """
    
    try:
        # Execute the query and fetch all results
        df = pd.read_sql(query, conn)
        
        # Group by table and collect columns
        for table_name, group in df.groupby('table_name'):
            # Get just the column names for the schema
            schema[table_name] = group['column_name'].tolist()
            
        return schema
    
    except Exception as e:
        print(f"Error fetching database schema: {e}")
        return None
