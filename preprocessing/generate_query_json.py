from sql_parser import parse_sql
from file_utils import read_queries_from_directory
from selectivity import get_database_schema
from typing import Dict, Any
import random
import re
import json
import psycopg2
from config import DB_CONFIG
from pathlib import Path
REPO_ROOT = Path(__file__).resolve()
while REPO_ROOT.name != "Learned-Optimizers-Benchmarking-Suite" and REPO_ROOT.parent != REPO_ROOT:
    REPO_ROOT = REPO_ROOT.parent
BASE_DIR = REPO_ROOT

DB_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"

def generate_query_json(query_name, parsed_query, original_sql, db_schema):
    """
    Generate the JSON output with exact alias matching in conditions
    """
    # Extract tables and ensure they're sorted
    tables = sorted(parsed_query['from_tables'])
    
    # Get the aliases dictionary in format {alias: table}
    aliases = parsed_query.get('aliases', {})

    # Create reverse mapping from table to aliases
    table_to_aliases = {}
    for alias, table in aliases.items():
        if table not in table_to_aliases:
            table_to_aliases[table] = []
        table_to_aliases[table].append(alias)

    # Create mapping from column to possible tables (for unqualified columns)
    column_to_tables = {}
    if db_schema:
        for table, columns in db_schema.items():
            for column in columns:
                if column not in column_to_tables:
                    column_to_tables[column] = []
                column_to_tables[column].append(table)

    # Process conditions to use aliases
    conditions = []

    def get_involved_tables(condition):
        """Find which tables/aliases are used in a condition"""
        involved = set()
        
        # Case 1: Find all qualified column references (alias.column or table.column)
        qualified_refs = re.findall(r'([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)', condition)
        seen_columns = set()

        for qualifier, column in qualified_refs:
            seen_columns.add(column)  # Mark this column as seen in a qualified reference
            # Check if it's an alias
            if qualifier in aliases:
                involved.add(qualifier)
            else:
                # Check if it's a full table name
                for table in tables:
                    if qualifier == table:
                        involved.add(table)
                        break

        # Now find truly unqualified columns (those not part of any qualified reference)
        # We need to exclude column names that appeared after a dot in qualified references
        all_words = re.findall(r'([a-zA-Z_][a-zA-Z0-9_]*)', condition)
        unqualified_cols = set()
        
        for i, word in enumerate(all_words):
            # Skip if this is a qualifier (followed by dot)
            if i < len(all_words)-1 and re.search(r'\.\s*$', condition.split(word)[1]):
                continue
            # Only consider if not seen as a column in qualified reference
            if word not in seen_columns:
                unqualified_cols.add(word)
        
        # Process truly unqualified columns
        for column in unqualified_cols:
            if column in column_to_tables:
                possible_tables = column_to_tables[column]
                for table in possible_tables:
                    if table in tables:
                        if table in aliases.values():
                            alias = [a for a, t in aliases.items() if t == table][0]
                            involved.add(alias)
                        else:
                            involved.add(table)
        
        return sorted(involved)

    # Process filters
    for filt in parsed_query['filters']:
        involved_aliases = get_involved_tables(filt)
        conditions.append({
            "names": involved_aliases,
            "condition": filt
        })

    # Process joins
    for join in parsed_query['joins']:
        involved_aliases = get_involved_tables(join)
        conditions.append({
            "names": involved_aliases,
            "condition": join
        })

    # Build final structure
    query_data = [
        tables,
        aliases,
        conditions,
        original_sql
    ]

    return {query_name: query_data}


from tqdm import tqdm

def generate_final_json(queries_dir: str, conn, test_size: float = 0.3) -> Dict[str, Any]:
    """Generate the complete JSON output with train/test split"""
    # Read all queries from directory
    queries, query_names = read_queries_from_directory(queries_dir)
    
    # Create train/test split with progress indication
    with tqdm(total=1, desc="Splitting queries") as pbar:
        all_indices = list(range(len(query_names)))
        random.shuffle(all_indices)
        split_point = int(len(all_indices) * test_size)
        test_indices = all_indices[:split_point]
        test_queries = [query_names[i] for i in test_indices]
        pbar.update(1)

    # Get database schema dynamically with progress indication
    with tqdm(total=1, desc="Fetching schema") as pbar:
        scheme = get_database_schema(conn)
        if scheme is None:
            raise Exception("Failed to fetch database schema")
        pbar.update(1)

    # Process all queries and build db_data with progress bar
    db_data = {}
    for query, query_name in tqdm(zip(queries, query_names), 
                                total=len(queries),
                                desc="Processing queries"):
        parsed_query = parse_sql(query)
        query_json = generate_query_json(query_name, parsed_query, query, scheme)
        db_data.update(query_json)
    

    # Build final JSON structure
    final_json = {
        "psycopg_connect_url": DB_URL,
        "db": "postgres",
        "join_types": ["HashJoin", "MergeJoin", "NestLoop"],
        "db_settings": "BEGIN; SET join_collapse_limit=20; SET from_collapse_limit=20; SET geqo_threshold = 20; COMMIT;",
        "test_queries": test_queries,
        "scheme": scheme,
        "db_data": db_data
    }
    
    return final_json

# Example usage:
if __name__ == "__main__":
    # Database connection parameters
    try:
        # Establish database connection
        conn = psycopg2.connect(DB_URL)
        
        # Generate the final JSON
        WORKLOADS_DIR = BASE_DIR / "workloads" / "imdb_pg_dataset" / "job"
        queries_directory = WORKLOADS_DIR
        output_json = generate_final_json(queries_directory, conn)
        
        # Save to file
        output_dir = BASE_DIR / "output"
        output_dir.mkdir(parents=True, exist_ok=True)
        with open(output_dir / "postgres_job_config_alt.json", "w") as f:
            json.dump(output_json, f, indent=4)
        
        print("JSON output with dynamic schema generated successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()