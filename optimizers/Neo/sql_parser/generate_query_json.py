# from sql_parser import parse_sql
import pandas as pd
from typing import Dict, Any
import random
import re
import json
import psycopg2
from .config import DB_CONFIG, get_database_schema
from typing import List
import os
from .sql_parser import parse_sql

DB_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"


import os
from typing import List, Tuple

def read_queries_from_directory_recursively(directory: str) -> Tuple[List[str], List[str]]:
    """
    Recursively read SQL queries from files in the specified directory and its subdirectories.
    
    Args:
        directory: The root directory to search for SQL files.
        
    Returns:
        A tuple containing:
        - List of SQL query strings
        - List of full file paths corresponding to each query
    """
    queries = []
    file_paths = []
    file_names = []
    
    for root, _, files in os.walk(directory):
        for filename in files:
            if filename.endswith(".sql"):
                filepath = os.path.join(root, filename)
                try:
                    with open(filepath, "r") as file:
                        query = file.read().strip()
                        if query:
                            queries.append(query)
                            file_paths.append(filepath)
                            file_names.append(filename)
                except IOError as e:
                    print(f"Error reading file {filepath}: {e}")
                    
    return (queries, file_names, file_paths)

def read_queries_from_directory(directory: str, fileorder: str = None) -> List[str]:
    """Read SQL queries from files in the specified directory, optionally enforcing file order."""
    queries = []
    queryNames = []

    if fileorder is not None:
        try:
            with open(fileorder, "r") as f:
                ordered_files = [line.strip() for line in f if line.strip()]
        except IOError as e:
            raise RuntimeError(f"Error reading file order from {fileorder}: {e}")

        for filename in ordered_files:
            filepath = os.path.join(directory, filename)
            if os.path.isfile(filepath) and filename.endswith(".sql"):
                try:
                    with open(filepath, "r") as file:
                        query = file.read().strip()
                        if query:
                            queries.append(query)
                            queryNames.append(filename)
                except IOError as e:
                    print(f"Error reading file {filename}: {e}")
    else:
        for filename in sorted(os.listdir(directory)):  # Optional: sort for deterministic order if no fileorder
            if filename.endswith(".sql"):
                filepath = os.path.join(directory, filename)
                try:
                    with open(filepath, "r") as file:
                        query = file.read().strip()
                        if query:
                            queries.append(query)
                            queryNames.append(filename)
                except IOError as e:
                    print(f"Error reading file {filename}: {e}")

    return [queries, queryNames]

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

def read_queries_from_directory_recursively(directory: str) -> Tuple[List[str], List[str]]:
    """
    Recursively read SQL queries from files in the specified directory and its subdirectories.
    
    Args:
        directory: The root directory to search for SQL files.
        
    Returns:
        A tuple containing:
        - List of SQL query strings
        - List of full file paths corresponding to each query
    """
    queries = []
    file_paths = []
    file_names = []
    
    for root, _, files in os.walk(directory):
        for filename in files:
            if filename.endswith(".sql"):
                filepath = os.path.join(root, filename)
                try:
                    with open(filepath, "r") as file:
                        query = file.read().strip()
                        if query:
                            queries.append(query)
                            file_paths.append(filepath)
                            file_names.append(filename)
                except IOError as e:
                    print(f"Error reading file {filepath}: {e}")
                    
    return (queries, file_names, file_paths)

def generate_test_query_json(query_name, parsed_query, original_sql, db_schema, file_path):
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
        original_sql,
        file_path
    ]

    return {query_name: query_data}

from tqdm import tqdm

def generate_complete_test_json(queries_dir: str, conn, test_size: float = 1.0) -> Dict[str, Any]:
    """Generate the complete JSON output with train/test split"""
    # Read all queries from directory
    (queries, query_names, file_paths) = read_queries_from_directory_recursively(queries_dir)
    
    print(f"Total queries read: {len(queries)}")
    # Create train/test split with progress indication
    with tqdm(total=1, desc="Splitting queries") as pbar:
        all_indices = list(range(len(file_paths)))
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
    for query, query_name, file_path in tqdm(zip(queries, query_names, file_paths), 
                                total=len(queries),
                                desc="Processing queries"):
        parsed_query = parse_sql(query)
        query_json = generate_test_query_json(query_name, parsed_query, query, scheme, file_path)
        db_data.update(query_json)
    
    print(len(db_data))
    print(len(test_queries))
    # Build final JSON structure
    final_json = {
        "psycopg_connect_url": DB_URL,
        "db": "postgres",
        # "join_types": ["HashJoin", "MergeJoin", "NestLoop"],
        "join_types": ["HashJoin"],
        "db_settings": "BEGIN; SET join_collapse_limit=20; SET from_collapse_limit=20; SET geqo_threshold = 20; COMMIT;",
        "test_queries": test_queries,
        "scheme": scheme,
        "db_data": db_data
    }
    
    return final_json


def generate_final_json(queries_dir: str, conn, test_size: float = 0.0, fileorder: str = None) -> Dict[str, Any]:
    """Generate the complete JSON output with train/test split"""
    # Read all queries from directory
    queries, query_names = read_queries_from_directory(queries_dir, fileorder=fileorder)
    
    print(f"Total queries read: {len(queries)}")
    # Create train/test split with progress indication
    with tqdm(total=1, desc="Splitting queries") as pbar:
        all_indices = list(range(len(query_names)))
        # random.shuffle(all_indices)
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
    
    print(len(db_data))
    print(len(test_queries))
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

from pathlib import Path
REPO_ROOT = Path(__file__).resolve()
while REPO_ROOT.name != "Learned-Optimizers-Benchmarking-Suite" and REPO_ROOT.parent != REPO_ROOT:
    REPO_ROOT = REPO_ROOT.parent

# Example usage:
if __name__ == "__main__":
    # Database connection parameters
    try:
        # Establish database connection
        conn = psycopg2.connect(DB_URL)
        
        # Generate the final JSON.
        queries_directory = f'{REPO_ROOT}/workloads/imdb_pg_dataset/job'
        output_json = generate_final_json(queries_directory, conn)
        
        # Save to file
        with open(f'{REPO_ROOT}/Neo/config/postgres_job_config_alt.json', "w") as f:
            json.dump(output_json, f, indent=4)
        
        print("JSON output with dynamic schema generated successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()