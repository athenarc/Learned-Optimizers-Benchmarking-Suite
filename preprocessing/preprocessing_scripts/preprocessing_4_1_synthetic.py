import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import query_directory, TABLES, DB_CONFIG, connect_to_db, get_alchemy_engine
import pandas as pd
from tqdm import tqdm
from sql_parser import parse_sql
from file_utils import read_queries_from_directory
from query_template import get_query_template_no_correl, get_query_template, get_query_template_no_selectivity
from selectivity import get_database_schema, DatabaseCache
from typing import Dict, Any
from selectivity import fetch_column_data, estimate_selectivity
import tqdm
from pathlib import Path
import psycopg2

movie_company_queries = [
    # 1-join
    """SELECT mc.*
    FROM movie_companies mc, company_name cn
    WHERE mc.company_id = cn.id AND mc.company_id < ?""",
    
    # 2-joins
    """SELECT mc.*
    FROM movie_companies mc, company_name cn, title t
    WHERE mc.company_id = cn.id AND mc.movie_id = t.id AND mc.company_id < ?""",
    
    # 3-joins
    """SELECT mc.*
    FROM movie_companies mc, company_name cn, title t, company_type ct
    WHERE mc.company_id = cn.id AND mc.movie_id = t.id AND mc.company_type_id = ct.id AND mc.company_id < ?""",
    
    # 4-joins
    """SELECT mc.*
    FROM movie_companies mc, company_name cn, title t, company_type ct, kind_type kt
    WHERE mc.company_id = cn.id AND mc.movie_id = t.id AND mc.company_type_id = ct.id AND t.kind_id = kt.id AND mc.company_id < ?""",
    
    # 5-joins
    """SELECT mc.*
    FROM movie_companies mc, company_name cn, title t, company_type ct, kind_type kt, cast_info ci
    WHERE mc.company_id = cn.id AND mc.movie_id = t.id AND mc.company_type_id = ct.id AND t.kind_id = kt.id AND t.id = ci.movie_id AND mc.company_id < ?""",
    
    # 6-joins
    """SELECT mc.*
    FROM movie_companies mc, company_name cn, title t, company_type ct, kind_type kt, cast_info ci, name n
    WHERE mc.company_id = cn.id AND mc.movie_id = t.id AND mc.company_type_id = ct.id AND t.kind_id = kt.id AND t.id = ci.movie_id AND ci.person_id = n.id AND mc.company_id < ?""",
    
    # 7-joins
    """SELECT mc.*
    FROM movie_companies mc, company_name cn, title t, company_type ct, kind_type kt, cast_info ci, name n, role_type rt
    WHERE mc.company_id = cn.id AND mc.movie_id = t.id AND mc.company_type_id = ct.id AND t.kind_id = kt.id AND t.id = ci.movie_id AND ci.person_id = n.id AND ci.role_id = rt.id AND mc.company_id < ?""",
    
    # 8-joins
    """SELECT mc.*
    FROM movie_companies mc, company_name cn, title t, company_type ct, kind_type kt, cast_info ci, name n, role_type rt, char_name chn
    WHERE mc.company_id = cn.id AND mc.movie_id = t.id AND mc.company_type_id = ct.id AND t.kind_id = kt.id AND t.id = ci.movie_id AND ci.person_id = n.id AND ci.role_id = rt.id AND ci.person_role_id = chn.id AND mc.company_id < ?""",
    
    # 9-joins
    """SELECT mc.*
    FROM movie_companies mc, company_name cn, title t, company_type ct, kind_type kt, cast_info ci, name n, role_type rt, char_name chn, movie_keyword mk
    WHERE mc.company_id = cn.id AND mc.movie_id = t.id AND mc.company_type_id = ct.id AND t.kind_id = kt.id AND t.id = ci.movie_id AND ci.person_id = n.id AND ci.role_id = rt.id AND ci.person_role_id = chn.id AND t.id = mk.movie_id AND mc.company_id < ?""",
    
    # 10-joins
    """SELECT mc.*
    FROM movie_companies mc, company_name cn, title t, company_type ct, kind_type kt, cast_info ci, name n, role_type rt, char_name chn, movie_keyword mk, keyword k
    WHERE mc.company_id = cn.id AND mc.movie_id = t.id AND mc.company_type_id = ct.id AND t.kind_id = kt.id AND t.id = ci.movie_id AND ci.person_id = n.id AND ci.role_id = rt.id AND ci.person_role_id = chn.id AND t.id = mk.movie_id AND mk.keyword_id = k.id AND mc.company_id < ?""",
    
    # 11-joins
    """SELECT mc.*
    FROM movie_companies mc, company_name cn, title t, company_type ct, kind_type kt, cast_info ci, name n, role_type rt, char_name chn, movie_keyword mk, keyword k, movie_info mi
    WHERE mc.company_id = cn.id AND mc.movie_id = t.id AND mc.company_type_id = ct.id AND t.kind_id = kt.id AND t.id = ci.movie_id AND ci.person_id = n.id AND ci.role_id = rt.id AND ci.person_role_id = chn.id AND t.id = mk.movie_id AND mk.keyword_id = k.id AND t.id = mi.movie_id AND mc.company_id < ?""",
    
    # 12-joins
    """SELECT mc.*
    FROM movie_companies mc, company_name cn, title t, company_type ct, kind_type kt, cast_info ci, name n, role_type rt, char_name chn, movie_keyword mk, keyword k, movie_info mi, info_type it
    WHERE mc.company_id = cn.id AND mc.movie_id = t.id AND mc.company_type_id = ct.id AND t.kind_id = kt.id AND t.id = ci.movie_id AND ci.person_id = n.id AND ci.role_id = rt.id AND ci.person_role_id = chn.id AND t.id = mk.movie_id AND mk.keyword_id = k.id AND t.id = mi.movie_id AND mi.info_type_id = it.id AND mc.company_id < ?"""
]

cast_info_scans = [
    # 1-join
    """SELECT ci.*
    FROM cast_info ci, name n
    WHERE ci.person_id = n.id AND ci.person_id < ?""",
    
    # 2-joins
    """SELECT ci.*
    FROM cast_info ci, name n, title t
    WHERE ci.person_id = n.id AND ci.movie_id = t.id AND ci.person_id < ?""",
    
    # 3-joins
    """SELECT ci.*
    FROM cast_info ci, name n, title t, kind_type kt
    WHERE ci.person_id = n.id AND ci.movie_id = t.id AND t.kind_id = kt.id AND ci.person_id < ?""",
    
    # 4-joins
    """SELECT ci.*
    FROM cast_info ci, name n, title t, kind_type kt, role_type rt
    WHERE ci.person_id = n.id AND ci.movie_id = t.id AND t.kind_id = kt.id AND ci.role_id = rt.id AND ci.person_id < ?""",
    
    # 5-joins
    """SELECT ci.*
    FROM cast_info ci, name n, title t, kind_type kt, role_type rt, char_name cn
    WHERE ci.person_id = n.id AND ci.movie_id = t.id AND t.kind_id = kt.id AND ci.role_id = rt.id AND ci.person_role_id = cn.id AND ci.person_id < ?""",
    
    # 6-joins
    """SELECT ci.*
    FROM cast_info ci, name n, title t, kind_type kt, role_type rt, char_name cn, movie_companies mc
    WHERE ci.person_id = n.id AND ci.movie_id = t.id AND t.kind_id = kt.id AND ci.role_id = rt.id AND ci.person_role_id = cn.id AND t.id = mc.movie_id AND ci.person_id < ?""",
    
    # 7-joins
    """SELECT ci.*
    FROM cast_info ci, name n, title t, kind_type kt, role_type rt, char_name cn, movie_companies mc, company_name comp
    WHERE ci.person_id = n.id AND ci.movie_id = t.id AND t.kind_id = kt.id AND ci.role_id = rt.id AND ci.person_role_id = cn.id AND t.id = mc.movie_id AND mc.company_id = comp.id AND ci.person_id < ?""",
    
    # 8-joins
    """SELECT ci.*
    FROM cast_info ci, name n, title t, kind_type kt, role_type rt, char_name cn, movie_companies mc, company_name comp, company_type ct
    WHERE ci.person_id = n.id AND ci.movie_id = t.id AND t.kind_id = kt.id AND ci.role_id = rt.id AND ci.person_role_id = cn.id AND t.id = mc.movie_id AND mc.company_id = comp.id AND mc.company_type_id = ct.id AND ci.person_id < ?""",
    
    # 9-joins
    """SELECT ci.*
    FROM cast_info ci, name n, title t, kind_type kt, role_type rt, char_name cn, movie_companies mc, company_name comp, company_type ct, movie_keyword mk
    WHERE ci.person_id = n.id AND ci.movie_id = t.id AND t.kind_id = kt.id AND ci.role_id = rt.id AND ci.person_role_id = cn.id AND t.id = mc.movie_id AND mc.company_id = comp.id AND mc.company_type_id = ct.id AND t.id = mk.movie_id AND ci.person_id < ?""",
    
    # 10-joins
    """SELECT ci.*
    FROM cast_info ci, name n, title t, kind_type kt, role_type rt, char_name cn, movie_companies mc, company_name comp, company_type ct, movie_keyword mk, keyword k
    WHERE ci.person_id = n.id AND ci.movie_id = t.id AND t.kind_id = kt.id AND ci.role_id = rt.id AND ci.person_role_id = cn.id AND t.id = mc.movie_id AND mc.company_id = comp.id AND mc.company_type_id = ct.id AND t.id = mk.movie_id AND mk.keyword_id = k.id AND ci.person_id < ?""",
    
    # 11-joins
    """SELECT ci.*
    FROM cast_info ci, name n, title t, kind_type kt, role_type rt, char_name cn, movie_companies mc, company_name comp, company_type ct, movie_keyword mk, keyword k, movie_info mi
    WHERE ci.person_id = n.id AND ci.movie_id = t.id AND t.kind_id = kt.id AND ci.role_id = rt.id AND ci.person_role_id = cn.id AND t.id = mc.movie_id AND mc.company_id = comp.id AND mc.company_type_id = ct.id AND t.id = mk.movie_id AND mk.keyword_id = k.id AND t.id = mi.movie_id AND ci.person_id < ?""",
    
    # 12-joins
    """SELECT ci.*
    FROM cast_info ci, name n, title t, kind_type kt, role_type rt, char_name cn, movie_companies mc, company_name comp, company_type ct, movie_keyword mk, keyword k, movie_info mi, info_type it
    WHERE ci.person_id = n.id AND ci.movie_id = t.id AND t.kind_id = kt.id AND ci.role_id = rt.id AND ci.person_role_id = cn.id AND t.id = mc.movie_id AND mc.company_id = comp.id AND mc.company_type_id = ct.id AND t.id = mk.movie_id AND mk.keyword_id = k.id AND t.id = mi.movie_id AND mi.info_type_id = it.id AND ci.person_id < ?"""
]

movie_keyword_queries = [
    # 1-join
    """SELECT mk.*
    FROM movie_keyword mk, keyword k
    WHERE mk.keyword_id = k.id AND mk.keyword_id < ?""",
    
    # 2-joins
    """SELECT mk.*
    FROM movie_keyword mk, keyword k, title t
    WHERE mk.keyword_id = k.id AND mk.movie_id = t.id AND mk.keyword_id < ?""",
    
    # 3-joins
    """SELECT mk.*
    FROM movie_keyword mk, keyword k, title t, kind_type kt
    WHERE mk.keyword_id = k.id AND mk.movie_id = t.id AND t.kind_id = kt.id AND mk.keyword_id < ?""",
    
    # 4-joins
    """SELECT mk.*
    FROM movie_keyword mk, keyword k, title t, kind_type kt, movie_companies mc
    WHERE mk.keyword_id = k.id AND mk.movie_id = t.id AND t.kind_id = kt.id AND t.id = mc.movie_id AND mk.keyword_id < ?""",
    
    # 5-joins
    """SELECT mk.*
    FROM movie_keyword mk, keyword k, title t, kind_type kt, movie_companies mc, company_name cn
    WHERE mk.keyword_id = k.id AND mk.movie_id = t.id AND t.kind_id = kt.id AND t.id = mc.movie_id AND mc.company_id = cn.id AND mk.keyword_id < ?""",
    
    # 6-joins
    """SELECT mk.*
    FROM movie_keyword mk, keyword k, title t, kind_type kt, movie_companies mc, company_name cn, company_type ct
    WHERE mk.keyword_id = k.id AND mk.movie_id = t.id AND t.kind_id = kt.id AND t.id = mc.movie_id AND mc.company_id = cn.id AND mc.company_type_id = ct.id AND mk.keyword_id < ?""",
    
    # 7-joins
    """SELECT mk.*
    FROM movie_keyword mk, keyword k, title t, kind_type kt, movie_companies mc, company_name cn, company_type ct, cast_info ci
    WHERE mk.keyword_id = k.id AND mk.movie_id = t.id AND t.kind_id = kt.id AND t.id = mc.movie_id AND mc.company_id = cn.id AND mc.company_type_id = ct.id AND t.id = ci.movie_id AND mk.keyword_id < ?""",
    
    # 8-joins
    """SELECT mk.*
    FROM movie_keyword mk, keyword k, title t, kind_type kt, movie_companies mc, company_name cn, company_type ct, cast_info ci, name n
    WHERE mk.keyword_id = k.id AND mk.movie_id = t.id AND t.kind_id = kt.id AND t.id = mc.movie_id AND mc.company_id = cn.id AND mc.company_type_id = ct.id AND t.id = ci.movie_id AND ci.person_id = n.id AND mk.keyword_id < ?""",
    
    # 9-joins
    """SELECT mk.*
    FROM movie_keyword mk, keyword k, title t, kind_type kt, movie_companies mc, company_name cn, company_type ct, cast_info ci, name n, role_type rt
    WHERE mk.keyword_id = k.id AND mk.movie_id = t.id AND t.kind_id = kt.id AND t.id = mc.movie_id AND mc.company_id = cn.id AND mc.company_type_id = ct.id AND t.id = ci.movie_id AND ci.person_id = n.id AND ci.role_id = rt.id AND mk.keyword_id < ?""",
    
    # 10-joins
    """SELECT mk.*
    FROM movie_keyword mk, keyword k, title t, kind_type kt, movie_companies mc, company_name cn, company_type ct, cast_info ci, name n, role_type rt, char_name chn
    WHERE mk.keyword_id = k.id AND mk.movie_id = t.id AND t.kind_id = kt.id AND t.id = mc.movie_id AND mc.company_id = cn.id AND mc.company_type_id = ct.id AND t.id = ci.movie_id AND ci.person_id = n.id AND ci.role_id = rt.id AND ci.person_role_id = chn.id AND mk.keyword_id < ?""",
    
    # 11-joins
    """SELECT mk.*
    FROM movie_keyword mk, keyword k, title t, kind_type kt, movie_companies mc, company_name cn, company_type ct, cast_info ci, name n, role_type rt, char_name chn, movie_info mi
    WHERE mk.keyword_id = k.id AND mk.movie_id = t.id AND t.kind_id = kt.id AND t.id = mc.movie_id AND mc.company_id = cn.id AND mc.company_type_id = ct.id AND t.id = ci.movie_id AND ci.person_id = n.id AND ci.role_id = rt.id AND ci.person_role_id = chn.id AND t.id = mi.movie_id AND mk.keyword_id < ?""",
    
    # 12-joins
    """SELECT mk.*
    FROM movie_keyword mk, keyword k, title t, kind_type kt, movie_companies mc, company_name cn, company_type ct, cast_info ci, name n, role_type rt, char_name chn, movie_info mi, info_type it
    WHERE mk.keyword_id = k.id AND mk.movie_id = t.id AND t.kind_id = kt.id AND t.id = mc.movie_id AND mc.company_id = cn.id AND mc.company_type_id = ct.id AND t.id = ci.movie_id AND ci.person_id = n.id AND ci.role_id = rt.id AND ci.person_role_id = chn.id AND t.id = mi.movie_id AND mi.info_type_id = it.id AND mk.keyword_id < ?"""
]

def get_selectivity_from_query_template(query_template, db_schema):
    """
    Given a query template, selectivity and the database schema, return queries with similar selectivities
    """
    # Extract the filter conditions from the query template
    filter_conditions = query_template['filter_conditions']
    
    # Initialize a list to store the selectivities
    selectivities = []
    
    # Iterate through the filter conditions
    for condition in filter_conditions:
        # Get the table and column from the condition
        table, column = condition.split('.')
        
        # Fetch the data for the column from the database
        column_data = fetch_column_data(db_schema, table, column)
        
        if column_data is not None:
            # Estimate the selectivity for the condition
            selectivity = estimate_selectivity(column_data, condition)
            selectivities.append(selectivity)
    
    return selectivities

def generate_selectivity_queries(conn, table_name: str, column_name: str, operator: str, 
                              original_query: str, queryName: str, num_joins: int) -> Dict[str, str]:
    """
    Generate queries with filters at different selectivity points (10%, 25%, 75%, 100%)
    and write them to SQL files in the specified directory structure.
    
    Args:
        conn: Active database connection object
        table_name: Name of the table to analyze
        column_name: Name of the column to use for filtering
        operator: The operator used in the original filter (=, >, <, etc.)
        original_query: The base query to modify with filters
        base_output_dir: Base directory for output files
        query_id: Identifier for this query (e.g., '55311')
    
    Returns:
        Dictionary with selectivity levels as keys and file paths as values
    """
    query_id = queryName.split('.')[0]  # Extract the query ID from the filename
    # First check if we have a valid connection
    if conn is None or conn.closed:
        print("Error: Database connection is not available or closed")
        return {}

    # Create directory structure if it doesn't exist
    parent_dir = f'{num_joins}_joins'
    from pathlib import Path
    REPO_ROOT = Path(__file__).resolve()
    while REPO_ROOT.name != "Learned-Optimizers-Benchmarking-Suite" and REPO_ROOT.parent != REPO_ROOT:
        REPO_ROOT = REPO_ROOT.parent    
    BASE_DIR = REPO_ROOT
    query_dir = BASE_DIR / "workloads" / "imdb_pg_dataset" / "experiment4" / "4.1" / parent_dir / query_id
    os.makedirs(query_dir, exist_ok=True)
    
    # Use percentiles to get reliable values without NULLs
    dist_query = f"""
    WITH valid_values AS (
        SELECT {column_name} 
        FROM {table_name}
        WHERE {column_name} IS NOT NULL
    ),
    percentiles AS (
        SELECT 
            percentile_disc(0.01) WITHIN GROUP (ORDER BY {column_name}) AS p01,
            percentile_disc(0.10) WITHIN GROUP (ORDER BY {column_name}) AS p10,
            percentile_disc(0.20) WITHIN GROUP (ORDER BY {column_name}) AS p20,
            percentile_disc(0.40) WITHIN GROUP (ORDER BY {column_name}) AS p40,
            percentile_disc(0.60) WITHIN GROUP (ORDER BY {column_name}) AS p60,
            percentile_disc(0.80) WITHIN GROUP (ORDER BY {column_name}) AS p80,
            percentile_disc(0.99) WITHIN GROUP (ORDER BY {column_name}) AS p99
        FROM valid_values
    )
    SELECT 
        p01 AS "1",
        p10 AS "10",
        p20 AS "20",
        p40 AS "40",
        p60 AS "60",
        p80 AS "80",
        p99 AS "99"
    FROM percentiles;
    """
    
    try:
        # Execute the distribution query
        dist_df = pd.read_sql(dist_query, conn)
        
        if dist_df.empty:
            return {}
        
        # Get sample values for each selectivity range
        samples = {
            '0%': dist_df.iloc[0]['1'],
            '10%': dist_df.iloc[0]['10'],
            '20%': dist_df.iloc[0]['20'],
            '40%': dist_df.iloc[0]['40'],
            '60%': dist_df.iloc[0]['60'],
            '80%': dist_df.iloc[0]['80'],
            '100%': dist_df.iloc[0]['99'],
        }

        generated_files = {}
        
        # Generate queries and write to files
        for key, value in samples.items():
            if pd.isna(value):  # Skip if we got NULL despite precautions
                continue
            
            if value is not None:
                # If the column is not a string column, do the below
                if not isinstance(value, str):
                    # Replace the filter in the base query with the new value
                    modified_query = original_query.replace(
                        f"{column_name} {operator} ?",
                        f"{column_name} {operator} {value}"
                    )
                else:
                    escaped_value = value.replace("'", "''")
                    modified_query = original_query.replace(
                        f"{column_name} {operator} '?'",  # Note the quotes around ?
                        f"{column_name} {operator} '{escaped_value}'"  # Wrap value in quotes
                    )                    
                
                # Determine filename suffix based on selectivity level
                suffix = {
                    '0%': '0',
                    '10%': '10',
                    '20%': '20',
                    '40%': '40',
                    '60%': '60',
                    '80%': '80',
                    '100%': '100'
                }.get(key, 'x')
                
                parent_dir = query_dir / key 
                os.makedirs(parent_dir, exist_ok=True)
                
                # Create the output filename
                output_file = parent_dir / f"{query_id}_{suffix}.sql"
                
                # Write the query to file with proper indentation
                with open(output_file, 'w') as f:
                    f.write(modified_query)
                
                generated_files[key] = str(output_file)
        
        return generated_files
    
    except Exception as e:
        print(f"Error generating selectivity queries: {e}")
        return {}

import time
import json
from sqlalchemy import text

def execute_explain_analyze_default(conn, query: str) -> dict:
    conn = psycopg2.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        dbname=DB_CONFIG['dbname'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password']
    )
    """Execute EXPLAIN ANALYZE on the given query and return the QEP data"""
    try:
        explain_query = f"EXPLAIN (ANALYZE, FORMAT JSON) {query}"
        cursor = conn.cursor()
        cursor.execute(explain_query)
        qep_data = cursor.fetchall()
        
        # Return the QEP data
        return qep_data[0][0]
    except Exception as e:
        print(f"Error executing EXPLAIN ANALYZE: {str(e)}")
        return {}

def execute_explain_analyze_force_seq_scan(conn, query: str, examined_table: str) -> dict:
    conn = psycopg2.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        dbname=DB_CONFIG['dbname'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password']
    )
    """Execute EXPLAIN ANALYZE on the given query and return the QEP data"""
    try:
        pg_hint = f"/*+ SeqScan({examined_table}) */ "
        query = pg_hint + query
        explain_query = f"EXPLAIN (ANALYZE, FORMAT JSON) {query}"
        cursor = conn.cursor()
        cursor.execute(explain_query)
        qep_data = cursor.fetchall()
        
        # Return the QEP data
        return qep_data[0][0]
    except Exception as e:
        print(f"Error executing EXPLAIN ANALYZE: {str(e)}")
        return {}

def execute_explain_analyze_force_index_scan(conn, query: str, examined_table: str) -> dict:
    conn = psycopg2.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        dbname=DB_CONFIG['dbname'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password']
    )
    """Execute EXPLAIN ANALYZE on the given query and return the QEP data"""
    try:
        pg_hint = f"/*+ IndexScan({examined_table}) */ "
        query = pg_hint + query
        explain_query = f"EXPLAIN (ANALYZE, FORMAT JSON) {query}"
        cursor = conn.cursor()
        cursor.execute(explain_query)
        qep_data = cursor.fetchall()
        
        # Return the QEP data
        return qep_data[0][0]
    except Exception as e:
        print(f"Error executing EXPLAIN ANALYZE: {str(e)}")
        return {}
    
def save_qep_to_file(qep_data: dict, output_path: Path):
    """Save the QEP data to a JSON file"""
    try:
        with open(output_path, 'w') as f:
            json.dump(qep_data, f, indent=2)
        print(f"Saved QEP to {output_path}")
    except Exception as e:
        print(f"Error saving QEP to file: {str(e)}")

def process_generated_queries(conn, queries: dict, examined_table: str = ""):
    """Process all generated queries and save their QEPs"""
    for sel_level, query_path in queries.items():
        try:
            # Read the generated query
            with open(query_path, 'r') as f:
                query = f.read().strip()
            
            if not query:
                print(f"Skipping empty query in {query_path}")
                continue
            
            # Get QEP
            qep_data = execute_explain_analyze_default(conn, query)
            if not qep_data:
                continue
            
            # Save to classic_qep.json in the same directory
            output_path = Path(query_path).parent / "classic_qep.json"
            save_qep_to_file(qep_data, output_path)
            
            seq_scan_qep_data = execute_explain_analyze_force_seq_scan(conn, query, examined_table)
            if not seq_scan_qep_data:
                continue
            # Save to classic_qep.json in the same directory
            output_path = Path(query_path).parent / "seq_scan_qep.json"
            save_qep_to_file(seq_scan_qep_data, output_path)
            index_scan_qep_data = execute_explain_analyze_force_index_scan(conn, query, examined_table)

            if not index_scan_qep_data:
                continue
            # Save to classic_qep.json in the same directory
            output_path = Path(query_path).parent / "index_scan_qep.json"
            save_qep_to_file(index_scan_qep_data, output_path)
            
        except Exception as e:
            print(f"Error processing {query_path}: {str(e)}")

import re

if __name__ == "__main__":
    workload = movie_company_queries
    
    count = 0
    for query in workload:
        parsed_query = parse_sql(query)
        examined_table = ""
        if count < len(movie_company_queries):
            queryName = "movie_company_" + str(count) + ".sql"
            examined_table = "mc"
        elif count < len(movie_company_queries) + len(cast_info_scans):
            queryName = "cast_info_" + str(count - len(movie_company_queries)) + ".sql"
            examined_table = "ci"
        else:
            queryName = "movie_keyword_" + str(count - len(movie_company_queries) - len(cast_info_scans)) + ".sql"
            examined_table = "mk"

        num_joins = len(parsed_query['joins'])
        filters = parsed_query['filters']
        aliases = parsed_query['aliases']
        tables = parsed_query['from_tables']
        
        for filter in filters:
            if '?' in filter:
                # This is the filter we want to modify
                filter_condition = filter
                break
        
        # From the filter condition, we can extract the alias and column names
        Alias = filter_condition.split('.')[0]
        column_name = filter_condition.split('.')[1].split(' ')[0]  # Get the column name before the operator
        operator = filter_condition.split(' ')[1]  # Get the operator (e.g., <, >, =)
        
        # Go from the alias to the table name
        for alias, table in aliases.items():
            if alias == Alias:
                table_name = table
                break

        conn = connect_to_db()
        if conn:
            queries = generate_selectivity_queries(conn, table_name, column_name, operator, query, queryName, num_joins)
            print("Generated queries with different selectivities:")
            for sel, q in queries.items():
                print(f"{sel}: {q}")

            process_generated_queries(conn, queries, examined_table)
            conn.close()
        else:
            print("Failed to connect to database")
        count += 1
        if count == 7:
            break