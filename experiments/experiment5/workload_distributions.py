import os
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import colorsys
from matplotlib.colors import to_rgb, to_hex
import matplotlib.patches as mpatches
import random
from pathlib import Path
REPO_ROOT = Path(__file__).resolve()
while REPO_ROOT.name != "Learned-Optimizers-Benchmarking-Suite" and REPO_ROOT.parent != REPO_ROOT:
    REPO_ROOT = REPO_ROOT.parent
PREPROC_DIR = REPO_ROOT / "preprocessing"
IMDB_PG_DATASET_DIR = REPO_ROOT / "workloads" / "imdb_pg_dataset"
JOB_DIR = IMDB_PG_DATASET_DIR / "job"
JOB_D_DIR = IMDB_PG_DATASET_DIR / "job_d"
JOB_EXTENDED_DIR = IMDB_PG_DATASET_DIR / "job_extended"
JOB_LIGHT_DIR = IMDB_PG_DATASET_DIR / "job_light"
JOB_SYNTHETIC_DIR = IMDB_PG_DATASET_DIR / "job_synthetic"
JOB_LERO_DIR = IMDB_PG_DATASET_DIR / "job_lero"

# Constants
optimizers = ['Postgres', 'NEO', 'BAO', 'LOGER', 'FASTgres', 'LERO']

import sys
import os

preproc_path = PREPROC_DIR

# Add it to sys.path if not already there
if preproc_path not in sys.path:
    sys.path.append(preproc_path)

import os
from config import query_directory, TABLES, DB_CONFIG, connect_to_db
from typing import List, Dict, Tuple
from file_utils import read_queries_from_directory, write_query_results_to_file
import pandas as pd
from tqdm import tqdm
from sql_parser import parse_sql
from query_template import get_query_template
from collections import defaultdict
import networkx as nx
import pandas as pd
from sql_parser import parse_sql
import matplotlib.pyplot as plt
from sql_parser import parse_sql
from selectivity import get_database_schema

job = JOB_DIR
job_dynamic = JOB_D_DIR
job_extended = JOB_EXTENDED_DIR
job_light = JOB_LIGHT_DIR
job_synthetic = JOB_SYNTHETIC_DIR
job_lero = JOB_LERO_DIR

job_alts = [job_dynamic, job_extended, job_light, job_synthetic, job_lero]

from query_template import get_query_template_no_correl
from config import connect_to_db
from selectivity import DatabaseCache
import os
import re
from query_template import SQLInfoExtractor
import pglast

def build_query_profile(parsed_query: dict, query_name: str, query_info: dict, query: str, alias_mapping: Dict[str, str] = None) -> dict:
    sql_operator_pattern = re.compile(
        r'(>=|<=|!=|=|>|<|LIKE|like|IN|BETWEEN|IS\s+NOT\s+NULL|IS\s+NULL|NOT\s*(=|LIKE|like|IN|BETWEEN))'
    )

    node = pglast.parse_sql(query)
    extractor = SQLInfoExtractor()
    extractor(node)
    
    info = extractor.info

    selected_columns = []
    for column in parsed_query.get('select_columns', []):
        if '.' in column:
            table, column_name = column.split('.', 1)
            if alias_mapping and table in alias_mapping:
                table = alias_mapping[table]
            selected_columns.append((table, column_name))

    # Process join enumeration
    join_enumeration = []
    for i, join in enumerate(query_info.get('join_details', []), 1):
        join_entry = {
            'join_number': f'join{i}',
            'tables': join['tables'],
            'condition': join['condition'],
            'actual_rows': None
        }
        if 'execution_data' in join:
            join_entry['actual_rows'] = join['execution_data'].get('actual_rows')
        join_enumeration.append(join_entry)

    profile = {
        'query_name': query_name,
        'num_joins': len(parsed_query.get('joins', [])),
        'num_tables': len(set(parsed_query.get('from_tables', []))),
        'selected_columns': sorted(selected_columns),
        'num_columns_selected': len(parsed_query.get('select_columns', [])),
        'num_predicates': len(parsed_query.get('filters', [])),
        'low_selectivity_predicates': 0,
        'high_selectivity_predicates': 0,
        'tables': sorted(info.get('tables', [])),
        'predicates': sorted(info.get('predicates', [])),
        'joins': parsed_query.get('joins', []),
        'join_details': query_info.get('join_details', []),
        'join_enumeration': join_enumeration
    }

    for table, filters in query_info.get('filters', {}).items():
        for column, value in filters.items():
            if isinstance(value, float):  # valid selectivity
                selectivity = value
                if selectivity <= 0.05:
                    profile['low_selectivity_predicates'] += 1
                else:
                    profile['high_selectivity_predicates'] += 1
    
    return profile

def process_queries_in_directory(directory: str, db_cache: DatabaseCache, db_schema: dict[str, list[str]]):
    results = []
    sql_files = [f for f in os.listdir(directory) if f.endswith(".sql")]
    query_profiles = []
    # If the length of sql_files is over 25.000 then sample 25k queries
    # if len(sql_files) > 25000:
    #     sql_files = random.sample(sql_files, 25000)

    for filename in tqdm(sql_files, desc="Processing SQL files", unit="file"):
        filepath = os.path.join(directory, filename)
        try:
            with open(filepath, "r") as file:
                query = file.read().strip()
                if query:
                    parsed_query = parse_sql(query, split_parentheses=True, db_schema=db_schema)
                    aliases = parsed_query.get('aliases', {})
                    query_info = get_query_template_no_correl(parsed_query, db_cache, alias_mapping=aliases, original_query=query)
                    profile = build_query_profile(parsed_query, filename, query_info, query, alias_mapping=aliases)
                    query_profiles.append(profile)
                    results.append((filename, query_info))
        except IOError as e:
            print(f"Error reading file {filename}: {e}")
    
    return results, query_profiles

from typing import List, Dict, Any
from collections import defaultdict
from distribution import MetadataDistribution

TYPES = ["tables", "predicates", "selected_columns", "join_enumeration"]

def build_workload_distributions(query_profiles: List[Dict[str, Any]], use_predefined_bins: bool = False, config: Dict = None):
    """
    Build MetadataDistribution for each feature type from a list of query profiles.
    """
    workload_data = defaultdict(list)

    for profile in query_profiles:
        for t in TYPES:
            values = profile.get(t, [])
            # Store the values as lists (ensure hashability)
            workload_data[t].append(values)

    dists = {}

    for t in TYPES:
        data = workload_data[t]
        if use_predefined_bins and config and "bin_values" in config and t in config["bin_values"]:
            slots = config["bin_values"][t]
            dists[t] = MetadataDistribution(t, data, slots=slots)
        else:
            dists[t] = MetadataDistribution(t, data)

    return dists

from collections import Counter
from scipy.spatial.distance import jensenshannon
import numpy as np

def compute_workload_distance_from_dists(dists1: Dict[str, MetadataDistribution], dists2: Dict[str, MetadataDistribution]) -> float:
    distances = []

    for key in TYPES:
        dist1_obj = dists1.get(key)
        dist2_obj = dists2.get(key)
        
        if dist1_obj is None or dist2_obj is None:
            continue

        dist1 = dist1_obj.get()
        dist2 = dist2_obj.get()

        if type == "selected_columns":
            print(dist2)
            
        import json
        def make_hashable_key(k):
            return json.dumps(k, sort_keys=True)
        
        dist1_serialized = pd.Series(
            {make_hashable_key(k): v for k, v in dist1.items()}
        )
        dist2_serialized = pd.Series(
            {make_hashable_key(k): v for k, v in dist2.items()}
        )

        all_bins = dist1_serialized.index.union(dist2_serialized.index)

        p = np.array([dist1_serialized.get(k, 0.0) for k in all_bins])
        q = np.array([dist2_serialized.get(k, 0.0) for k in all_bins])
        p = p / (p.sum() if p.sum() != 0 else 1)
        q = q / (q.sum() if q.sum() != 0 else 1)
        jsd = jensenshannon(p, q)
        print(f"Jensen-Shannon Divergence for {key}: {jsd}")
        distances.append(jsd)

    return float(np.mean(distances)) if distances else 0.0

def main():
    """Main function to run the workload distribution analysis."""
    print("Initializing database connection and preloading cache...")
    conn = connect_to_db()
    try:
        db_schema = get_database_schema(conn)
        if db_schema is None:
            raise Exception("Failed to fetch database schema")
        print("Fetched DB Schema moving on to caching the tables")
        db_cache = DatabaseCache(conn)
        db_cache.preload_all_tables()
        print("Database cache ready.")        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()

    print("\nProcessing baseline workload: JOB")
    results, job_query_profiles = process_queries_in_directory(query_directory, db_cache, db_schema)
    job_query_profiles = pd.DataFrame(job_query_profiles)
    dists_job_original = build_workload_distributions(job_query_profiles.to_dict(orient='records'))
    print("Baseline workload distributions built.")

    OUTPUT_FILE = "workload_distribution_differences.txt"

    # Clear the output file before writing new results
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

    import warnings
    warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")

    for job_alt in job_alts:
        print(f"Processing {job_alt}")
        results, job_alt_query_profiles = process_queries_in_directory(job_alt, db_cache, db_schema)
        job_alt_query_profiles = pd.DataFrame(job_alt_query_profiles)
        dists_job_alt = build_workload_distributions(job_alt_query_profiles.to_dict(orient='records'))
        distance = compute_workload_distance_from_dists(dists_job_original, dists_job_alt)
        benchmark_name = os.path.basename(job_alt)
        print(f"Workload distance (Jensen-Shannon Divergence) between JOB and {benchmark_name}: {distance}")
        # Write the distance to the output file
        with open(OUTPUT_FILE, "a") as f:
            f.write(f"Workload distance (Jensen-Shannon Divergence) between JOB and {benchmark_name}: {distance}\n")

if __name__ == "__main__":
    main()