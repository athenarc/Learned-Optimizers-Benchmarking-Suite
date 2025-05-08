import logging
import json
import re
from utils.db_utils import explain_plan_parser_augmented
from collections import defaultdict, OrderedDict
from copy import deepcopy
from pathlib import Path

import psycopg2
from moz_sql_parser import parse

from plan import Plan

# Database connection settings
DB_SETTINGS = """BEGIN;
                SET join_collapse_limit=20;
                SET from_collapse_limit=20;
                SET statement_timeout = 300000;
                COMMIT;
                """

LOG = logging.getLogger(__name__)

# Planner settings to generate different query plans
planner_settings = [
    # Baseline settings (default behavior)
    {},

    # Disable specific scan methods (force alternative access paths)
    {"enable_bitmapscan": "off"},
    {"enable_indexscan": "off"},
    {"enable_indexonlyscan": "off"},
    {"enable_tidscan": "off"},

    # Disable specific join methods (force alternative join strategies)
    {"enable_hashjoin": "off"},
    {"enable_mergejoin": "off"},
    {"enable_nestloop": "off"},

    # Combinations of disabled scan methods
    {"enable_bitmapscan": "off", "enable_indexscan": "off"},
    {"enable_indexscan": "off", "enable_indexonlyscan": "off"},
    {"enable_tidscan": "off", "enable_bitmapscan": "off"},

    # Combinations of disabled join methods
    {"enable_hashjoin": "off", "enable_mergejoin": "off"},
    {"enable_mergejoin": "off", "enable_nestloop": "off"},
    {"enable_hashjoin": "off", "enable_nestloop": "off"},

    # Adjust cost constants (impact plan selection)
    {"seq_page_cost": "1.0", "random_page_cost": "4.0"},  # Default
    {"seq_page_cost": "1.5", "random_page_cost": "2.0"},  # Favor random access
    {"cpu_tuple_cost": "0.01", "cpu_index_tuple_cost": "0.005"},  # Default
    {"cpu_tuple_cost": "0.02", "cpu_index_tuple_cost": "0.01"},  # Higher CPU costs
    {"cpu_operator_cost": "0.0025"},  # Default
    {"cpu_operator_cost": "0.005"},  # Higher operator cost
    {"parallel_tuple_cost": "0.1"},  # Default
    {"parallel_tuple_cost": "0.2"},  # Higher parallel cost
    {"parallel_setup_cost": "1000.0"},  # Default
    {"parallel_setup_cost": "2000.0"},  # Higher parallel setup cost

    # Combinations of cost constants
    {"seq_page_cost": "1.0", "random_page_cost": "4.0", "cpu_tuple_cost": "0.01"},
    {"seq_page_cost": "1.5", "random_page_cost": "2.0", "cpu_tuple_cost": "0.02"},
    {"cpu_tuple_cost": "0.01", "cpu_index_tuple_cost": "0.005", "cpu_operator_cost": "0.0025"},
    {"cpu_tuple_cost": "0.02", "cpu_index_tuple_cost": "0.01", "cpu_operator_cost": "0.005"},

    # # Adjust JIT settings (impact JIT compilation behavior)
    # {"jit_above_cost": "100000"},  # Default
    # {"jit_above_cost": "200000"},  # Less aggressive JIT
    # {"jit_inline_above_cost": "500000"},  # Default
    # {"jit_inline_above_cost": "1000000"},  # Less aggressive inlining
    # {"jit_optimize_above_cost": "500000"},  # Default
    # {"jit_optimize_above_cost": "1000000"},  # Less aggressive optimization

    # # Combinations of JIT settings
    # {"jit_above_cost": "100000", "jit_inline_above_cost": "500000"},
    # {"jit_above_cost": "200000", "jit_optimize_above_cost": "1000000"},

    # # Genetic Query Optimizer (GEQO) settings (impact join ordering)
    # {"geqo": "on"},  # Enable GEQO
    # {"geqo_threshold": "12"},  # Default threshold
    # {"geqo_threshold": "8"},  # Lower threshold (more queries use GEQO)
    # {"geqo_effort": "5"},  # Default effort
    # {"geqo_effort": "10"},  # Higher effort (more exhaustive search)
    # {"geqo_selection_bias": "2.0"},  # Default bias
    # {"geqo_selection_bias": "1.5"},  # Lower bias (less selective)
    # {"geqo_seed": "0.5"},  # Fixed seed for reproducibility

    # # Combinations of GEQO settings
    # {"geqo": "on", "geqo_threshold": "8", "geqo_effort": "10"},
    # {"geqo": "on", "geqo_selection_bias": "1.5", "geqo_seed": "0.5"},

    # Mixed combinations of scan/join methods, cost constants, and GEQO
    {"enable_hashjoin": "off", "enable_mergejoin": "off", "seq_page_cost": "1.5", "random_page_cost": "2.0"},
    {"enable_indexscan": "off", "enable_bitmapscan": "off", "cpu_tuple_cost": "0.02", "cpu_index_tuple_cost": "0.01"},
    {"geqo": "on", "geqo_threshold": "8", "jit_above_cost": "200000"},
    {"enable_nestloop": "off", "enable_hashjoin": "off", "parallel_tuple_cost": "0.2", "parallel_setup_cost": "2000.0"},
]

# Function to generate a hint/setting string for the filename
def generate_hint_string(settings):
    if not settings:
        return "default"
    hint_parts = []
    for key, value in settings.items():
        hint_parts.append(f"{key}_{value}")
    return "_".join(hint_parts)

# Function to build and save optimizer plans
def build_and_save_optimizer_plans_augmented(env_config, path):
    db_data = env_config["db_data"]

    conn = psycopg2.connect(env_config["psycopg_connect_url"])

    for q in db_data.keys():
        p = Plan(*db_data[q])
        # Generate and save multiple query plans using the augmented parser
        generated_plans, _ = explain_plan_parser_augmented(
            p, p.initial_query, conn, exec_time=False, planner_settings=planner_settings
        )
        # Save each generated plan with a unique filename
        for i, plan in enumerate(generated_plans):
            hint_string = generate_hint_string(planner_settings[i])
            # q should be seperated from its .sql suffix and hint_string should be appended
            q = re.sub(r"\.sql$", "", q)
            plan_file = path / f"{q}_plan_{hint_string}.sql.json"
            plan.save(plan_file)        
    
    # Close the database connection
    conn.close()

# Main function to generate and save query plans with different settings
def main():
    # Database connection
    env_config_path = '/data/hdd1/users/kmparmp/Neo/config/postgres_env_config.json'
    with open(env_config_path, "r") as f:
        env_config = json.load(f)

    test_set = env_config['test_queries']
    env_config['db_data'] = {
        k: v for k, v in env_config['db_data'].items() if not k in test_set}
    
    output_dir = Path("runs/postgresql/optimizer")        
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    build_and_save_optimizer_plans_augmented(env_config, output_dir)

if __name__ == '__main__':
    main()