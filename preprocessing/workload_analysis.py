#!/usr/bin/env python
# coding: utf-8

# In[1]:


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


# In[2]:
from pathlib import Path
REPO_ROOT = Path(__file__).resolve()
while REPO_ROOT.name != "Learned-Optimizers-Benchmarking-Suite" and REPO_ROOT.parent != REPO_ROOT:
    REPO_ROOT = REPO_ROOT.parent
JOB_DIR = REPO_ROOT / "workloads" / "imdb_pg_dataset" / "job"

query_directory = JOB_DIR
def analyze_workload(queries: List[str], queryNames: List[str]) -> Tuple[Dict[str, int], Dict[str, int], nx.Graph, List[Tuple[str, nx.Graph]], Dict[Tuple[str, str], float]]:
    """Analyze the workload of SQL queries."""
    table_touch = defaultdict(int)
    column_touch = defaultdict(int)
    query_connect_graph = nx.Graph()
    per_query_graphs = []
    connection_counts = defaultdict(int)
    
    for query in queries:
        # Print the query Id
        parsed_query = parse_sql(query, split_parentheses=True)
        query_graph = nx.Graph()

        aliases = parsed_query.get('aliases', {})
        
        for table in parsed_query['from_tables']:
            table_touch[table] += 1
        
        for column in parsed_query['select_columns']:
            if '.' not in column:
                # print(query)
                # print(parsed_query)
                continue
            
            table, column_name = column.split('.', 1)
            column_touch[column] += 1
        
        for join in parsed_query['joins']:
            if '(' in join:
                print(join)
            
            table_1, table_2_condition = join.split('=', 1)
            table_1 = table_1.strip().split('.')[0]
            table_2 = table_2_condition.strip().split('.')[0]
            
            for alias, table_name in aliases.items():
                if table_1 == alias:
                    table_1 = table_name
                if table_2 == alias:
                    table_2 = table_name
            
            query_connect_graph.add_edge(table_1, table_2)
            query_graph.add_edge(table_1, table_2)
            connection_counts[(table_1, table_2)] += 1
        
        per_query_graphs.append([queryNames[queries.index(query)], query_graph])
    
    # Calculate proportions for table connections
    total_queries = len(queries)
    connection_proportions = {(table1, table2): count / total_queries for (table1, table2), count in connection_counts.items()}
    
    return table_touch, column_touch, query_connect_graph, per_query_graphs, connection_proportions


# In[3]:


[queries,queryNames] = read_queries_from_directory(query_directory)
table_touch, column_touch, query_connect_graph, per_query_graphs, connection_proportions = analyze_workload(queries, queryNames)

# Convert column touch to DataFrame
df_column_touch = pd.DataFrame(list(column_touch.items()), columns=['Column', 'Touch Count'])

# Sort by touch count in descending order
df_column_touch = df_column_touch.sort_values(by='Touch Count', ascending=False)

# Calculate proportion compared to total number of queries
total_queries = len(queries)
df_column_touch['Proportion'] = df_column_touch['Touch Count'] / total_queries


# In[4]:


# Convert table touch to DataFrame
df_table_touch = pd.DataFrame(list(table_touch.items()), columns=['Table', 'Touch Count'])

# Sort by touch count in descending order
df_table_touch = df_table_touch.sort_values(by='Touch Count', ascending=False)

# Calculate proportion compared to total number of queries
total_queries = len(queries)
df_table_touch['Proportion'] = df_table_touch['Touch Count'] / total_queries


# In[5]:


def visualize_query_connect_graph(
    query_connect_graph: nx.Graph,
    table_touch: Dict[str, int],
    total_queries: int,
    connection_proportions: Dict[Tuple[str, str], float]
):

    table_proportions = {table: count / total_queries for table, count in table_touch.items()}
    print("Table Proportions:", table_proportions)
    node_sizes = [table_proportions.get(table, 0) * 10000 for table in query_connect_graph.nodes()]
    print("Node Sizes:", node_sizes)
    node_labels = {table: f"{table}\n{proportion:.2f}" for table, proportion in table_proportions.items()}
    node_labels = {table: label for table, label in node_labels.items() if table in query_connect_graph.nodes()}
    plt.figure(figsize=(16, 12))
    pos = nx.spring_layout(query_connect_graph, k=0.5, iterations=100, scale=2)
    
    nx.draw(
        query_connect_graph,
        pos,
        with_labels=True,
        labels=node_labels,
        node_size=node_sizes,
        node_color='lightblue',
        font_size=10,
        font_weight='bold',
        edge_color='gray',
        width=1.5
    )
    
    edge_labels = {(u, v): f"{connection_proportions.get((u, v), 0):.3f}" for u, v in query_connect_graph.edges()}
    nx.draw_networkx_edge_labels(query_connect_graph, pos, edge_labels=edge_labels, font_color='red')
    
    plt.title("Query Connect Graph: Node Size = Table Touch Proportion, Edge Labels = Join Frequency", fontsize=16)
    plt.show()
    
visualize_query_connect_graph(query_connect_graph, table_touch, len(queries), connection_proportions)


# In[ ]:


from query_template import get_query_template_no_correl
from config import connect_to_db
from selectivity import DatabaseCache
import os

def process_queries_in_directory_cached_db(directory: str, db_cache: DatabaseCache):
    results = []
    sql_files = [f for f in os.listdir(directory) if f.endswith(".sql")]
    # sql_files = ['31c.sql']
    for filename in tqdm(sql_files, desc="Processing SQL files", unit="file"):
        print(f"Processing {filename}")
        filepath = os.path.join(directory, filename)
        
        try:
            with open(filepath, "r") as file:
                query = file.read().strip()
                if query:
                    parsed_query = parse_sql(query, split_parentheses=True)
                    aliases = parsed_query.get('aliases', {})
                    query_info = get_query_template_no_correl(parsed_query, db_cache, alias_mapping=aliases)
                    print(f"Query Info for {filename}: {query_info}")
                    results.append((filename, query_info))
        except IOError as e:
            print(f"Error reading file {filename}: {e}")
    
    return results

# In[9]:

query_directory = JOB_DIR
conn = connect_to_db()
db_cache = DatabaseCache(conn)
db_cache.preload_all_tables()
conn.close()

query_templates = process_queries_in_directory_cached_db(query_directory, db_cache)
write_query_results_to_file(query_templates, "job_query_templates.txt")

