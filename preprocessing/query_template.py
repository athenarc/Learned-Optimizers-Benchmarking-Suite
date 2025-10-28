import re
from collections import defaultdict
from typing import Dict, Set, List
from selectivity import fetch_column_data, estimate_selectivity, calculate_correlation, estimate_selectivity_for_null, DatabaseCache
from sqlalchemy import create_engine
from config import TABLES
import pandas as pd
from typing import Dict, Any

def analyze_underrepresented_templates(workload_templates, threshold=0.1):
    """
    Analyze the workload to identify underrepresented joins and filters.
    
    Args:
        workload_templates (List[Dict]): List of query templates in the workload.
        threshold (float): Threshold for underrepresentation (default: 0.1 or 10%).
    
    Returns:
        Dict: A dictionary containing underrepresented joins and filters for each table.
    """
    # Step 1: Calculate connection proportions
    connection_counts = defaultdict(int)
    total_queries = len(workload_templates)

    for query_template in workload_templates:
        for table, joins in query_template['joins'].items():
            for join_table in joins:
                connection_counts[(table, join_table)] += 1

    connection_proportions = {(table1, table2): count / total_queries for (table1, table2), count in connection_counts.items()}

    # Step 2: Calculate filter frequencies
    filter_counts = defaultdict(int)

    for query_template in workload_templates:
        for table, filters in query_template['filters'].items():
            for filter_condition in filters:
                filter_counts[filter_condition] += 1

    filter_frequencies = {filter_condition: count / total_queries for filter_condition, count in filter_counts.items()}

    # Step 3: Identify underrepresented joins and filters
    underrepresented_templates = defaultdict(lambda: {
        'joins': set(),
        'filters': set()
    })

    # Identify underrepresented joins
    for (table1, table2), proportion in connection_proportions.items():
        if proportion < threshold:
            underrepresented_templates[table1]['joins'].add(table2)
            underrepresented_templates[table2]['joins'].add(table1)

    # Identify underrepresented filters
    for filter_condition, frequency in filter_frequencies.items():
        if frequency < threshold:
            table = filter_condition.split('.')[0]  # Extract table name from filter condition
            underrepresented_templates[table]['filters'].add(filter_condition)

    return underrepresented_templates

def get_query_template(query_info: Dict[str, Set[str]], conn, alias_mapping: Dict[str, str] = None):
    """Process query information into a structured format, including selectivity."""
    grouped_select_columns = defaultdict(set)
    for column in query_info['select_columns']:
        # Check if the column is qualified with a table name
        if '.' in column:
            table, column_name = column.split('.', 1)
            grouped_select_columns[table].add(column_name)
    
    grouped_joins = defaultdict(set)
    for join in query_info['joins']:
        table_1, table_2_condition = join.split('=', 1)
        table_1 = table_1.strip().split('.')[0]
        table_2 = table_2_condition.strip().split('.')[0]
        grouped_joins[table_1].add(table_2)

    sql_operator_pattern = re.compile(r'(>=|<=|!=|=|>|<|LIKE|IN|BETWEEN|NOT\s*(=|LIKE|IN|BETWEEN))')
    grouped_filters = defaultdict(dict)
    for filter_condition in query_info['filters']:
        match = sql_operator_pattern.search(filter_condition)
        if match:
            operator = match.group()
            is_not_operator = 'NOT' in operator
            operator = operator.replace('NOT', '').strip()  

            if is_not_operator:
                table_column, filter = filter_condition.split('NOT ' + operator, 1)
            else:
                table_column, filter = filter_condition.split(operator, 1)
            table, column = table_column.strip().split('.', 1)
            filter = filter.strip().strip("'")  
            
            for alias, table_name in (alias_mapping or {}).items():
                if table == alias:
                    table = table_name

            column_data = fetch_column_data(conn, table, column)
            if column_data is not None:
                selectivity = estimate_selectivity(column_data, operator, filter)
                if is_not_operator:
                    selectivity = 1.0 - selectivity
                
                grouped_filters[table][filter_condition] = selectivity

    # Calculate correlation between joined tables
    correlations = {}
    for join_condition in query_info['joins']:
        print(join_condition)
        
        # Extract table-column pairs from the join condition
        left_side, right_side = join_condition.split('=', 1)
        left_side, right_side = left_side.strip(), right_side.strip()
        
        # Extract table and column names correctly
        table1, left_column = left_side.split('.')
        table2, right_column = right_side.split('.')
        
        # Revert the table variables to their original names if they were aliased
        if alias_mapping and table1 in alias_mapping:
            print(f"Alias mapping found for {table1}: {alias_mapping[table1]}")
            table1 = alias_mapping[table1]

        if alias_mapping and table2 in alias_mapping:
            print(f"Alias mapping found for {table2}: {alias_mapping[table2]}")
            table2 = alias_mapping[table2]
        
        # Calculate correlation
        correlation = calculate_correlation(conn, table1, left_column, table2, right_column)
        correlations[f"{table1}.{left_column} - {table2}.{right_column}"] = correlation
    
    return {
        'select_columns': grouped_select_columns,
        'joins': grouped_joins,
        'filters': grouped_filters,
        'correlations': correlations
    }

def analyze_postgres_query(cursor, query: str) -> Dict[str, Any]:
    """
    Execute EXPLAIN ANALYZE on PostgreSQL and return detailed join progression analysis.
    
    Args:
        cursor: PostgreSQL database cursor
        query: SQL query to analyze
        
    Returns:
        Dictionary with join progression and performance metrics
    """
    # Execute EXPLAIN ANALYZE in JSON format
    cursor.execute(f"EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON) {query}")
    qep = cursor.fetchone()[0][0]
    plan = qep['Plan']
    
    results = {
        'join_progression': [],
        'node_statistics': defaultdict(dict),
        'total_execution_time': qep['Execution Time'],
        'planning_time': qep['Planning Time']
    }
    
    # Recursive function to process plan nodes
    def process_node(node: Dict, parent_relation: str = None) -> None:
        nonlocal results
        
        node_type = node.get('Node Type')
        relation_name = node.get('Relation Name')
        alias = node.get('Alias')
        
        # Track basic node statistics
        node_key = alias or relation_name or node_type
        results['node_statistics'][node_key].update({
            'node_type': node_type,
            'actual_rows': node.get('Actual Rows'),
            'plan_rows': node.get('Plan Rows'),
            'actual_loops': node.get('Actual Loops', 1),
            'execution_time': node.get('Actual Total Time'),
            'startup_cost': node.get('Startup Cost'),
            'total_cost': node.get('Total Cost'),
            'parent_relation': parent_relation
        })
        
        # Process join nodes specifically
        if 'Join' in node_type or 'Nested Loop' in node_type:
            join_condition = node.get('Hash Cond') or node.get('Join Filter') or node.get('Merge Cond') or ''
            
            join_info = {
                'node_type': node_type,
                'join_condition': join_condition,
                'output_rows': node.get('Actual Rows'),
                'execution_time_ms': node.get('Actual Total Time'),
                'planner_estimate': node.get('Plan Rows'),
                'join_algorithm': get_join_algorithm(node_type),
                'child_nodes': []
            }
            
            # Add relation information for each side of the join
            if 'Plans' in node:
                for child in node['Plans']:
                    child_rel = child.get('Alias') or child.get('Relation Name') or child.get('Node Type')
                    child_info = {
                        'relation': child_rel,
                        'rows': child.get('Actual Rows'),
                        'time_ms': child.get('Actual Total Time'),
                        'node_type': child.get('Node Type')
                    }
                    # Look for index conditions that might reveal the join predicate
                    if child.get('Index Cond'):
                        child_info['index_condition'] = child.get('Index Cond')
                    
                    join_info['child_nodes'].append(child_info)

            # Infer missing condition in nested loops
            if not join_condition and node_type == 'Nested Loop':
                for child in node['Plans']:
                    inferred = infer_condition_from_child(child)
                    if inferred:
                        join_condition = inferred
                        join_info['join_condition'] = join_condition
                        join_info['inferred_condition'] = True
                        break
            
            results['join_progression'].append(join_info)
        
        # Recurse into child nodes
        if 'Plans' in node:
            for child in node['Plans']:
                process_node(child, parent_relation=relation_name or alias)
    
    def get_join_algorithm(node_type: str) -> str:
        """Map PostgreSQL node types to join algorithms"""
        if 'Hash' in node_type:
            return 'Hash Join'
        elif 'Merge' in node_type:
            return 'Merge Join'
        elif 'Nested Loop' in node_type:
            return 'Nested Loop'
        return node_type

    def infer_condition_from_child(child: Dict) -> str:
        """Infer join condition from child node (Index Cond or Filter)"""
        for key in ('Index Cond', 'Filter'):
            cond = child.get(key)
            if cond and isinstance(cond, str) and '=' in cond:
                return cond
        return ''
    
    process_node(plan)
    
    # Calculate some derived metrics
    for join in results['join_progression']:
        if len(join['child_nodes']) == 2:
            left, right = join['child_nodes']
            join['selectivity'] = join['output_rows'] / (left['rows'] * right['rows']) if (left['rows'] * right['rows']) > 0 else 0
            join['estimation_error'] = (join['planner_estimate'] - join['output_rows']) / join['output_rows'] if join['output_rows'] > 0 else 0
    
    return results

def get_query_template_no_correl(query_info: Dict[str, Set[str]], db_cache: DatabaseCache, alias_mapping: Dict[str, str] = None, original_query: str = None):
    """Process query information using preloaded data."""
    # Initialize structures
    grouped_select_columns = defaultdict(set)
    grouped_joins = defaultdict(set)
    grouped_filters = defaultdict(dict)
    join_details = []  # To store detailed join information
    
    # Process select columns
    for column in query_info['select_columns']:
        if '.' in column:
            table, column_name = column.split('.', 1)
            if alias_mapping and table in alias_mapping:
                table = alias_mapping[table]
            grouped_select_columns[table].add(column_name)

    # Process joins
    for join in query_info['joins']:
        table_1, table_2_condition = join.split('=', 1)
        table_1 = table_1.strip().split('.')[0]
        table_2 = table_2_condition.strip().split('.')[0]
        if alias_mapping and table_1 in alias_mapping:
            alias_1 = table_1
            table_1 = alias_mapping[table_1]
        if alias_mapping and table_2 in alias_mapping:
            alias_2 = table_2
            table_2 = alias_mapping[table_2]
        grouped_joins[table_1].add(table_2)

        # Store detailed join information
        join_details.append({
            'tables': (table_1, table_2),
            'aliases': (alias_1, alias_2) if alias_mapping else (None, None),
            'condition': join.strip(),
        })        

    execution_plan = None

    if original_query:
        # Connect to the database
        import psycopg2
        from config import DB_CONFIG
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            dbname=DB_CONFIG['dbname'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        try:
            with conn.cursor() as cursor:
                explain_data = analyze_postgres_query(cursor, original_query)
                # Map execution plan joins to our join details
                for jd in join_details:
                    for epj in explain_data['join_progression']:
                        # Check if this execution plan join matches our declared join
                        if (f"{jd['aliases'][0]}." in epj['join_condition'] and 
                            f"{jd['aliases'][1]}." in epj['join_condition']):
                            jd.update({
                                'execution_data': {
                                    'node_type': epj['node_type'],
                                    'actual_rows': epj['output_rows'],
                                    'plan_rows': epj['planner_estimate'],
                                    'execution_time_ms': epj['execution_time_ms'],
                                    'join_algorithm': epj['join_algorithm'],
                                    'selectivity': epj.get('selectivity', 0),
                                    'estimation_error': epj.get('estimation_error', 0)
                                }
                            })
                            break

                execution_plan = {
                    'total_execution_time': explain_data['total_execution_time'],
                    'planning_time': explain_data['planning_time'],
                    'node_statistics': explain_data['node_statistics']
                }

                cursor.close()
                conn.close()
        except Exception as e:
            print(f"Error executing EXPLAIN ANALYZE: {str(e)}")
        finally:
            if conn:
                conn.close()
    
    # Process filters
    sql_operator_pattern = re.compile(
        r'(>=|<=|!=|=|>|<|LIKE|like|IN|BETWEEN|IS\s+NOT\s+NULL|IS\s+NULL|NOT\s*(=|LIKE|like|IN|BETWEEN))'
    )
    
    aliases = {alias: table for alias, table in query_info['aliases'].items()}
    
    for filter_condition in query_info['filters']:
        filter_condition = re.sub(r'\s+', ' ', filter_condition)
        match = sql_operator_pattern.search(filter_condition)
        
        if not match:
            continue
            
        operator = match.group().strip()
        predicate_part = filter_condition.split(operator, 1)[0].strip()
        
        if '.' not in predicate_part:
            continue
            
        alias, column = predicate_part.split('.', 1)
        table = aliases.get(alias)
        
        if not table:
            continue
            
        # Handle different operator types
        if operator.upper() in ('IS NULL', 'IS NOT NULL'):
            selectivity = db_cache.estimate_selectivity(table, column, operator, None)
            grouped_filters[table][filter_condition] = selectivity
            continue
            
        # Handle regular operators
        is_not_operator = 'NOT' in operator.upper()
        base_operator = operator.replace('NOT', '').strip()
        
        try:
            if is_not_operator:
                parts = filter_condition.split('NOT ' + base_operator, 1)
            else:
                parts = filter_condition.split(base_operator, 1)
                
            if len(parts) != 2:
                continue
                
            _, filter_value = parts
            filter_value = filter_value.strip().strip("'")
            
            selectivity = db_cache.estimate_selectivity(table, column, base_operator, filter_value)
            if is_not_operator:
                selectivity = 1.0 - selectivity
                
            grouped_filters[table][filter_condition] = selectivity
        except Exception as e:
            print(f"Error processing filter '{filter_condition}': {e}")
    
    template = {
        'select_columns': grouped_select_columns,
        'joins': grouped_joins,
        'filters': grouped_filters,
        'join_details': join_details
    }

    if execution_plan:
        template['execution_plan'] = execution_plan

    return template

def get_query_template_no_selectivity(query_info: Dict[str, Set[str]]):
    """Process query information using preloaded data."""
    # Initialize structures
    grouped_select_columns = defaultdict(set)
    grouped_joins = defaultdict(set)
    grouped_filters = defaultdict(dict)
    
    # Process select columns
    for column in query_info['select_columns']:
        if '.' in column:
            table, column_name = column.split('.', 1)
            grouped_select_columns[table].add(column_name)
    
    # Process joins
    for join in query_info['joins']:
        table_1, table_2_condition = join.split('=', 1)
        table_1 = table_1.strip().split('.')[0]
        table_2 = table_2_condition.strip().split('.')[0]
        grouped_joins[table_1].add(table_2)
    
    # Process filters
    sql_operator_pattern = re.compile(
        r'(>=|<=|!=|=|>|<|LIKE|like|IN|BETWEEN|IS\s+NOT\s+NULL|IS\s+NULL|NOT\s*(=|LIKE|like|IN|BETWEEN))'
    )
    
    aliases = {alias: table for alias, table in query_info['aliases'].items()}
    
    for filter_condition in query_info['filters']:
        filter_condition = re.sub(r'\s+', ' ', filter_condition)
        match = sql_operator_pattern.search(filter_condition)
        
        if not match:
            continue
            
        operator = match.group().strip()
        predicate_part = filter_condition.split(operator, 1)[0].strip()
        
        if '.' not in predicate_part:
            continue
            
        alias, column = predicate_part.split('.', 1)
        table = aliases.get(alias)
        
        if not table:
            continue
            
        # Handle different operator types
        if operator.upper() in ('IS NULL', 'IS NOT NULL'):
            selectivity = 0.0
            grouped_filters[table][filter_condition] = selectivity
            continue
            
        # Handle regular operators
        is_not_operator = 'NOT' in operator.upper()
        base_operator = operator.replace('NOT', '').strip()
        
        try:
            if is_not_operator:
                parts = filter_condition.split('NOT ' + base_operator, 1)
            else:
                parts = filter_condition.split(base_operator, 1)
                
            if len(parts) != 2:
                continue
                
            _, filter_value = parts
            filter_value = filter_value.strip().strip("'")
            
            selectivity = 0.0
            if is_not_operator:
                selectivity = 1.0 - selectivity
                
            grouped_filters[table][filter_condition] = selectivity
        except Exception as e:
            print(f"Error processing filter '{filter_condition}': {e}")
    
    return {
        'select_columns': grouped_select_columns,
        'joins': set(query_info['joins']),
        'filters': grouped_filters
    }

from collections import defaultdict
from typing import List, Dict, Set

def accumulate_query_templates(query_templates: List[Dict], schema: Dict[str, List[str]]):
    """Accumulate all query templates into one big table grouped by table name, including correlations."""
    accumulated = defaultdict(lambda: {
        'select_columns': set(),  # Columns selected from this table
        'joins': set(),          # Tables joined with this table
        'filters': defaultdict(dict),  # Filters applied to this table
        'correlations': defaultdict(dict)  # Correlations with other tables
    })
    
    # Step 1: Accumulate data from all query templates
    for template in query_templates:
        # Accumulate select columns
        for table, columns in template['select_columns'].items():
            accumulated[table]['select_columns'].update(columns)
        
        # Accumulate joins
        for table, joins in template['joins'].items():
            accumulated[table]['joins'].update(joins)
        
        # Accumulate filters
        for table, filters in template['filters'].items():
            for filter_condition, selectivity in filters.items():
                accumulated[table]['filters'][filter_condition] = selectivity
        
        # Accumulate correlations
        for correlation_key, correlation_value in template.get('correlations', {}).items():
            table1, table2 = correlation_key.split(' - ')
            accumulated[table1]['correlations'][table2] = correlation_value
            accumulated[table2]['correlations'][table1] = correlation_value
    
    for table, columns in schema.items():
        if table not in accumulated:
            accumulated[table] = {
                'select_columns': set(columns),
                'joins': set(),
                'filters': defaultdict(dict),
                'correlations': defaultdict(dict)
            }
        else:
            # Add columns that are not selected, joined, or filtered
            selected_columns = accumulated[table]['select_columns']
            all_columns = set(columns)
            unselected_columns = all_columns - selected_columns
            accumulated[table]['select_columns'].update(unselected_columns)
    
    return accumulated

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt

def find_optimal_clusters(templates, conn, max_clusters=10):
    vectorizer = TfidfVectorizer()
    X = vectorizer.fit_transform(map(str, templates))
    
    distortions = []
    for i in range(1, max_clusters + 1):
        kmeans = KMeans(n_clusters=i, random_state=42)
        kmeans.fit(X)
        distortions.append(kmeans.inertia_)
    
    plt.figure(figsize=(8, 5))
    plt.plot(range(1, max_clusters + 1), distortions, marker='o')
    plt.xlabel('Number of clusters')
    plt.ylabel('Distortion')
    plt.title('Elbow Method for Optimal Clusters')
    plt.show()

def cluster_queries(queries, templates, conn, num_clusters=3):
    vectorizer = TfidfVectorizer()
    X = vectorizer.fit_transform(map(str, templates))
    
    kmeans = KMeans(n_clusters=num_clusters, random_state=42)
    labels = kmeans.fit_predict(X)
    
    # Reduce dimensions for visualization
    pca = PCA(n_components=2)
    X_reduced = pca.fit_transform(X.toarray())
    
    plt.figure(figsize=(8, 5))
    plt.scatter(X_reduced[:, 0], X_reduced[:, 1], c=labels, cmap='viridis', marker='o')
    plt.xlabel('PCA Component 1')
    plt.ylabel('PCA Component 2')
    plt.title('Query Clustering Visualization')
    plt.colorbar()
    plt.show()
    
    clustered_queries = defaultdict(list)
    for idx, label in enumerate(labels):
        clustered_queries[label].append(queries[idx])
    
    return clustered_queries

import pglast
from pglast.visitors import Visitor
from pglast import ast
from pprint import pprint

def print_ast(node):
    stmt = node[0].stmt
    pprint(stmt(depth=30, skip_none=True))

class SQLInfoExtractor(Visitor):
    def __init__(self):
        super().__init__()
        self.tables = set()
        self.joins = set()
        self.predicates = set()
        self.aliasname_fullname = {}

    @property
    def info(self):
        return {
            'tables': self.tables,
            'joins': self.joins,
            'predicates': self.predicates,
            'aliasname_fullname': self.aliasname_fullname,
        }

    def visit(self, ancestors, node):

        # Extract table names and aliases
        if isinstance(node, ast.RangeVar):
            table_name = node.relname
            self.tables.add(table_name)
            if hasattr(node, 'alias') and node.alias is not None:
                alias_name = node.alias.aliasname
                self.aliasname_fullname[alias_name] = table_name

        # Extract explicit join information
        if isinstance(node, ast.JoinExpr):
            if isinstance(node.larg, ast.RangeVar) and isinstance(node.rarg, ast.RangeVar):
                left_table = node.larg.relname
                right_table = node.rarg.relname
                if node.larg.alias:
                    left_table = node.larg.alias.aliasname
                if node.rarg.alias:
                    right_table = node.rarg.alias.aliasname
                if node.quals and isinstance(node.quals, ast.A_Expr):
                    lexpr = node.quals.lexpr.fields if hasattr(node.quals.lexpr, 'fields') else []
                    rexpr = node.quals.rexpr.fields if hasattr(node.quals.rexpr, 'fields') else []
                    if len(lexpr) == 2 and len(rexpr) == 2:
                        self.joins.add([lexpr[0].sval, lexpr[1].sval, rexpr[0].sval, rexpr[1].sval])

        # Extract implicit join conditions from WHERE clause
        if isinstance(node, ast.A_Expr):
            if node.name[0].sval == '=':
                lexpr = node.lexpr.fields if hasattr(node.lexpr, 'fields') else []
                rexpr = node.rexpr.fields if hasattr(node.rexpr, 'fields') else []
                # for join, both have values
                if len(lexpr) == 2 and len(rexpr) == 2:
                    self.joins.add((lexpr[0].sval, lexpr[1].sval, rexpr[0].sval, rexpr[1].sval))
                elif len(lexpr) == 1 and len(rexpr) == 1:
                    self.joins.add(("", lexpr[0].sval, "", rexpr[0].sval))

            # Extract other predicates
            if node.name[0].sval in ('>', '=', '<', '<=', '>='):
                lexpr = node.lexpr.fields if hasattr(node.lexpr, 'fields') else []
                if hasattr(node.rexpr, 'val'):
                    if hasattr(node.rexpr.val, 'ival'):
                        rexpr = node.rexpr.val.ival
                    elif hasattr(node.rexpr.val, 'sval'):
                        rexpr = node.rexpr.val.sval
                    else:
                        rexpr = None
                    if len(lexpr) == 2:
                        self.predicates.add((lexpr[0].sval, lexpr[1].sval, node.name[0].sval, rexpr))
                    elif len(lexpr) == 1:
                        self.predicates.add(("", lexpr[0].sval, node.name[0].sval, rexpr))
                elif hasattr(node.rexpr, 'arg'):
                    if hasattr(node.rexpr.arg, 'val'):
                        if hasattr(node.rexpr.arg.val, 'sval'):
                            rexpr = node.rexpr.arg.val.sval
                            if len(lexpr) == 2:
                                self.predicates.add((lexpr[0].sval, lexpr[1].sval, node.name[0].sval, rexpr))
                            elif len(lexpr) == 1:
                                self.predicates.add(("", lexpr[0].sval, node.name[0].sval, rexpr))

        # Handle subqueries and additional join conditions
        if isinstance(node, ast.SubLink):
            # Extract join conditions in subquery
            subselect = node.subselect
            if subselect:
                self(subselect)

        if isinstance(node, ast.BoolExpr):
            # Traverse all arguments in AND/OR expressions
            for arg in node.args:
                self(arg)