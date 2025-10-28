import pandas as pd
import re
from typing import Optional
from scipy.stats import pearsonr
from typing import Dict, Set, Any, List
from file_utils import read_queries_from_directory
from config import TABLES
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
from sqlalchemy.sql import text
import numpy as np
from tqdm import tqdm

class DatabaseCache:
    def __init__(self, conn):
        self.conn = conn
        self.tables: Dict[str, pd.DataFrame] = {}
        self.column_stats: Dict[str, Dict[str, Any]] = {}
        self.column_value_indices: Dict[str, Dict[str, pd.Index]] = {}
        self.column_min_max: Dict[str, Dict[str, tuple]] = {}
        
    def preload_all_tables(self, sample_fraction: float = 1.0):
        """Load all tables with optional sampling for large tables."""
        for table_name, columns in TABLES.items():
            try:
                # Skip large binary columns that won't be used in predicates
                columns_to_load = [col for col in columns if col not in ['md5sum', 'note']]
                
                query = f"SELECT {','.join(columns_to_load)} FROM {table_name}"
                
                df = pd.read_sql(query, self.conn)
                self.tables[table_name] = df
                self._precompute_column_metadata(table_name)
                print(f"Loaded {len(df):,} rows from {table_name}")
            except Exception as e:
                print(f"Error loading {table_name}: {str(e)}")
    
    def _precompute_column_metadata(self, table_name: str):
        """Precompute statistics and value indices for each column."""
        if table_name not in self.tables:
            return
            
        df = self.tables[table_name]
        self.column_stats[table_name] = {}
        self.column_value_indices[table_name] = {}
        self.column_min_max[table_name] = {}
        
        for column in df.columns:
            col_data = df[column]

            non_null_data = col_data.dropna()
            sample_values = non_null_data.sample(min(5, len(non_null_data))).tolist() if len(non_null_data) > 0 else []
            
            # Basic statistics
            stats = {
                'null_count': col_data.isna().sum(),
                'distinct_count': col_data.nunique(),
                'dtype': str(col_data.dtype),
                'sample_values': sample_values
            }
            
            # For numeric columns
            if pd.api.types.is_numeric_dtype(col_data):
                stats.update({
                    'min': col_data.min(),
                    'max': col_data.max(),
                    'mean': col_data.mean()
                })
                self.column_min_max[table_name][column] = (col_data.min(), col_data.max())
            
            self.column_stats[table_name][column] = stats
   
    def estimate_selectivity(self, table_name: str, column_name: str, operator: str, filter_value: str) -> float:
        """Estimate selectivity for a given filter condition."""
        column_data = self.get_column_values(table_name, column_name)
        if column_data is None:
            return 0.0
        
        # Handle IS NULL and IS NOT NULL separately
        if operator in ['IS NULL', 'IS NOT NULL']:
            is_not_null = (operator == 'IS NOT NULL')
            return estimate_selectivity_for_null(column_data, is_not_null)
        
        # For other operators, estimate selectivity
        return estimate_selectivity(column_data, operator, filter_value)

    def get_column_values(self, table: str, column: str) -> Optional[pd.Series]:
        """Get all values for a column."""
        if table in self.tables and column in self.tables[table].columns:
            return self.tables[table][column]
        return None
    
    def get_column_stats(self, table: str, column: str) -> Optional[Dict[str, Any]]:
        """Get statistics for a column."""
        if table in self.column_stats and column in self.column_stats[table]:
            return self.column_stats[table][column]
        return None
    
    def get_column_value_indices(self, table: str, column: str) -> Optional[pd.Index]:
        """Get value indices for a column."""
        if table in self.column_value_indices and column in self.column_value_indices[table]:
            return self.column_value_indices[table][column]
        return None

    def get_connection(self) -> Connection:
        """Get the database connection."""
        return self.conn

    def get_table_stats(self, table_name: str) -> Dict[str, Any]:
        """
        Get comprehensive statistics for a table including row count, column statistics,
        and other metadata useful for query planning.
        
        Args:
            table_name: Name of the table to get statistics for
            
        Returns:
            Dictionary containing:
            - row_count: Total number of rows
            - columns: Dictionary of column statistics
            - sample_rows: Sample of rows from the table
            - foreign_keys: List of foreign key relationships
        """
        if table_name not in self.tables:
            # If table not loaded, try to get basic stats from database
            return self._get_table_stats_from_db(table_name)
            
        df = self.tables[table_name]
        stats = {
            'row_count': len(df),
            'columns': {},
            'sample_rows': df.head(3).to_dict('records'),
            'foreign_keys': self._get_foreign_keys(table_name)
        }
        
        # Add column statistics
        for column in df.columns:
            non_null_values = df[column].dropna()
            sample_values = non_null_values.sample(min(5, len(non_null_values))).tolist() if not non_null_values.empty else []

            col_stats = {
                'dtype': str(df[column].dtype),
                'null_count': df[column].isna().sum(),
                'distinct_count': df[column].nunique(),
                'sample_values': sample_values
            }
            
            # Numeric-specific stats
            if pd.api.types.is_numeric_dtype(df[column]):
                col_stats.update({
                    'min': df[column].min(),
                    'max': df[column].max(),
                    'mean': df[column].mean(),
                    'std': df[column].std()
                })
            
            stats['columns'][column] = col_stats
            
        return stats
    
    def _get_table_stats_from_db(self, table_name: str) -> Dict[str, Any]:
        """
        Fallback method to get basic table statistics directly from database
        when the full table isn't loaded in cache.
        """
        stats = {
            'row_count': 0,
            'columns': {},
            'sample_rows': [],
            'foreign_keys': []
        }

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
            
            # Get row count
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                stats['row_count'] = cursor.fetchone()[0]
                
                # Get column information
                cursor.execute(f"""
                    SELECT column_name, data_type, is_nullable 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}'
                """)
                for col_name, data_type, is_nullable in cursor.fetchall():
                    stats['columns'][col_name] = {
                        'dtype': data_type,
                        'is_nullable': is_nullable == 'YES'
                    }
                
                # Get sample rows
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
                cols = [desc[0] for desc in cursor.description]
                stats['sample_rows'] = [dict(zip(cols, row)) for row in cursor.fetchall()]
                
                # Get foreign keys
                stats['foreign_keys'] = self._get_foreign_keys(table_name)
                
        except Exception as e:
            print(f"Error getting stats for {table_name}: {str(e)}")
            
        return stats
    
    def _get_foreign_keys(self, table_name: str) -> List[Dict[str, str]]:
        """
        Get foreign key relationships for a table by querying PostgreSQL metadata.
        Returns list of dictionaries with:
        - constraint_name: Name of the foreign key constraint
        - column: Local column name
        - foreign_table: Referenced table name
        - foreign_column: Referenced column name
        """
        fks = []
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
                cursor.execute(f"""
                    SELECT 
                        tc.constraint_name,
                        kcu.column_name, 
                        ccu.table_name AS foreign_table_name,
                        ccu.column_name AS foreign_column_name 
                    FROM 
                        information_schema.table_constraints AS tc 
                        JOIN information_schema.key_column_usage AS kcu
                          ON tc.constraint_name = kcu.constraint_name
                        JOIN information_schema.constraint_column_usage AS ccu
                          ON ccu.constraint_name = tc.constraint_name
                    WHERE 
                        tc.constraint_type = 'FOREIGN KEY' 
                        AND tc.table_name = '{table_name}'
                """)
                for constraint, column, foreign_table, foreign_column in cursor.fetchall():
                    fks.append({
                        'constraint_name': constraint,
                        'column': column,
                        'foreign_table': foreign_table,
                        'foreign_column': foreign_column
                    })
        except Exception as e:
            print(f"Error getting foreign keys for {table_name}: {str(e)}")

        return fks

def fetch_column_data(conn, table: str, column: str) -> Optional[pd.Series]:
    """Fetch all values of a column from the database."""
    try:
        query = f"SELECT {column} FROM {table};"
        df = pd.read_sql(query, conn)
        return df[column]
    except Exception as e:
        print(f"Error fetching data for {table}.{column}: {e}")
        return None

def estimate_selectivity(column_data, operator: str, filter_value: str) -> float:
    """Estimate the selectivity of a filter condition on a column."""
    try:
        total_rows = len(column_data)
        if total_rows == 0:
            return 0.0
        
        # Handle string/object columns (for lexicographical comparisons)
        if pd.api.types.is_string_dtype(column_data) or column_data.dtype == 'object':  
            # Preprocess the filter value: remove whitespace and surrounding quotes
            filter_value = filter_value.strip().strip("'")
            
            # Use a normalized operator for easier matching
            op = operator.upper().strip()

            if op == '=':
                satisfying_rows = (column_data == filter_value).sum()
            elif op in ('!=', '<>'):
                satisfying_rows = (column_data != filter_value).sum()
            ## ADDED: Correct handling for lexicographical (alphabetical) range comparisons
            elif op == '>':
                satisfying_rows = (column_data > filter_value).sum()
            elif op == '<':
                satisfying_rows = (column_data < filter_value).sum()
            elif op == '>=':
                satisfying_rows = (column_data >= filter_value).sum()
            elif op == '<=':
                satisfying_rows = (column_data <= filter_value).sum()
            ## CHANGED: Enhanced LIKE to handle both '%' and '_' wildcards
            elif op == 'LIKE':
                # Convert SQL LIKE pattern to a regex pattern
                # 1. Escape special regex characters in the filter
                # 2. Replace SQL wildcards with regex equivalents
                pattern = re.escape(filter_value).replace('%', '.*').replace('_', '.')
                # Use str.contains with regex=True. na=False treats NULLs as non-matches.
                satisfying_rows = column_data.str.contains(pattern, regex=True, na=False).sum()
            ## ADDED: Support for string BETWEEN
            elif op == 'BETWEEN':
                try:
                    # Assumes format is "'value1' AND 'value2'"
                    lower, upper = [v.strip().strip("'") for v in filter_value.split('AND')]
                    satisfying_rows = column_data.between(lower, upper).sum()
                except ValueError:
                    return 0.0 # Invalid BETWEEN format
            elif op == 'IN':
                # Parse values from a string like "('A', 'B', 'C')"
                values = [v.strip().strip("'") for v in filter_value.strip('()').split(',')]
                satisfying_rows = column_data.isin(values).sum()
            else:
                # Unsupported operator for strings
                return 0.0
        # Handle numeric columns
        else:
            try:
                op = operator.upper().strip()
                if op == 'BETWEEN':
                    lower, upper = [float(v.strip()) for v in filter_value.split('AND')]
                    satisfying_rows = ((column_data >= lower) & (column_data <= upper)).sum()
                elif op == 'IN':
                    values = [float(v.strip()) for v in filter_value.strip('()').split(',')]
                    satisfying_rows = column_data.isin(values).sum()
                else:
                    # For all other operators, we need a single numeric value
                    filter_numeric = float(filter_value.strip())
                    if op == '=':
                        satisfying_rows = (column_data == filter_numeric).sum()
                    elif op == '>':
                        satisfying_rows = (column_data > filter_numeric).sum()
                    elif op == '<':
                        satisfying_rows = (column_data < filter_numeric).sum()
                    elif op == '>=':
                        satisfying_rows = (column_data >= filter_numeric).sum()
                    elif op == '<=':
                        satisfying_rows = (column_data <= filter_numeric).sum()
                    elif op in ('!=', '<>'):
                        satisfying_rows = (column_data != filter_numeric).sum()
                    else:
                        return 0.0
            except (ValueError, TypeError):
                # Failed to convert filter value to a number for a numeric column
                return 0.0
        
        return satisfying_rows / total_rows

    except Exception as e:
        print(f"Error estimating selectivity: {e}")
        return 0.0

def estimate_selectivity_for_null(column_data, is_not_null: bool) -> float:
    """Estimate the selectivity of IS NULL or IS NOT NULL conditions.
    
    Args:
        column_data: Pandas Series containing the column data
        is_not_null: Boolean indicating if this is IS NOT NULL (True) or IS NULL (False)
    
    Returns:
        Estimated selectivity (ratio of rows matching the condition)
    """
    try:
        total_rows = len(column_data)
        if total_rows == 0:
            return 0.0
        
        # Count NULL values (both pandas NA and numpy NaN)
        null_count = column_data.isna().sum()
        null_ratio = null_count / total_rows
        
        # For IS NOT NULL, return the ratio of non-null values
        if is_not_null:
            return 1.0 - null_ratio
        # For IS NULL, return the ratio of null values
        return null_ratio
        
    except Exception as e:
        print(f"Error estimating NULL selectivity: {e}")
        return 0.0

def detect_column_type(conn, table, column):
    """
    Detect if a column is numerical or categorical.
    """
    # Fetch a sample of the data to determine the type
    query = f"SELECT {column} FROM {table} LIMIT 100;"
    sample_data = pd.read_sql(query, conn)
    
    # Check if the column is numerical
    if pd.api.types.is_numeric_dtype(sample_data[column]):
        return 'numerical'
    else:
        return 'categorical'

def calculate_correlation(conn, table1, column1, table2, column2):
    """
    Calculate correlation or similarity based on column types.
    """
    # Detect column types
    type1 = detect_column_type(conn, table1, column1)
    type2 = detect_column_type(conn, table2, column2)
    
    # Fetch data from the database
    query = f"SELECT {table1}.{column1}, {table2}.{column2} FROM {table1} JOIN {table2} ON {table1}.{column1} = {table2}.{column2};"
    df = pd.read_sql(query, conn)
    
    # Drop rows with missing values
    df.dropna(inplace=True)
    
    # Handle different column type combinations
    if type1 == 'numerical' and type2 == 'numerical':
        col1_data = df[column1].values.flatten()
        col2_data = df[column2].values.flatten()
        
        # Pearson correlation for numerical columns
        correlation, _ = pearsonr(col1_data, col2_data)
        return correlation
    elif type1 == 'categorical' and type2 == 'categorical':
        # Jaccard similarity for categorical columns
        set1 = set(df[column1])
        set2 = set(df[column2])
        intersection = len(set1.intersection(set2))
        union = len(set1.union(set2))
        jaccard_similarity = intersection / union if union != 0 else 0
        return jaccard_similarity
    else:
        # Mixed types: skip or handle differently
        print(f"Skipping mixed types: {table1}.{column1} ({type1}) and {table2}.{column2} ({type2})")
        return None

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
    
def get_query_plan(conn, query):
    """Get the execution plan for a given SQL query."""
    try:
        # Use EXPLAIN to get the execution plan
        explain_query = f"EXPLAIN (FORMAT JSON) {query}"
        result = pd.read_sql(explain_query, conn)
        return result
    except Exception as e:
        print(f"Error fetching query plan: {e}")
        return None

def generate_selectivity_queries(self, table_name: str, column_name: str, base_query: str) -> Dict[str, str]:
    """
    Generate queries with filters at different selectivity points (10%, 25%, 75%, 100%)
    
    Args:
        table_name: Name of the table to analyze
        column_name: Name of the column to use for filtering
        base_query: The base query to modify with filters (must contain the table)
    
    Returns:
        Dictionary with selectivity levels as keys and generated queries as values
    """
    # First, execute the distribution analysis query
    dist_query = f"""
    WITH dist_values AS (
      SELECT 
        DISTINCT {column_name},
        cume_dist() OVER (ORDER BY {column_name}) AS cumulative_dist
      FROM {table_name}
    ),
    max_value AS (
      SELECT MAX({column_name}) AS {column_name}, 1.0 AS cumulative_dist
      FROM {table_name}
    )
    SELECT 
      {column_name},
      cumulative_dist AS selectivity
    FROM (
      SELECT * FROM dist_values
      UNION ALL
      SELECT * FROM max_value
    ) combined
    WHERE 
      (cumulative_dist BETWEEN 0.099 AND 0.101) OR  -- ~10%
      (cumulative_dist BETWEEN 0.249 AND 0.251) OR  -- ~25%
      (cumulative_dist BETWEEN 0.745 AND 0.755) OR  -- ~75%
      (cumulative_dist = 1.0)                      -- 100%
    ORDER BY cumulative_dist;
    """
    
    try:
        # Execute the distribution query
        dist_df = pd.read_sql(dist_query, self.conn)
        
        if dist_df.empty:
            return {}
        
        # Get sample values for each selectivity range
        samples = {
            '10%': None,
            '25%': None,
            '75%': None,
            '100%': None
        }
        
        # Find values closest to our target selectivities
        for _, row in dist_df.iterrows():
            sel = row['selectivity']
            val = row[column_name]
            
            if 0.099 <= sel <= 0.101 and samples['10%'] is None:
                samples['10%'] = val
            elif 0.249 <= sel <= 0.251 and samples['25%'] is None:
                samples['25%'] = val
            elif 0.745 <= sel <= 0.755 and samples['75%'] is None:
                samples['75%'] = val
            elif sel == 1.0 and samples['100%'] is None:
                samples['100%'] = val
        
        # Generate filtered queries
        generated_queries = {}
        column_type = self.column_stats.get(table_name, {}).get(column_name, {}).get('dtype', 'unknown')
        
        for sel_level, filter_value in samples.items():
            if filter_value is None:
                continue
                
            # Handle different data types appropriately
            if column_type in ['object', 'str', 'string']:
                # String/object type needs quotes
                filter_clause = f"{table_name}.{column_name} = '{filter_value}'"
            else:
                # Numeric types don't need quotes
                filter_clause = f"{table_name}.{column_name} = {filter_value}"
            
            # Insert WHERE clause into the base query
            if "WHERE" in base_query.upper():
                # Add to existing WHERE clause
                modified_query = base_query.replace(
                    "WHERE", 
                    f"WHERE {filter_clause} AND ", 
                    1
                )
            else:
                # Add new WHERE clause
                modified_query = base_query + f" WHERE {filter_clause}"
            
            generated_queries[sel_level] = modified_query
        
        return generated_queries
    
    except Exception as e:
        print(f"Error generating selectivity queries: {e}")
        return {}