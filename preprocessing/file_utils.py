import os
from typing import List, Tuple

def read_queries_from_directory(directory: str) -> List[Tuple[str, str, str]]:
    """
    Recursively read SQL queries from .sql files in the given directory.

    Returns:
        A list of tuples (query, file_location, query_name) where:
            - query = the SQL text
            - file_location = relative path from the given directory
            - query_name = filename without extension
    """
    results = []

    for root, _, files in os.walk(directory):
        for filename in files:
            if filename.endswith(".sql"):
                filepath = os.path.join(root, filename)
                try:
                    with open(filepath, "r", encoding="utf-8") as file:
                        query = file.read().strip()
                        if query:
                            rel_path = os.path.relpath(filepath, directory)
                            query_name = os.path.splitext(filename)[0]
                            results.append((query, rel_path, query_name))
                except IOError as e:
                    print(f"Error reading file {filepath}: {e}")

    return results


def write_query_results_to_file(results, output_file="query_results.txt"):
    """Write the processed query information to a file."""
    with open(output_file, "w") as file:
        for filename, query_info in results:
            file.write(f"\nQuery: {filename}\n")
            file.write("Select Columns:\n")
            for table, columns in query_info['select_columns'].items():
                file.write(f"  {table}: {', '.join(columns)}\n")
            
            file.write("\nJoins:\n")
            for join_condition in query_info['joins']:
                file.write(f"  {join_condition}\n")
            
            file.write("\nFilters and Selectivity:\n")
            for table, filters in query_info['filters'].items():
                file.write(f"  {table}:\n")
                for condition, selectivity in filters.items():
                    file.write(f"    {condition}: {selectivity:.2f}\n")

            # Write correlations if they exist
            if 'correlations' in query_info:
                file.write("\nCorrelations:\n")
                for columns, correlation in query_info['correlations'].items():
                    file.write(f"  {columns}: {correlation:.2f}\n")
                    
    print(f"Query results written to {output_file}")