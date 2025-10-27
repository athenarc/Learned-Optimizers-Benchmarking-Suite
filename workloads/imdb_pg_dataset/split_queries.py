import re

def split_sql_file(input_file, output_dir, output_prefix=''):
    # Read the input file
    with open(input_file, 'r') as f:
        content = f.read()
    
    # Split the content into individual SQL statements
    # This regex looks for statements ending with a semicolon followed by optional whitespace
    sql_statements = re.split(r';\s*', content.strip())
    
    # Remove any empty statements that might result from the split
    sql_statements = [stmt.strip() for stmt in sql_statements if stmt.strip()]
    
    # Save each statement to a separate file
    for i, statement in enumerate(sql_statements, start=1):
        output_file = f"{output_dir}/{output_prefix}{i}.sql"
        with open(output_file, 'w') as f:
            # Add the semicolon back to each statement
            f.write(statement + ';')
        
        print(f"Saved {output_file}")
    
    print(f"\nTotal queries processed: {len(sql_statements)}")

# Example usage:
input_filename = '/data/hdd1/users/kmparmp/Learned-Optimizers-Benchmarking-Suite/workloads/imdb_pg_dataset/job-light.sql'
output_directory = '/data/hdd1/users/kmparmp/Learned-Optimizers-Benchmarking-Suite/workloads/imdb_pg_dataset/job_light/'
split_sql_file(input_filename, output_directory)