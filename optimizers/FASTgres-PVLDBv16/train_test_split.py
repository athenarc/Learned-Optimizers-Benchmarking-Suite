import json
import os
import random

# Define the path to your queries directory
queries_directory = "/data/hdd1/users/kmparmp/workloads/tpcds"

# Check if the directory exists
if not os.path.exists(queries_directory):
    raise FileNotFoundError(f"Queries directory does not exist: {queries_directory}")

# List all query files in the directory
query_files = [f for f in os.listdir(queries_directory) if os.path.isfile(os.path.join(queries_directory, f))]
if not query_files:
    raise ValueError("No query files found in the specified directory.")

# Shuffle and split the queries randomly into 80-20
random.seed(42)  # Ensures reproducibility
random.shuffle(query_files)

split_index = int(0.8 * len(query_files))
train_queries = query_files[:split_index]
test_queries = query_files[split_index:]

# Create the train-test split dictionary
train_test_split = {
    "train": train_queries,
    "test": test_queries
}

# Save the split to a JSON file
output_file = "/data/hdd1/users/kmparmp/FASTgres-PVLDBv16/train_test_split_tpcds.json"
with open(output_file, "w") as f:
    json.dump(train_test_split, f, indent=4)

print(f"Train-test split created and saved to: {output_file}")
