import json

# Load the JSON data from the file
with open('/data/hdd1/users/kmparmp/FASTgres-PVLDBv16/output_job1.json', 'r') as file:
    data = json.load(file)

total_min_latency = 0

# Iterate over each query in the JSON data
for query, latencies in data.items():
    # Exclude the 'opt' key and find the minimum latency
    min_latency = min(value for key, value in latencies.items() if key != 'opt')
    total_min_latency += min_latency

print(f'Total minimal latency for all queries: {total_min_latency}')
