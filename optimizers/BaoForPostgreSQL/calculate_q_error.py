import re

# Function to read predicted latencies from the predictions file
def read_predicted_latencies(predicted_file_path):
    predicted_latencies = {}
    
    with open(predicted_file_path, "r") as file:
        for line in file:
            match = re.match(r"(\d+\.\d+) - Query #(\d+) - Best Predicted Latency: (\d+\.\d+)", line.strip())
            if match:
                timestamp = float(match.group(1))  # Timestamp
                query_num = int(match.group(2))  # Query Number
                predicted_latency = float(match.group(3))  # Predicted Latency (in milliseconds)
                predicted_latencies[query_num] = predicted_latency
                
    return predicted_latencies

# Function to read actual latencies from the real latencies file
def read_actual_latencies(actual_file_path):
    actual_latencies = {}
    
    with open(actual_file_path, "r") as file:
        for line in file:
            match = re.match(r"Query #(\d+) - .+\.sql: (\d+\.\d+) seconds", line.strip())
            if match:
                query_num = int(match.group(1))  # Query Number
                actual_latency = float(match.group(2)) * 1000  # Convert seconds to milliseconds
                actual_latencies[query_num] = actual_latency
                
    return actual_latencies

# Function to calculate Q-error (max of predicted/actual and actual/predicted)
def calculate_q_error(predicted_latencies, actual_latencies):
    q_errors = {}
    total_q_error = 0
    num_queries = 0
    
    # Iterate over each query to calculate the Q-error
    for query_num in predicted_latencies:
        if query_num in actual_latencies:
            predicted_latency = predicted_latencies[query_num]
            actual_latency = actual_latencies[query_num]
            
            # Calculate Q-error: max(predicted/actual, actual/predicted)
            q_error = max(predicted_latency / actual_latency, actual_latency / predicted_latency)
            
            q_errors[query_num] = q_error
            total_q_error += q_error
            num_queries += 1
    
    # Calculate the average Q-error
    avg_q_error = total_q_error / num_queries if num_queries > 0 else 0
    
    return q_errors, avg_q_error

# Main function
def main(predicted_file_path, actual_file_path, output_file_path):
    # Read the predicted and actual latencies
    predicted_latencies = read_predicted_latencies(predicted_file_path)
    actual_latencies = read_actual_latencies(actual_file_path)
    
    # Calculate Q-error
    q_errors, avg_q_error = calculate_q_error(predicted_latencies, actual_latencies)
    
    # Write the Q-errors to a file
    with open(output_file_path, "w") as output_file:
        output_file.write("Query # - Q-error (relative):\n")
        for query_num, q_error in q_errors.items():
            output_file.write(f"Query #{query_num} - Q-error: {q_error:.4f}\n")
        
        output_file.write(f"\nAverage Q-error: {avg_q_error:.4f}\n")
    
    print(f"Q-errors saved to {output_file_path}")

if __name__ == "__main__":
    # File paths for predicted and actual latencies
    predicted_file_path = "/data/hdd1/users/kmparmp/BaoForPostgreSQL/bao_server/best_latency_log.txt"  # Change to your predicted latencies file
    actual_file_path = "/data/hdd1/users/kmparmp/BaoForPostgreSQL/query_real_latencies.txt"  # Change to your actual latencies file
    output_file_path = "/data/hdd1/users/kmparmp/BaoForPostgreSQL/job_q_errors_output.txt"  # Change to your desired output file path
    
    # Call the main function
    main(predicted_file_path, actual_file_path, output_file_path)
