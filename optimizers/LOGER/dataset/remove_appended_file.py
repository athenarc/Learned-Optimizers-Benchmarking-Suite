import os

def remove_appended_line(directory, line_to_remove, file_extension=None):
    """
    Traverse the directory tree and remove a specific line from every file.
    
    :param directory: The root directory to start traversing from.
    :param line_to_remove: The line to remove from each file.
    :param file_extension: Optional. Only process files with this extension (e.g., '.sql').
    """
    for root, _, files in os.walk(directory):
        for file in files:
            if file_extension and not file.endswith(file_extension):
                continue  # Skip files that don't match the extension
            file_path = os.path.join(root, file)
            try:
                with open(file_path, 'r') as f:
                    lines = f.readlines()  # Read all lines from the file
                
                # Remove the specific line if it exists
                lines = [line for line in lines if line.strip() != line_to_remove.strip()]
                
                # Write the cleaned content back to the file
                with open(file_path, 'w') as f:
                    f.writelines(lines)
                
                print(f"Removed line from: {file_path}")
            except Exception as e:
                print(f"Failed to process {file_path}: {e}")

if __name__ == "__main__":
    # Directory to start from
    base_dir = "/data/hdd1/users/kmparmp/workloads/"
    
    # Line to remove
    line_to_remove = 'SELECT "clear_cache"();'
    
    # Optional: Only process .sql files
    file_extension = '.sql'
    
    # Call the function
    remove_appended_line(base_dir, line_to_remove, file_extension)
    print(f"Line removed from all {file_extension} files in the directory tree.")