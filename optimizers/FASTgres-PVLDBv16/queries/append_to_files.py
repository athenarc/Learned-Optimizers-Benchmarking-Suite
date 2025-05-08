import os

def append_to_files(directory, line_to_append, file_extension=None):
    """
    Traverse the directory tree and append a line to every file.
    
    :param directory: The root directory to start traversing from.
    :param line_to_append: The line to append to each file.
    :param file_extension: Optional. Only append to files with this extension (e.g., '.sql').
    """
    for root, _, files in os.walk(directory):
        for file in files:
            if file_extension and not file.endswith(file_extension):
                continue  # Skip files that don't match the extension
            file_path = os.path.join(root, file)
            try:
                with open(file_path, 'a') as f:
                    f.write('\n' + line_to_append + '\n')
                print(f"Appended to: {file_path}")
            except Exception as e:
                print(f"Failed to append to {file_path}: {e}")

if __name__ == "__main__":
    # Directory to start from
    base_dir = "/data/hdd1/users/kmparmp/LOGER/dataset"
    
    # Line to append
    line_to_append = 'SELECT "clear_cache"();'
    
    # Optional: Only append to .sql files
    file_extension = '.sql'
    
    # Call the function
    append_to_files(base_dir, line_to_append, file_extension)
    print(f"Line appended to all {file_extension} files in the directory tree.")