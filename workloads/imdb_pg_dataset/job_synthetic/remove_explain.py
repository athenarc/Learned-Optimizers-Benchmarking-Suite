import os
import argparse

def process_file(file_path):
    with open(file_path, 'r') as f:
        lines = f.readlines()
    if len(lines) < 2:
        # Not enough lines to remove
        return
    # Backup original file
    # Write file skipping the first two lines
    with open(file_path, 'w') as f:
        f.writelines(lines[2:])

def main():

    for root, dirs, files in os.walk("/data/hdd1/users/kmparmp/workloads/imdb_pg_dataset/queries_imdb_synthetic_100k/"):
        for file in files:
            if file.endswith(".sql"):
                file_path = os.path.join(root, file)
                process_file(file_path)

if __name__ == "__main__":
    main()