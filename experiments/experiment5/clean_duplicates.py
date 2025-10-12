import os

dst = "/data/hdd1/users/kmparmp/Learned-Optimizers-Benchmarking-Suite/experiments/experiment5/5.3/stack_sampled/test"

for root, dirs, files in os.walk(dst):
    for f in files:
        if f.endswith(".sql"):
            # Only keep if filename matches the directory prefix (with "__")
            parent = os.path.basename(root)  # e.g. "q6__q6-002"
            if not f.startswith(parent):
                file_path = os.path.join(root, f)
                print(f"Deleting extra SQL file: {file_path}")
                os.remove(file_path)
