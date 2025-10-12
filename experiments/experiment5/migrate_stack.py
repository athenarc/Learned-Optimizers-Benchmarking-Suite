import os
import shutil

src = "/data/hdd1/users/kmparmp/Learned-Optimizers-Benchmarking-Suite/experiments/experiment5/5.3/stack/test"
dst = "/data/hdd1/users/kmparmp/Learned-Optimizers-Benchmarking-Suite/experiments/experiment5/5.3/stack_sampled/test"

for target in os.listdir(dst):
    target_path = os.path.join(dst, target)

    if not os.path.isdir(target_path):
        continue  # skip non-dirs

    # Convert "q1__q1-001" → "q1/q1-001"
    src_sub = target.replace("__", "/")
    src_path = os.path.join(src, src_sub)

    if os.path.isdir(src_path):
        print(f"Copying from {src_path} → {target_path}")
        for item in os.listdir(src_path):
            s_item = os.path.join(src_path, item)
            d_item = os.path.join(target_path, item)

            if os.path.isdir(s_item):
                shutil.copytree(s_item, d_item, dirs_exist_ok=True)
            else:
                shutil.copy2(s_item, d_item)
    else:
        print(f"Skipping {target_path} (no matching source found)")
