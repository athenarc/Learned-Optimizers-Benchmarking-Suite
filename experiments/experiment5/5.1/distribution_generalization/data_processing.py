import os
import shutil

def process_run1_directories(root_dir):
    # The specific directory we want to keep unchanged
    exclude_dir = "/data/hdd1/users/kmparmp/Learned-Optimizers-Benchmarking-Suite/experiments/experiment5/5.1/distribution_generalization/job_light/run1"
    
    for dirpath, dirnames, filenames in os.walk(root_dir, topdown=False):
        # Skip processing if this is the directory we want to keep
        if os.path.normpath(dirpath) == os.path.normpath(exclude_dir):
            print(f"Skipping protected directory: {dirpath}")
            continue
            
        if os.path.basename(dirpath) == 'run1':
            parent_dir = os.path.dirname(dirpath)
            print(f"Processing {dirpath}")
            
            # Move all files from run1 to parent directory
            for filename in filenames:
                src = os.path.join(dirpath, filename)
                dst = os.path.join(parent_dir, filename)
                
                # Handle potential filename conflicts by adding a prefix
                counter = 1
                while os.path.exists(dst):
                    name, ext = os.path.splitext(filename)
                    dst = os.path.join(parent_dir, f"{name}_{counter}{ext}")
                    counter += 1
                
                print(f"Moving {src} to {dst}")
                shutil.move(src, dst)
            
            # Move all subdirectories from run1 to parent directory
            for dirname in dirnames:
                src = os.path.join(dirpath, dirname)
                dst = os.path.join(parent_dir, dirname)
                
                # Handle potential directory name conflicts
                counter = 1
                while os.path.exists(dst):
                    dst = os.path.join(parent_dir, f"{dirname}_{counter}")
                    counter += 1
                
                print(f"Moving directory {src} to {dst}")
                shutil.move(src, dst)
            
            # Remove the now-empty run1 directory
            print(f"Removing empty directory {dirpath}")
            try:
                os.rmdir(dirpath)
            except OSError as e:
                print(f"Failed to remove {dirpath}: {e}")

if __name__ == "__main__":
    target_dir = "/data/hdd1/users/kmparmp/Learned-Optimizers-Benchmarking-Suite/experiments/experiment5/5.1/distribution_generalization/job_light"
    process_run1_directories(target_dir)
    print("Operation completed")