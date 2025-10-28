# BAO: The Bandit Optimizer

This directory contains the implementation of Bao, a learned query optimizer that "steers" the PostgreSQL optimizer using hints. For more technical details, please refer to the [original paper](https://rm.cab/bao).

This guide provides instructions for using Bao within the evaluation suite.

### Prerequisites

1.  The main Docker environment for the evaluation suite must be built and running.
2.  You have a Conda installation (e.g., [Miniconda](https://docs.conda.io/en/latest/miniconda.html)).

---

## 1. Environment Setup

All BAO commands must be run from a dedicated Conda environment.

1.  **Create and activate the Conda environment:**
    ```bash
    conda env create -f environment.yml
    conda activate bao
    ```

Your environment is now ready.

---

## 2. Running BAO

Running BAO requires a two-step process in separate terminal sessions: first, you start the BAO server, and second, you run the training or testing script which acts as a client.

### Pre-Run: BAO State Cleanup (Important!)

To ensure a clean slate and reproducible results, you **must** clear any previous BAO state before starting a **new training session**.

1.  Navigate to the `bao_server` directory.
2.  Run the following cleanup commands:
    ```bash
    cd bao_server
    rm -f bao.db
    rm -rf bao_default_model
    python3 clean_experience.py
    cd ..
    ```

### Step A: Start the BAO Server
In your first terminal, start the server. It will wait for connections from the client script.
```bash
cd bao_server
python3 main.py
```

Keep this server running for the duration of your training or testing session.

### Step B: Run Training or Testing

In a second terminal, you will run the client scripts.

#### General Training Command
```bash
python3 train.py --query_dir <path/to/train/queries/> \
                 --output_file <path/to/results.txt> \
                 --target_checkpoint_dir <path/to/save/checkpoints/>
```
   - `--query_dir`: Path to the directory containing the training workload SQL files.
   - `--output_file`: Path where the training logs and latencies will be saved.
   - `--target_checkpoint_dir`: The directory where the final trained model will be saved.

#### General Testing Command

```bash
python3 test_run.py <path/to/test/queries/> \
                     <path/to/results.txt> \
                     <database_name> \
                     --checkpoint_dir <path/to/load/checkpoints/>
```

   - `<path/to/test/queries/>`: (Positional) Path to the directory with test queries.
   - `<path/to/results.txt>`: (Positional) Path to save the output results.
   - `<database_name>`: (Positional) The name of the database to connect to (e.g., imdbload).
   - `--checkpoint_dir`: The directory containing the pre-trained model to evaluate.   

---

### 3. Replicating Paper Experiments

For the exact commands, model paths, and setup needed to generate the results for each experiment (E1-E5) in our paper, refer to the detailed guide below.

👉 [BAO Experiment Reproduction Commands](experiments.md)

---

### 4. Reference from Original BAO Documentation
<details>
<summary><b>Click to expand for key concepts from the original BAO documentation.</b></summary>

#### Core Concepts

   - **Reinforcement Learning Approach**: BAO is designed as a reinforcement learning system that learns continuously from a stream of queries. Unlike supervised learning, the concepts of a fixed "training set" and "testing set" are less distinct. Performance at any given time depends on the queries it has seen previously. For robust evaluation, it's best to test on multiple random orderings of a workload.

   - **High Planning Time**: This prototype plans each potential hint (arm) sequentially. This means if you use 5 arms, BAO calls the PostgreSQL planner 5 times per query, increasing optimization overhead. This is less impactful for long-running queries but can be significant for shorter ones.

   - **Hint/Arm Tuning is Critical**: The default set of 5 arms was chosen for a specific hardware configuration. Performance on different hardware will likely require tuning this set of hints. The best way to do this is to manually test all possible arms on your workload and select the most promising subset.
 
For more details, please refer to the complete [original_documentation.md](original_documentation.md) file.

</details>