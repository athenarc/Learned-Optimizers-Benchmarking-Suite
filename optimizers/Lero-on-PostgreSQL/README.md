# Lero Optimizer

This directory contains the implementation of Lero, a learned query optimizer that follows a learning-to-rank paradigm to select the best query plan.

This guide provides instructions for using Lero within the evaluation suite.

### Prerequisites

1.  The main Docker environment for the evaluation suite must be built and running. It includes the version of PostgreSQL specifically patched for Lero.
2.  You have a Conda installation (e.g., [Miniconda](https://docs.conda.io/en/latest/miniconda.html)).

---

## 1. Environment Setup

All Lero commands must be run from a dedicated Conda environment.

1.  **Navigate to the Lero directory:**
    ```bash
    cd optimizers/Lero-on-PostgreSQL/lero
    ```
2.  **Create and activate the Conda environment:**
    ```bash
    conda env create -f environment.yml
    conda activate lero
    ```

---

## 2. Running Lero

This section provides general instructions for using the Lero optimizer.

### Pre-Run Workflow: Critical Instructions

**IMPORTANT:** Before running any Lero script, two manual configuration steps are required.

**1. Configure Lero-Patched PostgreSQL:**
Lero requires a special PostgreSQL configuration to function. You must perform these steps on the Docker container running the database:
1.  Connect to the running Docker container (e.g., `docker exec -it evaluation_suite bash`).
2.  Navigate to the PostgreSQL data directory (e.g., `/app/db`).
3.  Replace the standard configuration with the Lero-specific one:
    ```bash
    cp postgresql_lero.conf postgresql.conf
    ```
4.  Restart the PostgreSQL server to apply the changes. You can do this by running `pg_ctl restart -D /app/db` inside the container or by restarting the Docker container itself (`docker compose restart evaluation_suite`).

**2. Configure Client Connection:**
The scripts use a `.env` file in the repository root to manage database credentials. Before running any command, create a file named `.env` in the top-level directory of this project and populate it with your connection details. You can use the provided `.env.example` as a template.

**3. Server Management:**
All scripts in this guide now **manage the Lero server automatically**. They will start the server before execution and shut it down afterward. Please ensure no other Lero server is running before you start an experiment to avoid port conflicts.

### General Training Command (`train_model.py`)
This script trains a new Lero model from scratch.

```bash
python3 train_model.py --query_dir <path/to/train/queries/> \
                       --output_query_latency_file <path/to/results.log> \
                       --model_prefix <model_name> \
                       --target_checkpoint_dir <path/to/save/checkpoints/>
```
*   `--query_dir`: Path to the directory containing the training workload SQL files.
*   `--output_query_latency_file`: The log file for executed plan latencies during training.
*   `--model_prefix`: A prefix for the saved model files within the Lero server directory.
*   `--target_checkpoint_dir`: The directory where the final trained model will be saved.

### General Testing Command (`test.py`)
This script evaluates a pre-trained Lero model.

```bash
python3 test.py --query_path <path/to/test/queries/> \
                --output_query_latency_file <path/to/results.log> \
                --checkpoint_dir <path/to/load/checkpoints/>
```
*   `--query_path`: Path to the directory containing the test queries.
*   `--output_query_latency_file`: The file where test results will be logged.
*   `--checkpoint_dir`: The directory containing the pre-trained model to evaluate.

---

## 3. Replicating Paper Experiments

For the exact commands, model paths, and setup needed to generate the results for each experiment (E1-E5) in our paper, refer to the detailed guide below.

👉 [**Lero Experiment Reproduction Commands**](experiments.md)

---

## 4. Reference from original Lero Documentation

<details>
<summary><b>Click to expand for key concepts from the original Lero documentation.</b></summary>

### Learning-to-Rank Paradigm

The core idea of Lero is that learning the relative order (or rank) of query plans is an easier and more robust machine learning task than predicting the absolute latency of each plan. By focusing on ranking, Lero aims to build a more effective learned optimizer.

### Modified PostgreSQL

Lero requires a modified version of PostgreSQL to communicate with its external server. The `Dockerfile` in this evaluation suite **already handles this for you**. It applies the necessary patch (`0001-init-lero.patch`) during the build process, so you do not need to manually download, patch, or compile PostgreSQL.

For more details on the original setup and architecture, please refer to the complete [`original_documentation.md`](original_documentation.md) file.

</details>