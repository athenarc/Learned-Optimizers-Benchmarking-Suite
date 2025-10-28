# LOGER Optimizer

This directory contains the implementation of LOGER, as well as instructions for using LOGER within the evaluation suite.

### Prerequisites

1.  The main Docker environment for the evaluation suite must be built and running.
2.  You have a Conda installation (e.g., [Miniconda](https://docs.conda.io/en/latest/miniconda.html)).

---

## 1. Environment Setup

All LOGER commands must be run from a dedicated Conda environment.

1.  **Create and activate the Conda environment:**
    ```bash
    conda env create -f environment.yml
    conda activate loger
    ```
The `environment.yml` file handles all necessary dependencies, including PyTorch, DGL, and `psqlparse`.

---

## 2. Running LOGER

This section provides general instructions for training and testing the LOGER optimizer.

### Pre-Run Workflow: Critical Instructions

Before running any experiment, please follow these essential setup steps:

**1. Configure Database Connection:**
The scripts use a `.env` file in the repository root to manage database credentials. Before running, create a file named `.env` in the top-level project directory and populate it with your connection details. You can use the provided `.env.example` as a template.

**2. Clean Temporary Files (IMPORTANT!)**
To ensure a clean slate and prevent state from a previous run from interfering with results, you **must** clear the temporary dataset and checkpoint files before starting a **new training session**.

```bash
rm -f temps/*.pkl
rm -f pretrained/*.pkl
rm -f sql/*.pkl
rm -f log/*.log
```

### General Training Command (`train.py`)
This is the standard script for training a LOGER model from scratch.

```bash
python3 train.py -d <path/to/train_dir> <path/to/test_dir> \
                 -D <database_name> \
                 --target_checkpoint_dir <path/to/save/checkpoints/>
```
*   `-d <train_dir> <test_dir>`: (Required) Paths to the training and testing workload directories.
*   `-D <database_name>`: Specifies the PostgreSQL database name to connect to.
*   `--target_checkpoint_dir`: (Required) The directory where the final trained model and artifacts will be saved.

### General Testing Command (`test.py`)
This is the standard script for evaluating a pre-trained LOGER model.

```bash
python3 test.py <path/to/test/queries/> \
                -D <database_name> \
                --checkpoint_dir <path/to/load/checkpoints/> \
```
*   `<path/to/test/queries/>`: (Positional) Path to the directory containing the test queries.
*   `-D <database_name>`: The name of the database to connect to.
*   `--checkpoint_dir`: (Required) The directory containing the pre-trained model to evaluate.

---

## 3. Replicating Paper Experiments

For the exact commands, model paths, and setup needed to generate the results for each experiment (E1-E5) in our paper, refer to the detailed guide below.

👉 [**LOGER Experiment Reproduction Commands**](experiments.md)

---

## 4. LOGER Concepts and Notes

<details>
<summary><b>Click to expand for key concepts from the original LOGER documentation.</b></summary>

### Key Training Arguments

You can control LOGER's behavior with several important flags during training:

*   `-b <beam_size>`: Sets the beam size for the plan search (default: `4`).
*   `-w <weight>`: Specifies the weight factor for reward weighting (default: `0.1`).
*   `--bushy`: Allows the generation of bushy execution plans.
*   `--no-exploration`: Disables ε-beam search and uses a standard beam search instead.
*   `--no-expert-initialization`: Prevents the experience dataset from being initialized with plans from the native PostgreSQL optimizer.
*   `--seed <value>`: Sets the random seed for training to ensure reproducibility.

For a complete list of all available arguments, please refer to the complete [`original_documentation.md`](original_documentation.md) file.

</details>