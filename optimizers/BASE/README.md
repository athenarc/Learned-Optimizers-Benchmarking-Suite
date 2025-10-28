# BASE Optimizer

This directory contains the implementation of BASE, a two-stage reinforcement learning framework that bridges the gap between cost-model-based and latency-based training for query optimization.

This guide provides instructions for using BASE within the evaluation suite.

### Prerequisites

1.  The main Docker environment for the evaluation suite must be built and running.
2.  You have a Conda installation (e.g., [Miniconda](https://docs.conda.io/en/latest/miniconda.html)).

---

## 1. Environment Setup

All BASE commands must be run from a dedicated Conda environment.

1.  **Create and activate the Conda environment:**
    ```bash
    conda env create -f environment.yml
    conda activate base
    ```

---

## 2. General Usage

BASE uses a two-stage approach. First, a model is pre-trained using the PostgreSQL cost model. Second, this model is fine-tuned using actual query execution latencies.

**Important Note:** The paths for the training and testing workloads are **hardcoded** in the Python scripts. You must edit these files before running.

### Stage 1: Pre-training (Cost-based)

1.  **Edit `train_base.py`:** Open `train_base.py` and modify the `sql_directory` variable on line 44 to point to your training workload.
    ```python
    # in train_base.py
    sql_directory = "/path/to/training/workload/"
    ```
2.  **Run the pre-training script:**
    ```bash
    python3 train_base.py --path <path/to/save/model.pth> --epoch_end <num_epochs> [other_hyperparameters...]
    ```
    *   `--path`: Specifies the file path to save the final pre-trained model.
    *   `--epoch_end`: The number of epochs to train for.

### Stage 2: Fine-tuning & Evaluation (Latency-based)

1.  **Edit `Transfer_Active_job.py`:** Open `Transfer_Active_job.py` and modify the `sql_directory` variable on line 34 to point to your testing/evaluation workload.
    ```python
    # in Transfer_Active_job.py
    sql_directory = "/path/to/testing/workload/"
    ```
2.  **Run the fine-tuning/testing script:**
    ```bash
    python3 Transfer_Active_job.py
    ```

---

## 3. Status in this Evaluation Suite: Disclaimer

Despite significant efforts, we were unable to get the BASE optimizer fully operational within our evaluation framework. We encountered persistent integration and execution challenges that prevented us from successfully training the model and generating stable results.

Consequently:
*   **There are no results for BASE reported in our paper.**
*   We do not provide an `experiments.md` file with specific commands for our experimental suite (E1-E5) for this optimizer.

The general usage instructions in the section above are preserved from the original authors' documentation for completeness, should other researchers wish to attempt to use this codebase.

---

## 4. Reference from original BASE Documentation

<details>
<summary><b>Click to expand for key concepts from the original BASE documentation.</b></summary>

### The Two-Stage Framework

The core idea of BASE is to leverage the speed of the PostgreSQL cost model for efficient pre-training, which quickly teaches the model the general structure of good query plans. Then, it uses latency-based fine-tuning with real query executions to adapt the model to the specific hardware and data distribution, bridging the gap between estimated cost and true latency.

### Pre-training Dataset

The original authors provide a specific set of pre-training queries for the IMDB (JOB) workload, which can be found at this [Google Drive link](https://drive.google.com/drive/folders/16Dguw7xDWR19K_B7ZPUscfdCRg5r5mQ4?usp=drive_link).

For more details, please refer to the complete [`original_documentation.md`](original_documentation.md) file.

</details>