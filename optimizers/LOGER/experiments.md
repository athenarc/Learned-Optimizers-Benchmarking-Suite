# LOGER Experiment Reproduction Guide

This document provides the specific commands required to replicate the experiments presented in our paper using the LOGER optimizer.

### Pre-Run Workflow: Critical Instructions

**1. Configure Database Connection:**
The scripts use a `.env` file in the repository root to manage database credentials. Before running any command, create a file named `.env` in the top-level directory of this project and populate it with your connection details. You can use the provided `.env.example` as a template.

**2. Clean Temporary Files (IMPORTANT!)**
To ensure a clean slate and prevent state from a previous run from interfering with results, you **must** clear the temporary dataset and checkpoint files before starting a **training/testing session**.

```bash
rm -f temps/*.pkl
rm -f pretrained/*.pkl
rm -f sql/*.pkl
rm -f log/*.log
```

---

## (E1) End-to-End Performance & Value Model Fidelity

This experiment evaluates the baseline performance of LOGER across standard benchmarks.

### Command Templates for E1
Choose a workload from the table below and substitute the values into the `{placeholders}` in these templates.

**Training Template:**
```bash
python3 train.py \
    -d ../../experiments/experiment1/{WORKLOAD_LOWER}/train/ ../../experiments/experiment1/{WORKLOAD_LOWER}/test/ \
    -D {DB_NAME} \
    --target_checkpoint_dir ../../models/experiment1/{WORKLOAD_UPPER}/LOGER/
```

**Testing Template:**
```bash
python3 test.py ../../experiments/experiment1/{WORKLOAD_LOWER}/test/ \
    -D {DB_NAME} \
    --checkpoint_dir {CHECKPOINT_PATH} \
```

### Parameters and Models for E1
| Workload | `{WORKLOAD_LOWER}` | `{WORKLOAD_UPPER}` | `{DB_NAME}` | Hugging Face Model for Testing |
| :------- | :----------------- | :----------------- | :---------- | :----------------------------- |
| **JOB**  | `job`              | `JOB`              | `imdbload`  | `experiment1/JOB/LOGER/final_model/` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment1/JOB/LOGER/final_model)) |
| **TPC-H**| `tpch`             | `TPCH`             | `tpch`      | `experiment1/TPCH/LOGER/` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment1/TPCH/LOGER))|
| **TPC-DS**| `tpcds`            | `TPCDS`            | `tpcds`     | `experiment1/TPCDS/LOGER/` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment1/TPCDS/LOGER))|

*   **For `{CHECKPOINT_PATH}`:** Use your trained model path (e.g., `../../models/experiment1/JOB/LOGER/final_model/`) or the downloaded pre-trained model path.

---

## (E2) Sensitivity & Execution Stability

This experiment uses the **JOB workload**.

### Command Templates for E2
Substitute the `{placeholders}` from the table below.

**Training Template:**
```bash
python3 train.py \
    -d ../../experiments/experiment2/job/train/{SPLIT_PHILOSOPHY}/ ../../experiments/experiment2/job/test/{SPLIT_PHILOSOPHY}/{ORDER_POLICY}/ \
    -D imdbload \
    -o ../../experiments/experiment2/job/train/{SPLIT_PHILOSOPHY}/{ORDER_POLICY}.txt \
    --target_checkpoint_dir ../../models/experiment2/job/{SPLIT_PHILOSOPHY}/LOGER/{ORDER_POLICY}/
```

**Testing Template:**
```bash
python3 test.py ../../experiments/experiment2/job/test/{SPLIT_PHILOSOPHY}/{ORDER_POLICY}/ \
    -D imdbload \
    --checkpoint_dir {CHECKPOINT_PATH} \
```

### Parameters and Models for E2

| Split Philosophy         | Query Order Policy     | `{SPLIT_PHILOSOPHY}` | `{ORDER_POLICY}`   | Hugging Face Model Path                                                                                                          |
| :----------------------- | :--------------------- | :------------------- | :----------------- | :------------------------------------------------------------------------------------------------------------------------------- |
| **Random Split**         | Ascending Latency      | `random`             | `asc_latency`      | `experiment2/random_split/LOGER/asc_latency/final_model` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/random_split/LOGER/asc_latency/final_model)) |
|                          | Ascending Complexity   | `random`             | `asc_complexity`   | `experiment2/random_split/LOGER/asc_complexity/final_model` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/random_split/LOGER/asc_complexity/final_model)) |
|                          | Random                 | `random`             | `random`           | `experiment2/random_split/LOGER/random/final_model` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/random_split/LOGER/random)) |
| **Base Query Split**     | Ascending Latency      | `base_query`         | `asc_latency`      | `experiment2/base_query/LOGER/asc_latency/final_model` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/base_query/LOGER/asc_latency/final_model/)) |
|                          | Ascending Complexity   | `base_query`         | `asc_complexity`   | `experiment2/base_query/LOGER/asc_complexity/final_model` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/base_query/LOGER/asc_complexity/final_model)) |
|                          | Random                 | `base_query`         | `random`           | `experiment2/base_query/LOGER/random/final_model` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/base_query/LOGER/random/final_model)) |
| **Leave-One-Out Split**  | Ascending Latency      | `leave_one_out`      | `asc_latency`      | `experiment2/leave_one_out/LOGER/asc_latency/final_model` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/leave_one_out/LOGER/asc_latency/final_model)) |
|                          | Ascending Complexity   | `leave_one_out`      | `asc_complexity`   | `experiment2/leave_one_out/LOGER/asc_complexity/final_model` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/leave_one_out/LOGER/asc_complexity/final_model)) |
|                          | Random                 | `leave_one_out`      | `random`           | `experiment2/leave_one_out/LOGER/random/final_model` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/leave_one_out/LOGER/random/final_model)) |

---

## (E3) Learning Trajectory & Convergence

This experiment uses dedicated scripts on the **JOB workload**.

### Training
Run the `loger_train_experiment3.py` script. It will execute a long training run, saving checkpoints according to three policies into subdirectories.
```bash
python3 loger_train_experiment3.py \
    -d ../../experiments/experiment3/job/train/ ../../experiments/experiment3/job/test/ \
    -D imdbload \
    --checkpoint_dir_base ../../models/experiment3/LOGER/
```
This creates `epoch_checkpoints`, `loss_checkpoints`, and `query_checkpoints` inside the base directory.

### Testing & Analysis
The `test_loger_experiment3.py` script automatically finds and evaluates all checkpoints from the training run.
```bash
python3 test_loger_experiment3.py \
    {CHECKPOINT_PATH_BASE} \
    ../../experiments/experiment3/job/test/ \
    imdbload
```
*   **For `{CHECKPOINT_PATH_BASE}`:** Use your trained model's base directory (e.g., `../../models/experiment3/LOGER/`) or the downloaded pre-trained models' base directory.

**Model Checkpoints for E3:**

| Workload | Hugging Face Model Path | Link to Directory |
| :------- | :---------------------- | :---------------- |
| **JOB**  | `experiment3/LOGER/`    | [View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment3/LOGER) |

---

## (E4) Internal Decision-Making & Embeddings

### 4.1 Access Path Selection

This sub-experiment tests how LOGER adapts to different access paths, using both a synthetic workload and a schema change (an added index).

#### Scenario A: JOB-Synthetic Workload

This scenario uses a synthetic workload designed to test access path selection against the **standard `imdbload` database**.

**Training:**
```bash
python3 train.py \
    -d ../../experiments/experiment4/4.1/synthetic/ ../../experiments/experiment4/4.1/synthetic/ \
    -D imdbload \
    --target_checkpoint_dir ../../models/experiment4/4.1/job_synthetic/LOGER/
```

**Testing:**
```bash
python3 test.py ../../experiments/experiment4/4.1/synthetic/ \
    -D imdbload \
    --checkpoint_dir {CHECKPOINT_PATH} \
    -o ../../experiments/experiment4/4.1/results/loger_synthetic_test.csv
```

#### Scenario B: JOB Workload with Added Index

This scenario uses the standard JOB workload against a modified database containing a new index.

**Prerequisites:** This scenario requires a modified database schema.
1.  Connect to your PostgreSQL instance and clone the `imdbload` database:
    ```sql
    CREATE DATABASE imdbload_added_index WITH TEMPLATE imdbload;
    ```
2.  Connect to the new database and add an index:
    ```sql
    \c imdbload_added_index
    CREATE INDEX idx_title_production_year ON title(production_year);
    ```

**Training:**
```bash
python3 train.py \
    -d ../../experiments/experiment4/4.1/added_index/ ../../experiments/experiment4/4.1/added_index/ \
    -D imdbload_added_index \
    --target_checkpoint_dir ../../models/experiment4/4.1/job_added_index/LOGER/
```

**Testing:**
```bash
python3 test.py ../../experiments/experiment4/4.1/added_index/ \
    -D imdbload_added_index \
    --checkpoint_dir {CHECKPOINT_PATH} \
```

#### Model Checkpoints for 4.1

For the `{CHECKPOINT_PATH}` in the testing commands, you can use your own trained models or our pre-trained models from Hugging Face.

| Scenario                | Hugging Face Model Path                  | Link to Directory |
| :---------------------- | :--------------------------------------- | :---------------- |
| **JOB-Synthetic**       | -     | - |
| **JOB with Added Index**| `experiment4/4.1/job_added_index/LOGER/`   | [View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment4/4.1/job_added_index/LOGER) |

### 4.2 Physical Operator Composition

This sub-experiment evaluates LOGER's ability to compose different physical operators using the standard `imdbload` database.

**Testing:**
This test uses the **pre-trained model from Experiment 1** for the JOB workload.

```bash
python3 test.py ../../experiments/experiment4/4.2/ \
    -D imdbload \
    --checkpoint_dir {CHECKPOINT_PATH_E1_MODEL} \
```
*   **For `{CHECKPOINT_PATH_E1_MODEL}`:** Use the path to the pre-trained model for JOB from **Experiment 1**.

### 4.3 Join Operator Prediction Accuracy

This sub-experiment tests LOGER's accuracy when its internal action space is restricted to a single join operator.

**Required Setup for 4.3:**

**Important:** Before running, you must modify `model/dqn.py` to limit the available join operators for the model.

1.  Open `model/dqn.py` and go to the `join_method` definition (around line 273).
2.  **Uncomment the line** for the single join operator you wish to test and ensure all others are commented out.

**Example (to test with Hash Join only):**
```python
# In model/dqn.py

# Default (Commented out)
# join_method = Plan.NEST_LOOP_JOIN
# join_method = Plan.MERGE_JOIN
# join_method = Plan.HASH_JOIN

# HJ-only (Active)
join_method = Plan.NEST_LOOP_JOIN

# ... (and so on for other operators)
```

**Testing:**
This test uses the **pre-trained model from Experiment 1**. Use the table below to substitute the `{join_operator}` placeholder in the command template.

**Testing Template:**
```bash
python3 test.py ../../experiments/experiment4/4.3/{join_operator}/ \
    -D imdbload \
    --checkpoint_dir {CHECKPOINT_PATH_E1_MODEL} \
```
*   **For `{CHECKPOINT_PATH_E1_MODEL}`:** Use the path to the pre-trained model for JOB from **Experiment 1**.

**Parameters for 4.3:**
| Scenario              | `{join_operator}` |
| :-------------------- | :---------------- |
| **Hash Join only**    | `hashjoin`        |
| **Merge Join only**   | `mergejoin`       |
| **Nested Loop only**  | `nestloop`        |

### 4.4 Embedding Similarity
This sub-experiment uses the dedicated `test_loger_embeddings.py` script and the **pre-trained E1 model**.
```bash
python3 test_loger_embeddings.py ../../experiments/experiment4/4.4/run1/ \
    -D imdbload \
    --checkpoint_path {PATH_TO_E1_MODEL_PKL_FILE}
```
> **Note:**  `--checkpoint_path` must point to the specific `.pkl` model file from the E1 checkpoint, not just the directory.

---

## (E5) Generalization to Novel Conditions

### 5.1 Generalization to New Queries
**Scenario 1: Distribution Generalization**
*   **Training:** Use the model trained for **Experiment 1 (JOB workload)**.
*   **Testing Template:**
    ```bash
    python3 test.py ../../experiments/experiment5/5.1/distribution_generalization/{test_workload_name}/ \
                     -D imdbload --checkpoint_dir {CHECKPOINT_PATH_E1_MODEL}
    ```
    *   `{test_workload_name}` can be `job_d`, `job_extended`, `job_light`, or `job_synthetic`.

**Scenarios 2 & 3: Complexity and Selectivity Generalization**
*   **Training Template:**
    ```bash
    python3 train.py -d ../../experiments/5.1/{generalization_dimension}/train/ ../../experiments/5.1/{generalization_dimension}/test/ \
    -D imdbload --checkpoint_dir ../../models/experiment5/5.1/{generalization_dimension}/LOGER/
    ```
*   **Testing Template:**
    ```bash
    python3 test.py ../../experiments/5.1/{generalization_dimension}/test/ -D imdbload --checkpoint_dir {CHECKPOINT_PATH}
    ```

**Parameters:**
| Scenario                   | `{generalization_dimension}` |
| :------------------------- | :--------------------------- |
| **Complexity Generalization**| `complexity_generalization`  |
| **Selectivity Generalization**| `selectivity_generalization` |

**Model Checkpoints for 5.1:**

| Scenario                     | Hugging Face Model Path                       |
| :--------------------------- | :---------------------------------------------- |
| **Distribution Generalization**| Reuse the **Experiment 1 JOB** model. [(View Files)](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment1/JOB/LOGER/final_model)          |
| **Complexity Generalization**  | `experiment5/5.1/complexity_generalization/LOGER/final_model` [(View Files)](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment5/5.1/complexity_generalization/LOGER/final_model) |
| **Selectivity Generalization** | `experiment5/5.1/selectivity_generalization/LOGER/final_model` [(View Files)](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment5/5.1/selectivity_generalization/LOGER/final_model) |

### 5.2 Adaptability to New Schemas
This sub-experiment follows a standard train/test procedure.

**Training Template:**
```bash
python3 train.py -d ../../experiments/5.2/{SCHEMA_TYPE}/train/ ../../experiments/5.2/{SCHEMA_TYPE}/test/ -D {DB_NAME} \
        --target_checkpoint_dir ../../models/experiment5/5.2/{SCHEMA_TYPE}/LOGER/
```
**Testing Template:**
```bash
python3 test.py ../../experiments/5.2/{SCHEMA_TYPE}/test/ -D {DB_NAME} --checkpoint_dir {CHECKPOINT_PATH}
```

**Scenarios, Parameters, and Models for 5.2:**

| Scenario                     | Prerequisites                                   | `{SCHEMA_TYPE}` | `{DB_NAME}` | Hugging Face Model Path        |
| :--------------------------- | :---------------------------------------------- | :-------------- | :---------- | :----------------------------- |
| **Workload-Specific Schema Shift** | Uses standard `imdbload` DB.                  | `job`           | `imdbload`  | `experiment5/5.2/job/LOGER/final_model` [(View Files)](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment5/5.2/job/LOGER/final_model)    |
| **Structural Schema Shift**    | Must connect to the **SSB database**. | `ssb`           | `ssb`       | `experiment5/5.2/ssb/LOGER/final_model` [(View Files)](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment5/5.2/ssb/LOGER/final_model)   |

### 5.3 Adaptation to Distribution Shifts.

(Guide for 5.3 will be included once we have uploaded the datasets to huggingface)