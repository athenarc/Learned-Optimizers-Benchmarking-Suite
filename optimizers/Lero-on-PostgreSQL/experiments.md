# Lero Experiment Reproduction Guide

This document provides the specific commands required to replicate the experiments presented in our paper using the Lero optimizer.

### Pre-Run Workflow: Critical Instructions

**1. Configure Lero-Patched PostgreSQL (IMPORTANT!)**
Lero requires a special PostgreSQL configuration to function. Before running any experiment, you must perform these steps on the Docker container running the database:
1.  Connect to the running Docker container (e.g., `docker exec -it evaluation_suite bash`).
2.  Navigate to the PostgreSQL data directory (e.g., `/app/db`).
3.  Replace the standard configuration with the Lero-specific one:
    ```bash
    cp postgresql_lero.conf postgresql.conf
    ```
4.  Restart the PostgreSQL server to apply the changes. You can do this by running `pg_ctl restart -D /app/db` inside the container.

**2. Configure Client Connection:**
The scripts use a `.env` file in the repository root to manage database credentials. Before running any command, create a file named `.env` in the top-level directory of this project and populate it with your connection details. You can use the provided `.env.example` as a template.

**3. Server Management:**
All scripts in this guide manage the Lero server automatically. They will start the server before execution and shut it down afterward. Please ensure no other Lero server is running before you start an experiment to avoid port conflicts.

---

## (E1) End-to-End Performance & Value Model Fidelity

This experiment evaluates the baseline performance of Lero across standard benchmarks.

### Training (`train_model.py`)
```bash
python3 train_model.py \
    --query_dir ../../../experiments/experiment1/{WORKLOAD_LOWER}/train/ \
    --output_query_latency_file ../../../experiments/experiment1/{WORKLOAD_LOWER}/results/lero_train.log \
    --model_prefix lero_e1_{WORKLOAD_LOWER} \
    --target_checkpoint_dir ../../../models/experiment1/{WORKLOAD_UPPER}/LERO/
```

### Testing (`test.py`)
```bash
python3 test.py \
    --query_path ../../../experiments/experiment1/{WORKLOAD_LOWER}/test/ \
    --output_query_latency_file ../../../experiments/experiment1/{WORKLOAD_LOWER}/results/lero_test.log \
    --checkpoint_dir {CHECKPOINT_PATH}
```

### Parameters and Models for E1
| Workload | `{WORKLOAD_LOWER}` | `{WORKLOAD_UPPER}` | Hugging Face Model for `{CHECKPOINT_PATH}` |
| :------- | :----------------- | :----------------- | :--------------------------------------- |
| **JOB**  | `job`              | `JOB`              | `experiment1/JOB/LERO/job_lero_18` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment1/JOB/LERO/job_lero_18))                 |
| **TPC-H**| `tpch`             | `TPCH`             | -               |
| **TPC-DS**| `tpcds`            | `TPCDS`            | -              |

---

## (E2) Sensitivity & Execution Stability

This experiment uses the **JOB workload**.

### Training
```bash
python3 train_model.py \
    --query_dir ../../../experiments/experiment2/job/train/{SPLIT_PHILOSOPHY}/ \
    --output_query_latency_file ../../../experiments/experiment2/job/results/lero_{SPLIT}_{ORDER}_train.log \
    --model_prefix lero_e2_{SPLIT}_{ORDER} \
    --workload_order_file ../../../experiments/experiment2/job/train/{SPLIT_PHILOSOPHY}/{ORDER_POLICY}.txt \
    --target_checkpoint_dir ../../../models/experiment2/job/{SPLIT_PHILOSOPHY}/LERO/{ORDER_POLICY}/
```

### Testing
```bash
python3 test.py \
    --query_path ../../../experiments/experiment2/job/test/{SPLIT_PHILOSOPHY}/{ORDER_POLICY}/ \
    --output_query_latency_file ../../../experiments/experiment2/job/results/lero_{SPLIT}_{ORDER}_test.log \
    --checkpoint_dir {CHECKPOINT_PATH}
```

### Parameters and Models for E2
| Split Philosophy         | Query Order Policy     | `{SPLIT_PHILOSOPHY}` | `{ORDER_POLICY}`   | Hugging Face Model Path                                                                                                          |
| :----------------------- | :--------------------- | :------------------- | :----------------- | :------------------------------------------------------------------------------------------------------------------------------- |
| **Random Split**         | Ascending Latency      | `random`             | `asc_latency`      | `experiment2/random_split/LERO/asc_complexity/checkpoints/job_random_complexity_22` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/random_split/LERO/asc_complexity/checkpoints/job_random_complexity_22)) |
|                          | Ascending Complexity   | `random`             | `asc_complexity`   | `experiment2/random_split/LERO/asc_latency/checkpoints/job_random_latency_22` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/random_split/LERO/asc_latency/checkpoints/job_random_latency_22)) |
|                          | Random                 | `random`             | `random`           | `experiment2/random_split/LERO/random/checkpoints/job_random_random_22` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/random_split/LERO/random/checkpoints/job_random_random_22)) |
| **Base Query Split**     | Ascending Latency      | `base_query`         | `asc_latency`      | `experiment2/base_query/LERO/asc_latency/checkpoints/job_test_model_23` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/base_query/LERO/asc_latency/checkpoints/job_test_model_23)) |
|                          | Ascending Complexity   | `base_query`         | `asc_complexity`   | `experiment2/base_query/LERO/asc_complexity/checkpoints/job_base_complexity_23` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/base_query/LERO/asc_complexity/checkpoints/job_base_complexity_23)) |
|                          | Random                 | `base_query`         | `random`           | `experiment2/base_query/LERO/random/checkpoints/job_base_random_23` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/base_query/LERO/random/checkpoints/job_base_random_23)) |
| **Leave-One-Out Split**  | Ascending Latency      | `leave_one_out`      | `asc_latency`      | `experiment2/leave_one_out/LERO/asc_latency/checkpoints/job_leave_latency_19` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/leave_one_out/LERO/asc_latency/checkpoints/job_leave_latency_19)) |
|                          | Ascending Complexity   | `leave_one_out`      | `asc_complexity`   | `experiment2/leave_one_out/LERO/asc_complexity/checkpoints/job_leave_complexity_18` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/leave_one_out/LERO/asc_complexity/checkpoints/job_leave_complexity_18)) |
|                          | Random                 | `leave_one_out`      | `random`           | `experiment2/leave_one_out/LERO/random/checkpoints/job_leave_random_19` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/leave_one_out/LERO/random/checkpoints/job_leave_random_19)) |

---

## (E3) Learning Trajectory & Convergence

This experiment uses dedicated scripts (`train_lero_experiment3.py`, `test_lero_experiment3.py`) on the **JOB workload**.

### Training
This script executes a long training run, saving checkpoints according to three policies.
```bash
python3 train_lero_experiment3.py \
    --query_dir ../../../experiments/experiment3/job/train/ \
    --output_query_latency_file ../../../experiments/experiment3/job/results/lero_learning_trajectory.log \
    --model_prefix lero_e3_job \
    --checkpoint_dir_base ../../../models/experiment3/LERO/
```

### Testing & Analysis
This script automatically finds and evaluates all checkpoints from the training run.
```bash
python3 test_lero_experiment3.py \
    {CHECKPOINT_PATH_BASE} \
    ../../../experiments/experiment3/job/test/
```
*   **For `{CHECKPOINT_PATH_BASE}`:** Use your trained model's base directory (e.g., `../../../models/experiment3/LERO/`).

**Model Checkpoints for E3:** `experiment3/LERO/`

| Workload | Model Path on Hugging Face | Link to Directory |
| :------- | :------------------------- | :---------------- |
| **JOB**  | `experiment3/LERO/`         | [View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment3/LERO) |

---

## (E4) Internal Decision-Making & Embeddings

This experiment is divided into sub-sections, each analyzing a different aspect of Lero's internal logic.

### 4.1 Access Path Selection

This sub-experiment tests how Lero adapts to schema changes. It requires a full training and testing cycle for both scenarios.

#### Scenario A: JOB-Synthetic Workload
This scenario uses a synthetic workload against the **standard `imdbload` database**.

**Training:**
```bash
python3 train_model.py \
    --query_dir ../../../experiments/experiment4/4.1/synthetic/ \
    --output_query_latency_file ../../../experiments/experiment4/4.1/results/lero_synthetic_train.log \
    --model_prefix lero_e4_synthetic \
    --target_checkpoint_dir ../../../models/experiment4/4.1/job_synthetic/LERO/
```

**Testing:**
```bash
python3 test.py \
    --query_path ../../../experiments/experiment4/4.1/synthetic/ \
    --output_query_latency_file ../../../experiments/experiment4/4.1/results/lero_synthetic_test.log \
    --checkpoint_dir {CHECKPOINT_PATH}
```

#### Scenario B: JOB Workload with Added Index
This scenario uses the standard JOB workload against a modified database.

**Prerequisites:**
1.  Create the modified database: `CREATE DATABASE imdbload_added_index WITH TEMPLATE imdbload;`
2.  Add the new index: `\c imdbload_added_index; CREATE INDEX idx_title_production_year ON title(production_year);`
3.  **Update `lero/test_script/config.py`** to connect to the `imdbload_added_index` database.

**Training:**
```bash
python3 train_model.py \
    --query_dir ../../../experiments/experiment4/4.1/added_index/ \
    --output_query_latency_file ../../../experiments/experiment4/4.1/results/lero_added_index_train.log \
    --model_prefix lero_e4_added_index \
    --target_checkpoint_dir ../../../models/experiment4/4.1/job_added_index/LERO/
```

**Testing:**
```bash
python3 test.py \
    --query_path ../../../experiments/experiment4/4.1/added_index/ \
    --output_query_latency_file ../../../experiments/experiment4/4.1/results/lero_added_index_test.log \
    --checkpoint_dir {CHECKPOINT_PATH}
```

#### Model Checkpoints for 4.1

For the `{CHECKPOINT_PATH}` in the testing commands, you can use your own trained models or our pre-trained models from Hugging Face.

| Scenario                | Hugging Face Model Path                  | Link to Directory |
| :---------------------- | :--------------------------------------- | :---------------- |
| **JOB-Synthetic**       | -     | - |
| **JOB with Added Index**| -     | - |


### 4.2 Physical Operator Composition

This is an inference-only test using the **pre-trained E1 model** for JOB on the standard `imdbload` database.
```bash
python3 test.py \
    --query_path ../../../experiments/experiment4/4.2/ \
    --output_query_latency_file ../../../experiments/experiment4/4.2/results/lero_test.log \
    --checkpoint_dir {CHECKPOINT_PATH_E1_MODEL}
```

### 4.3 Join Operator Prediction Accuracy

This sub-Experiment was skipped because it does not apply to LERO's setup.

### 4.4 Embedding Similarity

This sub-experiment uses a dedicated script (`test_lero_embeddings.py`) and the **pre-trained E1 model** to generate artifacts for analysis.
```bash
python3 test_lero_embeddings.py ../../../experiments/experiment4/4.4/run1/ \
    --checkpoint_dir {CHECKPOINT_PATH_E1_MODEL}
```

---

## (E5) Generalization to Novel Conditions

### 5.1 Generalization to New Queries

#### Scenario 1: Distribution Generalization
This is an inference-only test using the **pre-trained E1 model** for JOB against four new, unseen query distributions.

**Testing Template:**
```bash
python3 test.py \
    --query_path ../../../experiments/experiment5/5.1/distribution_generalization/{test_workload_name}/ \
    --output_query_latency_file ../../../experiments/experiment5/5.1/distribution_generalization/results/lero_{test_workload_name}.log \
    --checkpoint_dir {CHECKPOINT_PATH_E1_MODEL}
```
*   `{test_workload_name}` can be `job_d`, `job_extended`, `job_light`, or `job_synthetic`.

#### Scenarios 2 & 3: Complexity and Selectivity Generalization
These scenarios require a full data preparation and training cycle on their specific, pre-defined data splits.

**Training Template:**
```bash
python3 train_model.py \
    --query_dir ../../../experiments/experiment5/5.1/{generalization_dimension}/train/ \
    --output_query_latency_file ../../../experiments/experiment5/5.1/{generalization_dimension}/results/lero_train.log \
    --model_prefix lero_e5_{generalization_dimension} \
    --target_checkpoint_dir ../../../models/experiment5/5.1/{generalization_dimension}/LERO/
```

**Testing Template:**
```bash
python3 test.py \
    --query_path ../../../experiments/experiment5/5.1/{generalization_dimension}/test/ \
    --output_query_latency_file ../../../experiments/experiment5/5.1/{generalization_dimension}/results/lero_test.log \
    --checkpoint_dir {CHECKPOINT_PATH}
```
*   `{generalization_dimension}` can be `complexity_generalization` or `selectivity_generalization`.

**Model Checkpoints for 5.1:**

| Scenario                     | Hugging Face Model Path                       |
| :--------------------------- | :---------------------------------------------- |
| **Distribution Generalization**| Reuse the **Experiment 1 JOB** model. [(View Files)](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment1/JOB/LERO/job_lero_18)          |
| **Complexity Generalization**  | `experiment5/5.1/complexity_generalization/LERO/checkpoints/job_complexity_generalization_11` [(View Files)](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment5/5.1/complexity_generalization/LERO/checkpoints/job_complexity_generalization_11) |
| **Selectivity Generalization** | `experiment5/5.1/selectivity_generalization/LERO/checkpoints/job_selectivity_generalization_14` [(View Files)](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment5/5.1/selectivity_generalization/LERO/checkpoints/job_selectivity_generalization_14) |

### 5.2 Adaptability to New Schemas

This sub-experiment requires a full training cycle for each scenario.

#### Scenario A: Workload-Specific Schema Shift (JOB)
**Training:**
```bash
python3 train_model.py \
    --query_dir ../../../experiments/experiment5/5.2/job/train/ \
    --output_query_latency_file ../../../experiments/experiment5/5.2/job/results/lero_train.log \
    --model_prefix lero_e5_job_schema \
    --target_checkpoint_dir ../../../models/experiment5/5.2/job/LERO/
```

**Testing:**
```bash
python3 test.py \
    --query_path ../../../experiments/experiment5/5.2/job/test/ \
    --output_query_latency_file ../../../experiments/experiment5/5.2/job/results/lero_test.log \
    --checkpoint_dir {CHECKPOINT_PATH}
```

#### Scenario B: Structural Schema Shift (SSB)
**Prerequisites:** Update `lero/test_script/config.py` to connect to the **SSB database**.

**Training:**
```bash
python3 train_model.py \
    --query_dir ../../../experiments/experiment5/5.2/ssb/train/ \
    --output_query_latency_file ../../../experiments/experiment5/5.2/ssb/results/lero_train.log \
    --model_prefix lero_e5_ssb_schema \
    --target_checkpoint_dir ../../../models/experiment5/5.2/ssb/LERO/
```

**Testing:**
```bash
python3 test.py \
    --query_path ../../../experiments/experiment5/5.2/ssb/test/ \
    --output_query_latency_file ../../../experiments/experiment5/5.2/ssb/results/lero_test.log \
    --checkpoint_dir {CHECKPOINT_PATH}
```

**Scenarios, Parameters, and Models for 5.2:**

| Scenario                     | Prerequisites                                   | `{SCHEMA_TYPE}` | `{DB_NAME}` | Hugging Face Model Path        |
| :--------------------------- | :---------------------------------------------- | :-------------- | :---------- | :----------------------------- |
| **Workload-Specific Schema Shift** | Uses standard `imdbload` DB.                  | `job`           | `imdbload`  | `experiment5/5.2/job/LERO/checkpoints/job_workload_generalization_17` [(View Files)](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment5/5.2/job/LERO/checkpoints/job_workload_generalization_17)    |
| **Structural Schema Shift**    | Must connect to the **SSB database**. | `ssb`           | `ssb`       | `experiment5/5.2/ssb/LERO/checkpoints/ssb_workload_generalization_4` [(View Files)](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment5/5.2/ssb/LERO/checkpoints/ssb_workload_generalization_4)   |

### 5.3 Adaptation to Distribution Shifts.

(Guide for 5.3 will be included once we have uploaded the datasets to huggingface)