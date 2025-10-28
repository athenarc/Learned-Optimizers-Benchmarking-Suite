# BAO Experiment Reproduction Guide

This document provides the specific commands required to replicate the experiments presented in our paper using the BAO optimizer.

### Pre-Run Workflow: Critical Instructions

**1. Configure Database Connection**
The scripts use a `.env` file in the repository root to manage database credentials. Before running any command, create a file named `.env` in the top-level directory of this project and populate it with your connection details. You can use the provided `.env.example` as a template.

**2. BAO State Cleanup (IMPORTANT!)**
To ensure a clean slate before any training run, you **must** clear the previous BAO state. Navigate to the `bao_server` directory and run the following commands:

```bash
cd bao_server
rm -f bao.db
rm -rf bao_default_model
python3 clean_experience.py
cd ..
```

**3. Server Management (Script-Dependent):**
The different scripts manage the BAO server differently. Pay close attention to the instructions in each experiment section.
*   **Manual Start (`train.py`):** This script requires you to start the BAO server in a separate terminal before running it.
*   **Automatic Management (`test_run.py`, `test_e4_3.py`):** These scripts handle starting and stopping the server automatically. Ensure no other BAO server is running.

---

## (E1) End-to-End Performance & Value Model Fidelity

This experiment evaluates the baseline performance of BAO across standard benchmarks.

### Training
Follow this **3-step process** for each workload:
1.  **Perform State Cleanup:** Run the cleanup commands listed in the "Pre-Run Workflow" section.
2.  **Start BAO Server:** In a separate terminal, run `cd bao_server && python3 main.py`.
3.  **Run Training Script:** Use the corresponding command from the table below.

| Workload | Training Command (`train.py`) |
| :------- | :--------------------------------------------------------------------------------------------------------- |
| **JOB**  | `python3 train.py --query_dir ../../experiments/experiment1/job/train/ --output_file ../../experiments/experiment1/job/results/bao_train.log --target_checkpoint_dir ../../models/experiment1/JOB/BAO/`      |
| **TPC-H**| `python3 train.py --query_dir ../../experiments/experiment1/tpch/train/ --output_file ../../experiments/experiment1/tpch/results/bao_train.log --target_checkpoint_dir ../../models/experiment1/TPCH/BAO/`     |
| **TPC-DS**| `python3 train.py --query_dir ../../experiments/experiment1/tpcds/train/ --output_file ../../experiments/experiment1/tpcds/results/bao_train.log --target_checkpoint_dir ../../models/experiment1/TPCDS/BAO/`    |

### Testing
The `test_run.py` script manages the server and state cleanup automatically. Choose a workload, substitute `{CHECKPOINT_PATH}`, and run the command.

**Testing Template:**
```bash
python3 test_run.py ../../experiments/experiment1/{WORKLOAD_LOWER}/test/ \
                     ../../experiments/experiment1/{WORKLOAD_LOWER}/results/bao_test.log \
                     {DB_NAME} \
                     --checkpoint_dir {CHECKPOINT_PATH}
```

**Parameters and Models for E1**:
| Workload | `{WORKLOAD_LOWER}` | `{DB_NAME}` | Hugging Face Model Path           |
| :------- | :----------------- | :---------- | :-------------------------------- |
| **JOB**  | `job`              | `imdbload`  | `experiment1/JOB/BAO/20250522_095820_bao_default_model` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment1/JOB/BAO/20250522_095820_bao_default_model)) |
| **TPC-H**| `tpch`             | `tpch`      | `experiment1/TPCH/BAO/20250328_144832_bao_default_model` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment1/TPCH/BAO/20250328_144832_bao_default_model)) |
| **TPC-DS**| `tpcds`            | `tpcds`     | `experiment1/TPCDS/BAO/20250328_134134_bao_default_model` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment1/TPCDS/BAO/20250328_134134_bao_default_model)) |

*   **For `{CHECKPOINT_PATH}`:** Use your trained model path (e.g., `../../models/experiment1/JOB/BAO/`) or the downloaded pre-trained model path.

---

## (E2) Sensitivity & Execution Stability

This experiment uses the **JOB workload**.

### Training
Follow this **3-step process** for each scenario:
1.  **Perform State Cleanup.**
2.  **Start BAO Server** in a separate terminal.
3.  **Run Training Script** using the template and parameters below.

**Training Template:**
```bash
python3 train.py \
    --query_dir ../../experiments/experiment2/train/{SPLIT_PHILOSOPHY}/ \
    --query_order_file ../../experiments/experiment2/train/{SPLIT_PHILOSOPHY}/{ORDER_POLICY}.txt
    --output_file ../../experiments/experiment2/job/results/{SPLIT_PHILOSOPHY}_{ORDER_POLICY}_train.log \
    --target_checkpoint_dir ../../models/experiment2/{SPLIT_PHILOSOPHY}/BAO/{ORDER_POLICY}/
```

### Testing
The `test_run.py` script manages the server automatically.

**Testing Template:**
```bash
python3 test_run.py ../../experiments/experiment2/job/test/{SPLIT_PHILOSOPHY}/{ORDER_POLICY}/ \
                     ../../experiments/experiment2/job/results/{SPLIT_PHILOSOPHY}_{ORDER_POLICY}_test.log \
                     imdbload \
                     --checkpoint_dir {CHECKPOINT_PATH}
```

### Parameters and Models for E2

| Split Philosophy         | Query Order Policy     | `{SPLIT_PHILOSOPHY}` | `{ORDER_POLICY}`   | Hugging Face Model Path                                                                                                          |
| :----------------------- | :--------------------- | :------------------- | :----------------- | :------------------------------------------------------------------------------------------------------------------------------- |
| **Random Split**         | Ascending Latency      | `random`             | `asc_latency`      | `experiment2/random_split/BAO/asc_latency/final_model/20250519_160929_bao_default_model` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/random_split/BAO/asc_latency/final_model/20250519_160929_bao_default_model)) |
|                          | Ascending Complexity   | `random`             | `asc_complexity`   | `experiment2/random_split/BAO/asc_complexity/final_model/20250908_172042_bao_default_model` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/random_split/BAO/asc_complexity/final_model/20250908_172042_bao_default_model)) |
|                          | Random                 | `random`             | `random`           | `experiment2/random_split/BAO/random/final_model/20250520_141757_bao_default_model` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/random_split/BAO/random/final_model/20250520_141757_bao_default_model)) |
| **Base Query Split**     | Ascending Latency      | `base_query`         | `asc_latency`      | `experiment2/base_query/BAO/asc_latency/final_model/20250515_101747_bao_default_model` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/base_query/BAO/asc_latency/final_model/20250515_101747_bao_default_model)) |
|                          | Ascending Complexity   | `base_query`         | `asc_complexity`   | `experiment2/base_query/BAO/asc_complexity/final_model/20250515_121137_bao_default_model` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/base_query/BAO/asc_complexity/final_model/20250515_121137_bao_default_model)) |
|                          | Random                 | `base_query`         | `random`           | `experiment2/base_query/BAO/random/final_model/20250520_163329_bao_default_model` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/base_query/BAO/random/final_model/20250520_163329_bao_default_model)) |
| **Leave-One-Out Split**  | Ascending Latency      | `leave_one_out`      | `asc_latency`      | `experiment2/leave_one_out/BAO/asc_latency/final_model/20250519_142044_bao_default_model` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/leave_one_out/BAO/asc_latency/final_model/20250519_142044_bao_default_model)) |
|                          | Ascending Complexity   | `leave_one_out`      | `asc_complexity`   | `experiment2/leave_one_out/BAO/asc_complexity/final_model/20250515_231428_bao_default_model` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/leave_one_out/BAO/asc_complexity/final_model/20250515_231428_bao_default_model)) |
|                          | Random                 | `leave_one_out`      | `random`           | `experiment2/leave_one_out/BAO/random/final_model/20250520_202959_bao_default_model` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/leave_one_out/BAO/random/final_model/20250520_202959_bao_default_model)) |

---

## (E3) Learning Trajectory & Convergence

This experiment analyzes BAO's learning process by evaluating model checkpoints saved at different intervals during a single, extended training run. All runs for this experiment use the **JOB workload**.

> **Note:** This experiment uses dedicated scripts: `train_bao_experiment3.py` and `test_bao_experiment3.py`.

### Training

Follow this **3-step process**:
1.  **Perform State Cleanup:** As described in the "Pre-Run Workflow" section, clear any previous BAO state.
2.  **Start BAO Server:** This script requires a manually started server. In a separate terminal, run:
    ```bash
    cd bao_server && python3 main.py
    ```
3.  **Run Training:** Execute the script below. It will perform a long training run, saving checkpoints according to three policies into subdirectories within your target path.

    ```bash
    python3 train_bao_experiment3.py \
        --query_dir ../../experiments/experiment3/job/train/ \
        --output_file ../../experiments/experiment3/job/results/bao_learning_trajectory.log \
        --checkpoint_dir_base ../../models/experiment3/BAO/
    ```

After running, the `--checkpoint_dir_base` will contain the following subdirectories:
*   `epoch_checkpoints/`: Models saved at regular epoch intervals.
*   `loss_checkpoints/`: Models saved when the validation loss improves.
*   `query_checkpoints/`: Models saved at regular query count intervals.

### Testing & Analysis

The `test_bao_experiment3.py` script automatically manages the server and is designed to evaluate **all checkpoints** for all three policies in one go. You only need to provide the **base directory** where the checkpoints were saved.

**Testing Command:**
```bash
python3 test_bao_experiment3.py \
    ../../models/experiment3/BAO/ \
    ../../experiments/experiment3/job/test/ \
    imdbload
```
*   The first argument is the `{CHECKPOINT_PATH_BASE}`.
*   The second is the path to the test queries.
*   The third is the database name.

**Setting the `{CHECKPOINT_PATH_BASE}`:**

*   **Option A (Your Trained Model):** Point to the base output directory from your training run (e.g., `../../models/experiment3/BAO/`). The script will recursively find and evaluate all models.

*   **Option B (Pre-trained Models):** Download our pre-computed checkpoints from Hugging Face and point to the local base directory that contains the `epoch_checkpoints`, `loss_checkpoints`, etc. subfolders.

**Model Checkpoints for E3:**

| Workload | Model Path on Hugging Face | Link to Directory |
| :------- | :------------------------- | :---------------- |
| **JOB**  | `experiment3/BAO/`         | [View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment3/BAO) |

---

## (E4) Internal Decision-Making

This experiment is divided into sub-sections, each analyzing a different aspect of BAO's decision-making process.

### 4.1 Access Path Selection

This sub-experiment tests how a BAO model reacts to different access paths.

#### Scenario A: JOB-Synthetic Workload

This scenario uses a synthetic workload designed to test access path selection against the **standard `imdbload` database**.

**Training:**
1.  **Perform State Cleanup.**
2.  **Start BAO Server** in a separate terminal.
3.  **Run Training:**
    ```bash
    python3 train.py \
        --query_dir ../../experiments/experiment4/4.1/synthetic/ \
        --output_file ../../experiments/experiment4/4.1/results/bao_synthetic_train.log \
        --target_checkpoint_dir ../../models/experiment4/4.1/job_synthetic/BAO/
    ```

**Testing:**
The `test_run.py` script manages the server automatically.
```bash
python3 test_run.py ../../experiments/experiment4/4.1/synthetic/ \
                     ../../experiments/experiment4/4.1/results/bao_synthetic_test.log \
                     imdbload \
                     --checkpoint_dir {CHECKPOINT_PATH}
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
1.  **Perform State Cleanup.**
2.  **Start BAO Server** in a separate terminal.
3.  **Run Training:**
    ```bash
    python3 train.py \
        --query_dir ../../experiments/experiment4/4.1/added_index/ \
        --output_file ../../experiments/experiment4/4.1/results/bao_added_index_train.log \
        --target_checkpoint_dir ../../models/experiment4/4.1/job_added_index/BAO/
    ```

**Testing:**
The `test_run.py` script manages the server automatically.
```bash
python3 test_run.py ../../experiments/experiment4/4.1/added_index/ \
                     ../../experiments/experiment4/4.1/results/bao_added_index_test.log \
                     imdbload_added_index \
                     --checkpoint_dir {CHECKPOINT_PATH}
```

#### Model Checkpoints for 4.1

For the `{CHECKPOINT_PATH}` in the testing commands, you can use your own trained models or our pre-trained models from Hugging Face.

| Scenario                | Hugging Face Model Path                  | Link to Directory |
| :---------------------- | :--------------------------------------- | :---------------- |
| **JOB-Synthetic**       | -     | - |
| **JOB with Added Index**| `experiment4/4.1/job_added_index/BAO/20250424_085727_bao_default_model`   | [View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment4/4.1/job_added_index/BAO/20250424_085727_bao_default_model) |

### 4.2 Physical Operator Composition

This sub-experiment evaluates BAO's ability to compose different physical operators. It uses the standard `imdbload` database.

**Testing:**
The `test_run.py` script manages the server automatically. For this test, it's recommended to reuse the robust model trained in Experiment 1.
```bash
python3 test_run.py ../../experiments/experiment4/4.2/ \
                     ../../experiments/experiment4/4.2/results/bao_test.log \
                     imdbload \
                     --checkpoint_dir {CHECKPOINT_PATH_E1_MODEL}
```
*   **For `{CHECKPOINT_PATH_E1_MODEL}`:** Use the path to the pre-trained model for JOB from **Experiment 1**.

### 4.3 Join Operator Prediction Accuracy

This sub-experiment uses a dedicated script (`test_exp_4_3.py`) to evaluate a pre-trained model when the database is restricted to a single join type.

**Prerequisites for testing:** Before starting the server, go to `bao_server/main.py` lines 61-66, and uncomment the idx that corresponds to the operator you want to use. 
```python
# Force index 3 which is the hash join plan
# idx = 3
# Force index 20 which is the nested loop plan
# idx = 20
# Force index 26 which is the merge join plan
# idx = 26
```

**Testing Command:**
The script manages the server automatically and tests the **pre-trained E1 model**.
```bash
python3 test_exp_4_3.py ../../experiments/experiment4/4.3/{join_operator}/ \
                      ../../experiments/experiment4/4.3/results/bao_{join_operator}_test.log \
                      imdbload \
                      --checkpoint_dir {CHECKPOINT_PATH_E1_MODEL}
```
*   **For `{join_operator}`:** Use one of `hashjoin`, `mergejoin`, or `nestloop`.
*   **For `{CHECKPOINT_PATH_E1_MODEL}`:** Use the path to the pre-trained model for JOB from **Experiment 1**.

### 4.4 Embedding Similarity and Plan Quality.

This sub-experiment analyzes the relationship between NEO's learned query embeddings and final plan quality.

**Note:** This experiment uses the dedicated test_run_embeddings.py script for analysis.

**Testing:**
The `test_run.py` script manages the server automatically. It's recommended to reuse the E1 model for this analysis.
```bash
python3 test_run_embeddings.py ../../experiments/experiment4/4.4/run1/ \
                     ../../experiments/experiment4/4.4/results/bao_test.log \
                     imdbload \
                     --checkpoint_dir {CHECKPOINT_PATH_E1_MODEL}
```
*   **For `{CHECKPOINT_PATH_E1_MODEL}`:** Use the path to the pre-trained model for JOB from **Experiment 1**.

## (E5) Generalization to Novel Conditions

This experiment assesses BAO's ability to generalize to queries and database conditions not seen during training.

### 5.1 Generalization to New Queries

**Scenario 1: Distribution Generalization**
This scenario tests how a single model, trained on a standard workload, generalizes to four different, unseen query distributions.

*   **Training:** Follow the training steps for **Experiment 1 (JOB workload)** to produce the base model.
*   **Testing:** Use the `test_run.py` script to test the E1 model against each new workload.
    **Testing Template:**
    ```bash
    python3 test_run.py ../../experiments/experiment5/5.1/distribution_generalization/{test_workload_name}/ \
                         ../../experiments/experiment5/5.1/distribution_generalization/results/bao_{test_workload_name}.log \
                         imdbload \
                         --checkpoint_dir {CHECKPOINT_PATH_E1_MODEL}
    ```
    **Parameters:**
    | Test Workload     | `{test_workload_name}` |
    | :---------------- | :--------------------- |
    | **JOB-Dynamic**   | `job_d`                |
    | **JOB-Extended**  | `job_extended`         |
    | **JOB-Light**     | `job_light`            |
    | **JOB-Synthetic** | `job_synthetic`        |

**Scenarios 2 & 3: Complexity and Selectivity Generalization**
These scenarios follow a standard train/test structure.

*   **Training Template:**
    ```bash
    # 1. Cleanup, 2. Start Server
    python3 train.py \
        --query_dir ../../experiments/experiment5/5.1/{generalization_dimension}/train/ \
        --output_file ../../experiments/experiment5/5.1/{generalization_dimension}/results/bao_train.log \
        --checkpoint_dir ../../models/experiment5/5.1/{generalization_dimension}/BAO/
    ```
*   **Testing Template:**
    ```bash
    python3 test_run.py ../../experiments/experiment5/5.1/{generalization_dimension}/test/ \
                         ../../experiments/experiment5/5.1/{generalization_dimension}/results/bao_test.log \
                         imdbload \
                         --checkpoint_dir {CHECKPOINT_PATH}
    ```
    **Parameters:**
    | Scenario                   | `{generalization_dimension}` |
    | :------------------------- | :--------------------------- |
    | **Complexity Generalization**| `complexity_generalization`  |
    | **Selectivity Generalization**| `selectivity_generalization` |

**Model Checkpoints for 5.1:**

| Scenario                     | Hugging Face Model Path                       |
| :--------------------------- | :---------------------------------------------- |
| **Distribution Generalization**| Reuse the **Experiment 1 JOB** model. [(View Files)](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment1/JOB/BAO/20250522_095820_bao_default_model)          |
| **Complexity Generalization**  | `experiment5/5.1/complexity_generalization/BAO/final_model/20250610_151348_bao_default_model` [(View Files)](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment5/5.1/complexity_generalization/BAO/final_model/20250610_151348_bao_default_model) |
| **Selectivity Generalization** | `experiment5/5.1/selectivity_generalization/BAO/final_model/20250720_131905_bao_default_model` [(View Files)](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment5/5.1/selectivity_generalization/BAO/final_model/20250720_131905_bao_default_model) |

### 5.2 Adaptability to New Schemas

This sub-experiment tests BAO's ability to adapt to schema changes.

**Command Templates:**
*   **Training Template:**
    ```bash
    # 1. Cleanup, 2. Start Server
    python3 train.py \
        --query_dir ../../experiments/experiment5/5.2/{SCHEMA_TYPE}/train/ \
        --output_file ../../experiments/experiment5/5.2/{SCHEMA_TYPE}/results/bao_train.log \
        --target_checkpoint_dir ../../models/experiment5/5.2/{SCHEMA_TYPE}/BAO/
    ```
*   **Testing Template:**
    ```bash
    python3 test_run.py ../../experiments/experiment5/5.2/{SCHEMA_TYPE}/test/ \
                         ../../experiments/experiment5/5.2/{SCHEMA_TYPE}/results/bao_test.log \
                         {DB_NAME} \
                         --checkpoint_dir {CHECKPOINT_PATH}
    ```

**Scenarios, Parameters, and Models for 5.2:**

| Scenario                     | Prerequisites                                   | `{SCHEMA_TYPE}` | `{DB_NAME}` | Hugging Face Model Path        |
| :--------------------------- | :---------------------------------------------- | :-------------- | :---------- | :----------------------------- |
| **Workload-Specific Schema Shift** | Uses standard `imdbload` DB.                  | `job`           | `imdbload`  | `experiment5/5.2/job/BAO/final_model/20250617_090407_bao_default_model` [(View Files)](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment5/5.2/job/BAO/final_model/20250617_090407_bao_default_model)    |
| **Structural Schema Shift**    | Must connect to the **SSB database**. | `ssb`           | `ssb`       | `experiment5/5.2/ssb/BAO/final_model/20250720_092936_bao_default_model` [(View Files)](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment5/5.2/ssb/BAO/final_model/20250720_092936_bao_default_model)   |

### 5.3 Adaptation to Distribution Shifts.

(Guide for 5.3 will be included once we have uploaded the datasets to huggingface)