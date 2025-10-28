# FASTgres Experiment Reproduction Guide

This document provides the specific commands required to replicate the experiments for the FASTgres optimizer.

### Pre-Run Workflow: Critical Instructions

**1. Configure Database Connection:**
The scripts use a `.env` file in the repository root to manage database credentials. Before running, create a `.env` file in the top-level project directory and populate it with your connection details.

**2. Data Preparation Pipeline:**
FASTgres requires several data artifacts to be generated before training can begin. This pipeline must be run once for each new workload (e.g., JOB, TPC-H, SSB).

*   **Step A: Generate Labels (`generate_labels.py`)**
    This script executes every query with every possible hint set to find the optimal one, creating a labeled "archive" file. **This step is extremely time-consuming** (hours or even days).
    ```bash
    python3 generate_labels.py <path/to/workload/> -o <path/to/save/archive.json> -db <db_name>
    ```

*   **Step B: Build Database Info (`update_db_info.py`)**
    This script gathers statistics from the database schema and workload, which are needed for featurization.
    ```bash
    python3 update_db_info.py <db_name> -mm db_info/{db_name}/ -l db_info/{db_name}/ -w db_info/{db_name}/ -q <path/to/workload/>
    ```

*   **Step C: Build Query Objects (`build_query_objects.py`)**
    This optional but highly recommended step pre-computes query representations to speed up the main training process.
    ```bash
    python3 build_query_objects.py <path/to/workload/> -sp <path/to/save/objects.pkl>
    ```
---

## (E1) End-to-End Performance & Value Model Fidelity

### Data Preparation
Run the 3-step Data Preparation Pipeline for each workload. For example, to prepare the **JOB** workload:
1.  **Generate Labels:** `python3 generate_labels.py ../../workloads/imdb_pg_dataset/job/ -o ../../experiments/experiment1/job/labels/job_archive.json -db imdb`
2.  **Build DB Info:** `python3 update_db_info.py imdb -mm db_info/imdb/ -l db_info/imdb/ -w db_info/imdb/ -q ../../workloads/imdb_pg_dataset/job/`
3.  **Build Query Objects:** `python3 build_query_objects.py ../../workloads/imdb_pg_dataset/job/ -sp ../../experiments/experiment1/job/labels/job_query_objects.pkl`

### Training
This script trains a new model and evaluates it based on a pre-defined train/test split.
```bash
python3 evaluate_queries.py ../../workloads/imdb_pg_dataset/{WORKLOAD_LOWER}/ \
    -db {DB_NAME} \
    -a ../../experiments/experiment1/{WORKLOAD_LOWER}/labels/{WORKLOAD_LOWER}_archive.json \
    -dbip db_info/{DB_NAME}/ \
    -qo ../../experiments/experiment1/{WORKLOAD_LOWER}/labels/{WORKLOAD_LOWER}_query_objects.pkl \
    -sd ../../experiments/experiment1/{WORKLOAD_LOWER}/results/ \
    -tcd {CHECKPOINT_PATH}
```

### Testing
This script loads a pre-trained model and runs it on a workload.
```bash
python3 test_fastgres.py ../../experiments/experiment1/{WORKLOAD_LOWER}/test/ \
    -D {DB_NAME} \
    --checkpoint_dir {CHECKPOINT_PATH} \
```

### Parameters and Models for E1
| Workload | `{WORKLOAD_LOWER}` | `{DB_NAME}` | Hugging Face Model for `{CHECKPOINT_PATH}` | Link to Directory |
| :------- | :----------------- | :---------- | :--------------------------------------- | :---------------- |
| **JOB**  | `job`              | `imdb`  | `experiment1/JOB/FASTgres/`              | [View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment1/JOB/FASTgres) |
| **TPC-H**| `tpch`             | `tpch`      | `experiment1/TPCH/FASTgres/`             | [View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment1/TPCH/FASTgres) |
| **TPC-DS**| `tpcds`            | `tpcds`     | `experiment1/TPCDS/FASTgres/`            | [View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment1/TPCDS/FASTgres) |

---

## (E2) Sensitivity & Execution Stability

This experiment investigates how FASTgres's performance is affected by different training data splits and query ordering policies. All runs for this experiment use the **JOB workload**.

### Data Preparation for E2

The following data preparation steps need to be performed only **once** for this entire experiment. The generated artifacts (`job_archive.json`, `job_query_objects.pkl`, and the DB info files) will be reused for all training scenarios below.

1.  **Generate Labels:**
    ```bash
    python3 generate_labels.py ../../workloads/imdb_pg_dataset/job/ -o ../../experiments/experiment1/job/labels/job_archive.json -db imdb
    ```
2.  **Build Database Info:**
    ```bash
    python3 update_db_info.py imdb -mm db_info/imdb/ -l db_info/imdb/ -w db_info/imdb/ -q ../../workloads/imdb_pg_dataset/job/
    ```
3.  **Build Query Objects:**
    ```bash
    python3 build_query_objects.py ../../workloads/imdb_pg_dataset/job/ -sp ../../experiments/experiment1/job/labels/job_query_objects.pkl
    ```

### Training and Testing Scenarios

After completing the one-time data preparation, you can run any of the following training and testing scenarios. Choose a scenario from the table and substitute the corresponding values into the `{placeholders}` in the templates.

**Training Template:**
```bash
python3 evaluate_queries.py ../../workloads/imdb_pg_dataset/job/ \
    -db imdb \
    -a ../../experiments/experiment1/job/labels/job_archive.json \
    -dbip db_info/imdb/ \
    -qo ../../experiments/experiment1/job/labels/job_query_objects.pkl \
    -sd ../../experiments/experiment2/job/results/ \
    -fo ../../experiments/experiment2/job/train/{SPLIT_PHILOSOPHY}/{ORDER_POLICY}.txt \
    -tcd ../../models/experiment2/job/{SPLIT_PHILOSOPHY}/FASTgres/{ORDER_POLICY}/
```

**Testing Template:**
```bash
python3 test_fastgres.py ../../experiments/experiment2/job/test/{SPLIT_PHILOSOPHY}/{ORDER_POLICY}/ \
    -D imdb \
    --checkpoint_dir {CHECKPOINT_PATH} \
    -o ../../experiments/experiment2/job/results/fastgres_{SPLIT_PHILOSOPHY}_{ORDER_POLICY}_test.log
```

### Parameters and Models for E2

| Split Philosophy | Query Order Policy | `{SPLIT_PHILOSOPHY}` | `{ORDER_POLICY}` | Hugging Face Model for `{CHECKPOINT_PATH}` |
| :--------------- | :----------------- | :------------------- | :--------------- | :--------------------------------------- |
| **Random Split** | Ascending Latency  | `random`             | `asc_latency`    | `experiment2/random_split/FASTgres/asc_latency/checkpoints/` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/random_split/FASTgres/asc_complexity/checkpoints/)) |
|                          | Ascending Complexity   | `random`             | `asc_complexity`   | `experiment2/random_split/FASTgres/asc_complexity/checkpoints/` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/random_split/FASTgres/asc_complexity/checkpoints/)) |
|                          | Random                 | `random`             | `random`           | `experiment2/random_split/FASTgres/random/checkpoints/` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/random_split/FASTgres/random/checkpoints/)) |
| **Base Query Split**     | Ascending Latency      | `base_query`         | `asc_latency`      | `experiment2/base_query/FASTgres/asc_latency/checkpoints/` ([View Files](experiment2/base_query/FASTgres/asc_latency/checkpoints/)) |
|                          | Ascending Complexity   | `base_query`         | `asc_complexity`   | `experiment2/base_query/FASTgres/asc_complexity/checkpoints/` ([View Files](experiment2/base_query/FASTgres/asc_complexity/checkpoints/)) |
|                          | Random                 | `base_query`         | `random`           | `experiment2/base_query/FASTgres/random/checkpoints/` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/base_query/FASTgres/random/checkpoints/)) |
| **Leave-One-Out Split**  | Ascending Latency      | `leave_one_out`      | `asc_latency`      | `experiment2/leave_one_out/FASTgres/asc_latency/checkpoints/` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/leave_one_out/FASTgres/asc_latency/checkpoints/)) |
|                          | Ascending Complexity   | `leave_one_out`      | `asc_complexity`   | `experiment2/leave_one_out/FASTgres/asc_complexity/checkpoints/` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/leave_one_out/FASTgres/asc_complexity/checkpoints/)) |
|                          | Random                 | `leave_one_out`      | `random`           | `experiment2/leave_one_out/FASTgres/random/checkpoints/` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/leave_one_out/FASTgres/random/checkpoints/)) |

---

## (E3, E4.3, E4.4) Inapplicable Experiments

Due to its supervised, offline training methodology, FASTgres cannot participate in the following experiments:
*   **(E3) Learning Trajectory**
*   **(E4.3) Join Operator Accuracy**
*   **(E4.4) Embedding Similarity**

---

## (E4) Internal Decision-Making

### 4.1 Access Path Selection

This sub-experiment tests how FASTgres adapts to different access paths, using both a synthetic workload and a schema change (an added index).

#### Scenario A: JOB-Synthetic Workload

This scenario uses a synthetic workload designed to test access path selection against the **standard `imdb` database**.

**Data Preparation:**
1.  **Generate Labels:**
    ```bash
    python3 generate_labels.py ../../experiments/experiment4/4.1/synthetic/ -o ../../experiments/experiment4/4.1/synthetic/labels/archive.json -db imdb
    ```
2.  **Build Database Info:**
    ```bash
    python3 update_db_info.py imdb -mm db_info/imdb/ -l db_info/imdb/ -w db_info/imdb/ -q ../../experiments/experiment4/4.1/synthetic/
    ```
3.  **Build Query Objects:**
    ```bash
    python3 build_query_objects.py ../../experiments/experiment4/4.1/synthetic/ -sp ../../experiments/experiment4/4.1/synthetic/labels/objects.pkl
    ```

**Training:**
```bash
python3 evaluate_queries.py ../../experiments/experiment4/4.1/synthetic/ \
    -db imdb \
    -a ../../experiments/experiment4/4.1/synthetic/labels/archive.json \
    -dbip db_info/imdb/ \
    -qo ../../experiments/experiment4/4.1/synthetic/labels/objects.pkl \
    -sd ../../experiments/experiment4/4.1/synthetic/results/ \
    -tcd ../../models/experiment4/4.1/job_synthetic/FASTgres/
```

**Testing:**
```bash
python3 test_fastgres.py ../../experiments/experiment4/4.1/synthetic/ \
    -D imdb \
    --checkpoint_dir {CHECKPOINT_PATH} \
    -o ../../experiments/experiment4/4.1/synthetic/results/test.log
```

#### Scenario B: JOB Workload with Added Index

This scenario uses the standard JOB workload against a modified database containing a new index.

**Prerequisites:** This scenario requires a modified database schema.
1.  Connect to your PostgreSQL instance and clone the `imdb` database:
    ```sql
    CREATE DATABASE imdb_added_index WITH TEMPLATE imdb;
    ```
2.  Connect to the new database and add an index:
    ```sql
    \c imdb_added_index
    CREATE INDEX idx_title_production_year ON title(production_year);
    ```

**Data Preparation:**
1.  **Generate Labels:**
    ```bash
    python3 generate_labels.py ../../experiments/experiment4/4.1/added_index/ -o ../../experiments/experiment4/4.1/added_index/labels/archive.json -db imdb_added_index
    ```
2.  **Build Database Info:**
    ```bash
    python3 update_db_info.py imdb_added_index -mm db_info/imdb_added_index/ -l db_info/imdb_added_index/ -w db_info/imdb_added_index/ -q ../../experiments/experiment4/4.1/added_index/
    ```
3.  **Build Query Objects:**
    ```bash
    python3 build_query_objects.py ../../experiments/experiment4/4.1/added_index/ -sp ../../experiments/experiment4/4.1/added_index/labels/objects.pkl
    ```

**Training:**
```bash
python3 evaluate_queries.py ../../experiments/experiment4/4.1/added_index/ \
    -db imdb_added_index \
    -a ../../experiments/experiment4/4.1/added_index/labels/archive.json \
    -dbip db_info/imdb_added_index/ \
    -qo ../../experiments/experiment4/4.1/added_index/labels/objects.pkl \
    -sd ../../experiments/experiment4/4.1/added_index/results/ \
    -tcd ../../models/experiment4/4.1/job_added_index/FASTgres/
```

**Testing:**
```bash
python3 test_fastgres.py ../../experiments/experiment4/4.1/added_index/ \
    -D imdb_added_index \
    --checkpoint_dir {CHECKPOINT_PATH} \
    -o ../../experiments/experiment4/4.1/added_index/results/test.log
```

#### Model Checkpoints for 4.1

For the `{CHECKPOINT_PATH}` in the testing commands, you can use your own trained models or our pre-trained models from Hugging Face.

| Scenario                | Hugging Face Model Path                  | Link to Directory |
| :---------------------- | :--------------------------------------- | :---------------- |
| **JOB-Synthetic**       | -                                         | - |
| **JOB with Added Index**| `experiment4/4.1/job_added_index/FASTgres/`| [View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment4/4.1/job_added_index/FASTgres) |


### 4.2 Physical Operator Composition

This is an inference-only test using the **pre-trained E1 model** for JOB.
```bash
python3 test_fastgres.py ../../experiments/experiment4/4.2/ \
    -D imdb \
    --checkpoint_dir {CHECKPOINT_PATH_E1_MODEL}
```

---

## (E5) Generalization to Novel Conditions

This experiment assesses FASTgres's ability to generalize to queries and database conditions not seen during training.

### 5.1 Generalization to New Queries

#### Scenario 1: Distribution Generalization

This is an inference-only test using the **pre-trained E1 model** for the JOB workload against four new, unseen query distributions.

**Testing Template:**
```bash
python3 test_fastgres.py ../../experiments/experiment5/5.1/distribution_generalization/{test_workload_name}/ \
    -D imdb \
    --checkpoint_dir {CHECKPOINT_PATH_E1_MODEL}
```

**Parameters:**
Use the testing template above, substituting `{test_workload_name}` with one of the following values:

| Test Workload       | `{test_workload_name}` |
| :------------------ | :--------------------- |
| **JOB-Dynamic**     | `job_d`                |
| **JOB-Extended**    | `job_extended`         |
| **JOB-Light**       | `job_light`            |
| **JOB-Synthetic**   | `job_synthetic`        |

#### Scenarios 2 & 3: Complexity and Selectivity Generalization

These scenarios require a full data preparation and training cycle on their specific, pre-defined data splits.

**Data Preparation Template:**
Substitute `{generalization_dimension}` with either `complexity_generalization` or `selectivity_generalization`.
1.  **Generate Labels:**
    ```bash
    python3 generate_labels.py ../../experiments/experiment5/5.1/{generalization_dimension}/ -o ../../experiments/experiment5/5.1/{generalization_dimension}/labels/archive.json -db imdb
    ```
2.  **Build Database Info:**
    ```bash
    python3 update_db_info.py imdb -mm db_info/imdb/ -l db_info/imdb/ -w db_info/imdb/ -q ../../experiments/experiment5/5.1/{generalization_dimension}/
    ```
3.  **Build Query Objects:**
    ```bash
    python3 build_query_objects.py ../../experiments/experiment5/5.1/{generalization_dimension}/ -sp ../../experiments/experiment5/5.1/{generalization_dimension}/labels/objects.pkl
    ```

**Training Template:**
```bash
python3 evaluate_queries.py ../../experiments/experiment5/5.1/{generalization_dimension}/ \
    -db imdb \
    -a ../../experiments/experiment5/5.1/{generalization_dimension}/labels/archive.json \
    -dbip db_info/imdb/ \
    -qo ../../experiments/experiment5/5.1/{generalization_dimension}/labels/objects.pkl \
    -sd ../../experiments/experiment5/5.1/{generalization_dimension}/results/ \
    -tcd ../../models/experiment5/5.1/{generalization_dimension}/FASTgres/
```

**Testing Template:**
```bash
python3 test_fastgres.py ../../experiments/experiment5/5.1/{generalization_dimension}/test/ \
    -D imdb \
    --checkpoint_dir {CHECKPOINT_PATH}
```

**Model Checkpoints for 5.1:**

| Scenario                     | Hugging Face Model Path                       | Link to Directory |
| :--------------------------- | :---------------------------------------------- | :---------------- |
| **Complexity Generalization**  | `experiment5/5.1/complexity_generalization/FASTgres/checkpoints/` | [View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment5/5.1/complexity_generalization/FASTgres/checkpoints) |
| **Selectivity Generalization** | `experiment5/5.1/selectivity_generalization/FASTgres/checkpoints/`| [View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment5/5.1/selectivity_generalization/FASTgres/checkpoints) |

### 5.2 Adaptability to New Schemas

This sub-experiment tests FASTgres's ability to adapt to schema changes, covering both workload-specific and structural shifts.

#### Scenario A: Workload-Specific Schema Shift (JOB)

This scenario trains and tests FASTgres on a specific split of the JOB workload designed to test schema adaptation. It uses the standard `imdb` database.

**Data Preparation:**
1.  **Generate Labels:**
    ```bash
    python3 generate_labels.py ../../experiments/experiment5/5.2/job/ -o ../../experiments/experiment5/5.2/job/labels/archive.json -db imdb
    ```
2.  **Build Database Info:**
    ```bash
    python3 update_db_info.py imdb -mm db_info/imdb/ -l db_info/imdb/ -w db_info/imdb/ -q ../../experiments/experiment5/5.2/job/
    ```
3.  **Build Query Objects:**
    ```bash
    python3 build_query_objects.py ../../experiments/experiment5/5.2/job/ -sp ../../experiments/experiment5/5.2/job/labels/objects.pkl
    ```

**Training:**
```bash
python3 evaluate_queries.py ../../experiments/experiment5/5.2/job/ \
    -db imdb \
    -a ../../experiments/experiment5/5.2/job/labels/archive.json \
    -dbip db_info/imdb/ \
    -qo ../../experiments/experiment5/5.2/job/labels/objects.pkl \
    -sd ../../experiments/experiment5/5.2/job/results/ \
    -tcd ../../models/experiment5/5.2/job/FASTgres/
```

**Testing:**
```bash
python3 test_fastgres.py ../../experiments/experiment5/5.2/job/test/ \
    -D imdb \
    --checkpoint_dir {CHECKPOINT_PATH}
```

#### Scenario B: Structural Schema Shift (SSB)

This scenario requires a full data preparation and training cycle for the completely different **SSB workload**.

**Data Preparation:**
1.  **Generate Labels:**
    ```bash
    python3 generate_labels.py ../../workloads/ssb/ -o ../../experiments/experiment5/5.2/ssb/labels/ssb_archive.json -db ssb
    ```
2.  **Build Database Info:**
    ```bash
    python3 update_db_info.py ssb -mm db_info/ssb/ -l db_info/ssb/ -w db_info/ssb/ -q ../../workloads/ssb/
    ```
3.  **Build Query Objects:**
    ```bash
    python3 build_query_objects.py ../../workloads/ssb/ -sp ../../experiments/experiment5/5.2/ssb/labels/ssb_query_objects.pkl
    ```

**Training:**
```bash
python3 evaluate_queries.py ../../workloads/ssb/ \
    -db ssb \
    -a ../../experiments/experiment5/5.2/ssb/labels/ssb_archive.json \
    -dbip db_info/ssb/ \
    -qo ../../experiments/experiment5/5.2/ssb/labels/ssb_query_objects.pkl \
    -sd ../../experiments/experiment5/5.2/ssb/results/ \
    -tcd ../../models/experiment5/5.2/ssb/FASTgres/
```

**Testing:**
```bash
python3 test_fastgres.py ../../experiments/experiment5/5.2/ssb/test/ \
    -D ssb \
    --checkpoint_dir {CHECKPOINT_PATH}
```

#### Model Checkpoints for 5.2

For the `{CHECKPOINT_PATH}` in the testing commands, you can use your own trained models or our pre-trained models from Hugging Face.

| Scenario                       | Hugging Face Model Path      | Link to Directory |
| :----------------------------- | :--------------------------- | :---------------- |
| **Workload-Specific (JOB)**    | `experiment5/5.2/job/FASTgres/checkpoints/`| [View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment5/5.2/job/FASTgres/checkpoints) |
| **Structural (SSB)**           | `experiment5/5.2/ssb/FASTgres/checkpoints/`| [View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment5/5.2/ssb/FASTgres/checkpoints) |

### 5.3 Adaptation to Distribution Shifts.

(Guide for 5.3 will be included once we have uploaded the datasets to huggingface)