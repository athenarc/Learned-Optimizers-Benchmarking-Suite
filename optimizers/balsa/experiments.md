# NEO/Balsa Experiment Reproduction Guide

This document provides the specific commands required to replicate the experiments presented in our paper using the NEO/Balsa optimizer.

### Configure Database Connection
The scripts use a `.env` file in the repository root to manage database credentials. Before running any command, create a file named `.env` in the top-level directory of this project and populate it with your connection details. You can use the provided `.env.example` as a template.

### Pre-Run: Completing the DB Connection

**Important**: Before running any script, in order to finalize connecting to the database, you must edit two files to select the target database:

1.  **Set Database Connection:** In `pg_executor/pg_executor/pg_executor.py`, uncomment the `DSN` line for your target database (e.g., `imdbload` for JOB).
2.  **Set Join Graph:** In `balsa/experience.py`, uncomment the `WithJoinGraph` line that matches your workload.
3.  **Apply Changes:** If you modified `pg_executor.py`, re-apply with `pip install -e pg_executor`.

---

## (E1) End-to-End Performance & Value Model Fidelity

This experiment evaluates the baseline performance of NEO across standard benchmarks.

### Command Templates for E1

To run an experiment, choose a workload from the table below and substitute the corresponding values into the `{placeholders}` in these templates.

**Training Template:**
```bash
python run.py --run {EXP_CLASS} --local \
    --workload_dir ../../experiments/experiment1/{WORKLOAD_LOWER}/train/ \
    --test_workload_dir ../../experiments/experiment1/{WORKLOAD_LOWER}/test/ \
    --target_checkpoint_dir ../../models/experiment1/{WORKLOAD_UPPER}/NEO/
```

**Testing Template:**
```bash
python test_model.py --run {EXP_CLASS} \
    --checkpoint_dir {CHECKPOINT_PATH} \
    --workload_dir ../../experiments/experiment1/{WORKLOAD_LOWER}/train/ \
    --test_workload_dir ../../experiments/experiment1/{WORKLOAD_LOWER}/test/
```

### Parameters and Models for E1
| Workload | `{EXP_CLASS}`                | `{WORKLOAD_LOWER}` | `{WORKLOAD_UPPER}` | Hugging Face Model for Testing                                                                                                                              |
| :------- | :--------------------------- | :----------------- | :----------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **JOB**  | `Neo_JOB_EvaluationBase`     | `job`              | `JOB`              | `experiment1/JOB/NEO/epoch49` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment1/JOB/NEO/epoch49))         |
| **TPC-H**| `Neo_TPC_H_EvaluationBase`   | `tpch`             | `TPCH`             | `experiment1/TPCH/NEO/checkpoints/epoch49` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment1/TPCH/NEO/checkpoints/epoch49)) |
| **TPC-DS**| `Neo_TPC_DS_EvaluationBase`  | `tpcds`            | `TPCDS`            | `experiment1/TPCDS/NEO/checkpoints/epoch49` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment1/TPCDS/NEO/checkpoints/epoch49)) |


### Testing Worklflow

For the `{CHECKPOINT_PATH}` in the Testing Template, you have two options:
-  **Option A (Your Trained Model):** Point to the output directory from your training run (e.g., `../../models/experiment1/JOB/NEO/epoch49/`).
-  **Option B (Pre-trained Model)**: Download the model from Hugging Face and point to its local path (e.g., `<path/to/models>/experiment1/JOB/NEO/epoch49/`).

---

## (E2) Sensitivity & Execution Stability

This experiment investigates how NEO's performance is affected by different training data splits and query ordering policies. All runs for this experiment use the JOB workload.

### Command Templates for E2

To run an experiment, choose a **Split Philosophy** and a **Query Order Policy** from the table below and substitute the corresponding values into the `{placeholders}` in these templates.

**Training Template:**
```bash
python run.py --run {EXP_CLASS} --local \
    --workload_dir ../../experiments/experiment2/train/{SPLIT_PHILOSOPHY}/ \
    --workload_order ../../experiments/experiment2/train/{SPLIT_PHILOSOPHY}/{ORDER_POLICY}.txt
    --test_workload_dir ../../experiments/experiment2/test/{SPLIT_PHILOSOPHY}/{ORDER_POLICY}/ \
    --target_checkpoint_dir ../../models/experiment2/{SPLIT_PHILOSOPHY}/NEO/{ORDER_POLICY}/
```

**Testing Template:**
```bash
python test_model.py --run {EXP_CLASS} \
    --checkpoint_dir {CHECKPOINT_PATH} \
    --workload_dir ../../experiments/experiment2/train/{SPLIT_PHILOSOPHY}/ \
    --test_workload_dir ../../experiments/experiment2/test/{SPLIT_PHILOSOPHY}/{ORDER_POLICY}/
```

### Parameters and Models for E2
| Split Philosophy         | Query Order Policy     | `{EXP_CLASS}`                | `{SPLIT_PHILOSOPHY}` | `{ORDER_POLICY}`   | Hugging Face Model for Testing                                                                                                             |
| :----------------------- | :--------------------- | :--------------------------- | :------------------- | :----------------- | :----------------------------------------------------------------------------------------------------------------------------------------- |
| **Random Split**         | Ascending Latency      | `Neo_JOBRandomSplit1`        | `random`             | `asc_latency`      | `experiment2/random_split/NEO/asc_latency/checkpoints/epoch49` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/random_split/NEO/asc_latency/checkpoints/epoch49)) |
|                          | Ascending Complexity   | `Neo_JOBRandomSplit1`        | `random`             | `asc_complexity`   | `experiment2/random_split/NEO/asc_complexity/checkpoints/epoch44` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/random_split/NEO/asc_complexity/checkpoints/epoch44)) |
|                          | Random                 | `Neo_JOBRandomSplit1`        | `random`             | `random`           | `experiment2/random_split/NEO/random/checkpoints/epoch49` ([View Files]([https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/random_split/NEO/random/checkpoints/epoch49])) |
| **Base Query Split**     | Ascending Latency      | `Neo_JOBBaseQuerySplit1`     | `base_query`         | `asc_latency`      | `experiment2/base_query/NEO/asc_latency/checkpoints/epoch49` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/base_query/NEO/asc_latency/checkpoints/epoch49)) |
|                          | Ascending Complexity   | `Neo_JOBBaseQuerySplit1`     | `base_query`         | `asc_complexity`   | `experiment2/base_query/NEO/asc_complexity/checkpoints/epoch49` ([View Files]([https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/base_query/NEO/asc_complexity/checkpoints/epoch49])) |
|                          | Random                 | `Neo_JOBBaseQuerySplit1`     | `base_query`         | `random`           | `experiment2/base_query/NEO/asc_complexity/checkpoints/epoch49` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/base_query/NEO/random/checkpoints/epoch49)) |
| **Leave-One-Out Split**  | Ascending Latency      | `Neo_JOBLeaveOneOutSplit1`   | `leave_one_out`      | `asc_latency`      | `experiment2/leave_one_out/NEO/asc_latency/checkpoints/epoch13` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/leave_one_out/NEO/asc_latency/checkpoints/epoch13)) |
|                          | Ascending Complexity   | `Neo_JOBLeaveOneOutSplit1`   | `leave_one_out`      | `asc_complexity`   | `experiment2/leave_one_out/NEO/asc_complexity/checkpoints/epoch10` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/leave_one_out/NEO/asc_complexity/checkpoints/epoch10)) |
|                          | Random                 | `Neo_JOBLeaveOneOutSplit1`   | `leave_one_out`      | `random`           | `experiment2/leave_one_out/NEO/random/checkpoints/epoch34` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment2/leave_one_out/NEO/random/checkpoints/epoch34)) |


### Testing Worklflow

For the `{CHECKPOINT_PATH}` in the Testing Template, you have two options:
-  **Option A (Your Trained Model):** Point to the output directory from your training run (e.g., `../../models/experiment2/random_split/NEO/asc_latency/checkpoints/epoch49`).
-  **Option B (Pre-trained Model)**: Download the model from Hugging Face and point to its local path (e.g., `<path/to/models>/experiment2/random_split/NEO/asc_latency/checkpoints/epoch49`).

---

## (E3) Learning Trajectory & Convergence

This experiment analyzes NEO's learning process by evaluating model checkpoints saved at different intervals during a single, extended training run. All runs for this experiment use the JOB workload.

**Note**: This experiment uses dedicated scripts: `train_neo_experiment3.py` and `test_neo_experiment3.py`.

**Training**

Run the training script below. It will execute a single, long training session and save checkpoints according to three different policies into subdirectories within your target path.

```bash
python train_neo_experiment3.py --run Neo_JOB_EvaluationBase --local \
    --workload_dir ../../experiments/experiment3/train/ \
    --test_workload_dir ../../experiments/experiment3/test/ \
    --target_checkpoint_dir ../../models/experiment3/NEO/
```

After running, the `--target_checkpoint_dir` will contain the following subdirectories:
- `epoch_checkpoints/`: Models saved at regular epoch intervals.
- `loss_checkpoints/`: Models saved whenever a new minimum validation loss is achieved.
- `query_checkpoints/`: Models saved whenever a new best-performing plan is found for any query.


**Testing & Analysis**

The testing script is designed to evaluate all checkpoints for all three policies automatically. You only need to provide the base directory where the checkpoints were saved

```bash
python test_neo_experiment3.py --run Neo_JOB_EvaluationBase \
    --checkpoint_dir {CHECKPOINT_PATH} \
    --workload_dir ../../experiments/experiment3/train/ \
    --test_workload_dir ../../experiments/experiment3/test/
```

**Setting the `{CHECKPOINT_PATH}`**:
-  **Option A (Your Trained Model):** Point to the base output directory from your training run (e.g., `../../models/experiment3/NEO/`). The script will recursively find and evaluate all models in the subdirectories.
-  **Option B (Pre-trained Model)**: Option B (Pre-trained Models): Download our pre-computed checkpoints from Hugging Face and point to the local base directory.

| Workload | Model Path on Hugging Face | Link to Directory                                                                                                              |
| :------- | :------------------------- | :----------------------------------------------------------------------------------------------------------------------------- |
| **JOB**  | `experiment3/NEO/`         | [View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment3/NEO) |

---

## (E4) Internal Decision-Making & Embeddings

This experiment is divided into sub-sections, each analyzing a different aspect of NEO's internal logic.

### 4.1 Access Path Selection

This sub-experiment tests how NEO adapts its plan choices when the underlying database schema changes (i.e., a new index is added).

**Prerequisites for 4.1**

**Important**: This sub-experiment requires manual database setup before running any commands.

1. **Clone the `imdbload` Database:** Connect to your PostgreSQL instance and create a copy of the `imdbload` database.
```SQL
CREATE DATABASE imdbload_added_index WITH TEMPLATE imdbload;
```

2. **Connect and Add the New Index:** Connect to the newly created database and add an index on `title.production_year`.
```SQL
\c imdbload_added_index
CREATE INDEX idx_title_production_year ON title(production_year);
```

3. **Update Code to Use New Database**: Modify the `pg_executor.py` and `experience.py` files as described in the Pre-Run: Database Connection section at the top of this guide, ensuring they point to the `imdbload_added_index` database.

**Scenario A: JOB-Synthetic Workload**

This scenario uses a synthetic workload designed to test access path selection.

**Training**

```bash
python run.py --run Neo_JOB_EvaluationBase --local \
    --workload_dir ../../experiments/experiment4/4.1/synthetic/ \
    --test_workload_dir ../../experiments/experiment4/4.1/synthetic \
    --target_checkpoint_dir ../../models/experiment4/4.1/job_synthetic/NEO/
```

**Testing**

```bash
python test_model.py --run Neo_JOB_EvaluationBase --local \
    --checkpoint_dir ../../models/experiment4/4.1/job_synthetic/NEO/ \
    --workload_dir ../../experiments/experiment4/4.1/synthetic/ \
    --test_workload_dir ../../experiments/experiment4/4.1/synthetic \
```

**Scenario B: JOB Workload with Added Index**

This scenario uses the standard JOB workload but runs it against the modified database containing the new index.

**Training**

```bash
python run.py --run Neo_JOB_EvaluationBase --local \
    --workload_dir ../../experiments/experiment4/4.1/added_index/ \
    --test_workload_dir ../../experiments/experiment4/4.1/added_index/ \
    --target_checkpoint_dir ../../models/experiment4/4.1/job_added_index/NEO/
```

**Testing**
```bash
python test_model.py --run Neo_JOB_EvaluationBase \
    --checkpoint_dir ../../models/experiment4/4.1/job_added_index/NEO/ \
    --workload_dir ../../experiments/experiment4/4.1/added_index/ \
    --test_workload_dir ../../experiments/experiment4/4.1/added_index/
```

**Model Checkpoints for 4.1: Disclaimer**

**Note**: We were unable to upload pre-trained models for this specific sub-experiment to the Hugging Face Hub.
Therefore, for both scenarios in 4.1, **you must use the models you generated during the Training step for testing**. The testing commands above are already configured to point to the local output directories from the training commands.

### 4.2 Physical Operator Composition

This sub-experiment evaluates NEO's ability to compose different physical operators.

Important: If you have just completed sub-experiment 4.1, you must revert your code configuration. Ensure that `pg_executor.py` and `experience.py` are set to connect to the standard imdbload database, not the `imdbload_added_index` clone.

**Training**

You can train a new model specifically on the 4.2 workload.

```bash
python run.py --run Neo_JOB_EvaluationBase --local \
    --workload_dir ../../experiments/experiment4/4.2/ \
    --test_workload_dir ../../experiments/experiment4/4.2/ \
    --target_checkpoint_dir ../../models/experiment4/4.2/NEO/
```

**Testing**
To evaluate performance, you can use either the model trained above or, more conveniently, reuse the pre-trained model from Experiment 1.

```bash
python test_model.py --run Neo_JOB_EvaluationBase \
    --checkpoint_dir {CHECKPOINT_PATH} \
    --workload_dir ../../experiments/experiment4/4.2/ \
    --test_workload_dir ../../experiments/experiment4/4.2/ \
```
**Setting the `{CHECKPOINT_PATH}`**:
-  **Option A (Your Trained Model):** Point to the output directory from your training run (e.g., `../../models/experiment4/4.2/NEO/epoch49/`).
-  **Option B (Pre-trained Model)**: Option B (Pre-trained Models):  For this experiment, you can use the model trained on the standard JOB workload from Experiment 1. Download it from Hugging Face and point to its local path.

**Model Checkpoint for 4.2 (Reused from E1):**

| Workload | Model Path on Hugging Face | Link to Directory                                                                                                              |
| :------- | :------------------------- | :----------------------------------------------------------------------------------------------------------------------------- |
| **JOB**  | `experiment1/JOB/NEO/epoch49`         | ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment1/JOB/NEO/epoch49)) |

### 4.3 Join Operator Prediction Accuracy

This sub-experiment tests NEO's accuracy when its action space is restricted to using only a single type of join operator.

**Required Setup for 4.3**

**Important**: Before running any command for a specific scenario, you must modify two source files to limit the available join operators.
1. Open `experiments.py` (around line 299) and `envs.py` (around line 69).
2. In both files, **uncomment the line** for the join operator you wish to test and ensure all other `search_space_join_ops` definitions are commented out.

**Example Code Block (in both files):**
```python
  # Default
  p.Define('search_space_join_ops',
           ['Hash Join', 'Merge Join', 'Nested Loop'],
           'Join operators to learn.')

  # NL-only
  # p.Define('search_space_join_ops',
  #          ['Nested Loop'],
  #          'Action space: join operators to learn and use.')
  
  # HJ-only
  # p.Define('search_space_join_ops',
  #          ['Hash Join'],
  #          'Action space: join operators to learn and use.')

  # MJ-only
  # p.Define('search_space_join_ops',
  #          ['Merge Join'],
  #          'Action space: join operators to learn and use.')
```

**Command Templates**
Use the table below to substitute the `{join_operator}` placeholder in the following templates.

**Training Template:**
```bash
python run.py --run Neo_JOB_EvaluationBase --local \
    --workload_dir ../../experiments/experiment4/4.3/{join_operator} \
    --test_workload_dir ../../experiments/experiment4/4.3/{join_operator} \
    --target_checkpoint_dir ../../models/experiment4/4.3/{join_operator}/NEO/
```

**Testing Template:**
```bash
python test_model.py --run Neo_JOB_EvaluationBase --local \
    --checkpoint_dir ../../models/experiment4/4.3/{join_operator}/NEO/
    --workload_dir ../../experiments/experiment4/4.3/{join_operator} \
    --test_workload_dir ../../experiments/experiment4/4.3/{join_operator} \
```

**Parameters for 4.3**
| Scenario              | `{join_operator}` |
| :-------------------- | :---------------- |
| **Hash Join only**    | `hashjoin`        |
| **Merge Join only**   | `mergejoin`       |
| **Nested Loop only**  | `nestloop`        |

**Model Checkpoints for 4.3: Disclaimer**

**Note**: We were unable to upload pre-trained models for this specific sub-experiment to the Hugging Face Hub.

Therefore, for all scenarios in 4.3, **you must use the models you generated during the Training step for testing**. The testing commands above are already configured to point to the local output directories from the training commands.

### 4.4 Embedding Similarity and Plan Quality.

This sub-experiment analyzes the relationship between NEO's learned query embeddings and final plan quality.

**Note:** This experiment uses the dedicated test_model_embeddings.py script for analysis.

**Training**

You can train a new model specifically on the 4.4 workload.

```bash
python run.py --run Neo_JOB_EvaluationBase --local \
    --workload_dir ../../experiments/experiment4/4.4/run1/ \
    --test_workload_dir ../../experiments/experiment4/4.4/run1/ \
    --target_checkpoint_dir ../../models/experiment4/4.4/NEO/
```

**Testing**
To evaluate performance, you can use either the model trained above or, more conveniently, reuse the pre-trained model from Experiment 1.

```bash
python test_model_embeddings.py --run Neo_JOB_EvaluationBase \
    --checkpoint_dir {CHECKPOINT_PATH} \
    --workload_dir ../../experiments/experiment4/4.4/run1 \
    --test_workload_dir ../../experiments/experiment4/4.4/run1 \
```
**Setting the `{CHECKPOINT_PATH}`**:
-  **Option A (Your Trained Model):** Point to the output directory from your training run (e.g., `../../models/experiment4/4.4/NEO/epoch49/`).
-  **Option B (Pre-trained Model)**: For this experiment, you can use the model trained on the standard JOB workload from Experiment 1. Download it from Hugging Face and point to its local path.

**Model Checkpoint for 4.4 (Reused from E1):**

| Workload | Model Path on Hugging Face | Link to Directory                                                                                                              |
| :------- | :------------------------- | :----------------------------------------------------------------------------------------------------------------------------- |
| **JOB**  | `experiment1/JOB/NEO/epoch49`         | ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment1/JOB/NEO/epoch49)) |

---

## (E5) Generalization to Novel Conditions

This experiment assesses NEO's ability to generalize to queries and database conditions not seen during training.

### 5.1 Generalization to New Queries

This sub-experiment evaluates how NEO generalizes to new queries that differ from the training set in terms of distribution, complexity, and selectivity.

**Scenario 1: Distribution Generalization**

This scenario tests how a single model, trained on a standard workload, generalizes to four different, unseen query distributions.

**Training:**
First, train a base model using the standard JOB training set from Experiment 1.

```bash
python run.py --run Neo_JOB_EvaluationBase --local \
    --workload_dir ../../experiments/experiment1/job/train/ \
    --test_workload_dir ../../experiments/experiment1/job/test/ \
    --target_checkpoint_dir ../../models/experiment5/5.1/distribution_generalization/NEO/
```

**Testing:**
Use the trained model to test against each of the four new workloads. Substitute the `{test_workload_name}` placeholder using the table below.

- **Testing Template:**
```bash
python test_model.py --run Neo_JOB_EvaluationBase \
    --checkpoint_dir {CHECKPOINT_PATH} \
    --workload_dir ../../experiments/experiment1/job/train/ \
    --test_workload_dir ../../experiments/experiment5/5.1/distribution_generalization/{test_workload_name}/
```

- **Parameters**

| Test Workload | {test_workload_name} |
| :------------------ | :--------------------- |
| JOB-Dynamic | job_d |
| JOB-Extended | job_extended |
| JOB-Light | job_light |
| JOB-Synthetic | job_synthetic |


**Scenarios 2 & 3: Complexity and Selectivity Generalization**

These two scenarios follow a standard train/test structure where the model is trained and tested on specific, pre-defined splits for each dimension.

**Command Templates:**
Use the table below to substitute the `{generalization_dimension}` placeholder.

- **Training**
```bash
python run.py --run Neo_JOB_EvaluationBase --local \
    --workload_dir ../../experiments/experiment4/4.4/run1/ \
    --test_workload_dir ../../experiments/experiment4/4.4/run1/ \
    --target_checkpoint_dir ../../models/experiment4/4.4/NEO/
```

- **Testing**
```bash
python test_model_embeddings.py --run Neo_JOB_EvaluationBase \
    --checkpoint_dir {CHECKPOINT_PATH} \
    --workload_dir ../../experiments/experiment4/4.4/run1 \
    --test_workload_dir ../../experiments/experiment4/4.4/run1 \
```

**Parameters**

| Scenario | {generalization_dimension} |
| :--------------------------- | :--------------------------- |
| Complexity Generalization| complexity_generalization |
| Selectivity Generalization | selectivity_generalization |

**Setting the `{CHECKPOINT_PATH}`**:
-  **Option A (Your Trained Model)**: Point to the output directory from your training run.
-  **Option B (Pre-trained Model)**: Download the appropriate model from the table below and point to its local path.

| Scenario | Model Path on Hugging Face | Link to Directory                                                                                                              |
| :------- | :------------------------- | :----------------------------------------------------------------------------------------------------------------------------- |
| **Distribution Generalization**  | `experiment1/JOB/NEO/epoch49`         | ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment1/JOB/NEO/epoch49)) |
| **Complexity Generalization**  | `experiment5/5.1/complexity_generalization/NEO/`         | ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment5/5.1/complexity_generalization/NEO/)) |
| **Selectivity Generalization**  | `experiment5/5.1/complexity_generalization/NEO/`         | ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment5/5.1/complexity_generalization/NEO/)) |


### 5.2 Adaptability to New Schemas

This sub-experiment tests NEO's ability to adapt to changes in the database schema, covering both workload-specific and structural shifts.

**Command Templates for 5.2**

To run an experiment, choose a scenario from the table below and substitute the corresponding values into the `{placeholders}` in the templates.

**Training Template**
```bash
python run.py --run {EXP_CLASS} --local \
    --workload_dir ../../experiments/experiment5/5.2/{SCHEMA_TYPE}/train/ \
    --test_workload_dir ../../experiments/experiment5/5.2/{SCHEMA_TYPE}/test/ \
    --target_checkpoint_dir ../../models/experiment5/5.2/{SCHEMA_TYPE}/
```

**Testing Template**
```bash
python test_model.py --run {EXP_CLASS} \
    --checkpoint_dir {CHECKPOINT_PATH} \
    --workload_dir ../../experiments/experiment5/5.2/{SCHEMA_TYPE}/train/ \
    --test_workload_dir ../../experiments/experiment5/5.2/{SCHEMA_TYPE}/test/
```

**Scenarios, Parameters, and Models for 5.2**
| Scenario                       | Prerequisites                                                               | `{SCHEMA_TYPE}` | `{EXP_CLASS}`              | Hugging Face Model for Testing                                                                                                                              |
| :----------------------------- | :-------------------------------------------------------------------------- | :-------------- | :------------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Workload-Specific Schema Shift** | Uses standard `imdbload` DB.                                                | `job`           | `Neo_JOB_EvaluationBase`   | `experiment5/5.2/job/NEO/checkpoints/epoch35` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment5/5.2/job/NEO/checkpoints/epoch35))         |
| **Structural Schema Shift**      | Must connect to the **SSB database** (see Database Connection section). | `ssb`           | `Neo_SSB_EvaluationBase`   | `experiment5/5.2/ssb/NEO/checkpoints/epoch49` ([View Files](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main/experiment5/5.2/ssb/NEO/checkpoints/epoch49))         |

**Setting the `{CHECKPOINT_PATH}`**:
- **Option A (Your Trained Model):** Point to the output directory from your training run (e.g., `../../models/experiment5/5.2/job/`).
- **Option B (Pre-trained Model):** Download the appropriate model from the table above and point to its local path.

### 5.3 Adaptation to Distribution Shifts.

(Guide for 5.3 will be included once we have uploaded the datasets to huggingface)