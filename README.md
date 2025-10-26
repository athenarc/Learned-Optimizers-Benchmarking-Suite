# What Drives Learned Optimizer Performance? A Systematic Evaluation

This repo includes the code base used in the paper "What Drives Learned Optimizer Performance? A Systematic Evaluation", as was submitted for review for the EDBT/ICDT 2026 Joint Conference.

---

## Repository Structure

The repository is organized as follows:

```
.
├── benchmark_scripts/              # Scripts to populate the database after startup (Setup)
├── installation_scripts/           # Scripts to configure the database (Setup)
├── workloads/                      # Base workloads used for training and evaluation (Base Workloads)
├── experiments/                    # Experiment inputs, results, and analysis notebooks (Experiments)
│   └── experimentID/
│       └── benchmark/
│           └── runID/
│               └── queryID/       # Individual query results, e.g., experiment1/job/run1/29c
│   └── *.ipynb                     # Analysis notebooks with figures (Results - Figures)
├── preprocessing/                  # Utility scripts: workload processing, SQL parsing, etc. (Utility Scripts)
├── optimizers/                     # Integrated learned query optimizers (Learned Query Optimizers)
│   ├── balsa/
│   ├── BaoForPostgreSQL/
│   ├── BASE/
│   ├── FASTgres-PVLDBv16/
│   ├── Lero-on-PostgreSQL/
│   ├── LOGER/
│   └── Neo/
├── docker_instructions.md          # Guide for building and managing Docker environments (Setup)
├── citations.md                    # Bibliographic references for workloads and related work (Citations)
```
---

## Setup

This repository serves as a **testbed for evaluating learned query optimizers (LQOs)**.
Our experimental environment consisted of two main components:

1. **CPU-only server:**
   Hosted multiple Docker environments (each with 40 GB of shared memory) running PostgreSQL v12.5 instances. These instances acted as the execution backends for the learned query optimizers.

2. **GPU server:**
   Used to train and run each LQO implementation.

For most users, it is **not necessary** to replicate this multi-server configuration. A single machine with Docker installed and GPU access is sufficient for experimentation.

Refer to the [setup guide](docker_instructions.md) for detailed instructions on building, configuring, and managing Docker containers.
Each optimizer may require specific dependencies or database configurations—please consult the corresponding README files listed below.

---

## Learned Query Optimizers

This repository integrates multiple state-of-the-art learned query optimizers.
Setup instructions, additional notes, and guidance on loading checkpoints for each experiment are provided below:

* **NEO:** See [optimizers/balsa/README.md](optimizers/balsa/README.md)
  *(Note: NEO is executed through the Balsa codebase, as the official NEO implementation was not publicly released.)*

* **BAO:** See [optimizers/BaoForPostgreSQL/README.md](optimizers/BaoForPostgreSQL/README.md)

* **LOGER:** See [optimizers/LOGER/README.md](optimizers/LOGER/README.md)

* **FASTgres:** See [optimizers/FASTgres-PVLDBv16/README.md](optimizers/FASTgres-PVLDBv16/README.md)

* **LERO:** See [optimizers/Lero-on-PostgreSQL/README.md](optimizers/Lero-on-PostgreSQL/README.md)
  *(Note: LERO requires non-default PostgreSQL configurations; refer to its README for setup details.)*

---

## Experiments

Our evaluation framework examines several key aspects of learned query optimizer performance.
Each experiment directory includes documentation describing its objectives, methodology, and any special database configurations required.

1. [(E1) End-to-End Performance & Value Model Fidelity](experiments/experiment1/README.md)
2. [(E2) Sensitivity & Execution Stability](experiments/experiment2/README.md)
3. [(E3) Learning Trajectory & Convergence](experiments/experiment3/README.md)
4. [(E4) Internal Decision-Making & Plan Representation](experiments/experiment4/README.md)
5. [(E5) Generalization to Novel Conditions](experiments/experiment5/README.md)

---

## Models

We provide pretrained model checkpoints for each experiment and optimizer in the following Hugging Face repository:
👉 [**LQO Evaluation Suite Models**](https://huggingface.co/EDBT-2026-Submission/LQO_Evaluation_Suite/tree/main)

Please refer to each optimizer’s documentation for detailed instructions on which model checkpoint to load for each experiment.

--- 

## Results - Figures

The results of each experiment, along with the figures reported in the publication, are available in the Jupyter notebooks below:
- [Experiment 1](experiments/experiment1/experiment1.ipynb)
- [Experiment 2](experiments/experiment2/experiment2.ipynb)
- [Experiment 3](experiments/experiment3/experiment3.ipynb)
- [Experiment 4](experiments/experiment4/experiment4.ipynb)
- [Experiment 5](experiments/experiment5/experiment5.ipynb)

---

## Base Workloads

The learned query optimizers (LQOs) were trained and evaluated using the following workloads. 

* [Join Order Benchmark (JOB)](workloads/imdb_pg_dataset/job)
* [TPC-H](workloads/tpch_loger)
* [TPC-DS](workloads/tpcds)
* [JOB-Synthetic](workloads/imdb_pg_dataset/job_synthetic)
* [JOB-Dynamic](workloads/imdb_pg_dataset/job_d)
* [JOB-Extended](workloads/imdb_pg_dataset/job_extended)
* [JOB-Light](workloads/imdb_pg_dataset/job_light)
* [Star Schema Benchmark (SSB)](workloads/ssb)
* [SSB – After Schema Transformation](workloads/ssb_new_schema)
* [STATS](workloads/stack)
* [STATS – Sampled](workloads/stack_sampled)

---

## Utility Scripts

The [preprocessing](preprocessing/) directory contains a separate repository implementing utility tools for supporting experiments. These include:

* An SQL parser
* Scripts to transform workloads into directories compatible with our experiment structure
* Programs to calculate workload distributions (as used in Experiment E5)
* PostgreSQL warm-up calls
* Additional miscellaneous utilities

For more details, refer to the [preprocessing README](preprocessing/README.md).

---

## Citations

We thank the authors of the previous work for making their research publicly available.
For full citation details, please refer to the [citations](citations.md) file.