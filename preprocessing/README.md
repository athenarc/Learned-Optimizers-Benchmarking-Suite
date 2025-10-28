# Preprocessing Utilities

This directory contains a suite of Python scripts and notebooks used to prepare, analyze, and transform the raw SQL workloads into the structured format required by our evaluation framework. These tools are essential for setting up the experiment directories as found in `../experiments/`.

## Overview

The preprocessing workflow generally involves:
1.  **Workload Analysis:** Understanding the statistical properties of a SQL workload.
2.  **Experiment Setup:** Splitting raw workloads into train/test sets, executing queries to get baseline performance (classic QEPs), and organizing them into the standardized `experiments/<exp_id>/...` structure.
3.  **Query Generation:** Creating new, synthetic queries for specific generalization experiments.

## Environment Setup

To run these scripts, create and activate the dedicated Conda environment:
```bash
conda env create -f environment.yml
conda activate preprocessing
```
Additionally, you must create a `.env` file in the root of the main repository to configure database credentials, using `.env.example` as a template.

---

## Directory Structure

The `preprocessing` directory is organized as follows:

```
.
├── preprocessing_scripts/  # Main scripts for setting up E1-E5
│   ├── preprocessing_1.py
│   ├── preprocessing_2.py
│   ├── ...
│   └── warm_cache.py
├── query_gen/              # Tools for generating synthetic queries
│   └── gen_queries.py
├── workload_analysis.py    # Core logic for analyzing workload characteristics
├── workload_analysis.ipynb # Notebook for visualizing workload analysis
├── sql_parser.py           # Utility for parsing SQL queries
├── selectivity.py          # Utilities for estimating query selectivity
├── distribution.py         # Utilities for statistical distribution analysis
└── ...                     # Other helper modules
```

---

## Key Components

### 1. `preprocessing_scripts/`

This is the most critical directory, containing the scripts used to generate the structured experiment directories. Each script is named according to the experiment it prepares.

*   **`preprocessing_1.py`, `preprocessing_2.py`, etc.:** These scripts correspond to the setup for Experiments E1, E2, and so on. They read queries from a source workload directory, execute them against the database to capture the classic query execution plan (`classic_qep.json`), and organize the queries and results into the required `train/` and `test/` subdirectories.

*   **`warm_cache.py`:** A utility script to execute a set of queries against the database multiple times. This is used to warm up the database buffer cache before running performance-sensitive experiments, ensuring more stable measurements.

**To replicate our experiment structure, you would run the scripts in this directory.** For example, to set up Experiment 1, you would configure and run `preprocessing_1.py`.

### 2. Workload Analysis (`workload_analysis.py` & `.ipynb`)

These files are used to understand the statistical properties of a given SQL workload.
*   `workload_analysis.py`: Contains functions to parse a directory of queries and calculate metrics like table-touch frequency, column-touch frequency, and join connection frequencies.
*   `workload_analysis.ipynb`: A Jupyter Notebook that uses the analysis script to generate visualizations, such as the query connection graph, which helps in understanding the structural properties of a benchmark. This was used to generate figures for our workload distribution analysis.

### 3. Query Generation (`query_gen/`)

This module is used to generate new, synthetic queries for generalization experiments (e.g., E4.1, E5.1).
*   `gen_queries.py`: The main script that takes query templates (`.toml` files) as input and generates variations by substituting parameters.
*   `templates/`: Contains template files that define the structure of a query, which can then be instantiated with different predicates or values.

### 4. Core Utility Modules

*   **`sql_parser.py`:** A robust SQL parser that extracts key components from a query string, such as tables, columns, joins, and filter predicates.
*   **`selectivity.py`:** Contains functions to estimate the selectivity of filter predicates by querying the database and analyzing column data distributions.
*   **`distribution.py` & `processor.py`:** These modules implement the logic for calculating the statistical distance between two workloads (e.g., Jensen-Shannon divergence), as used in our analysis for Experiment E5.
```