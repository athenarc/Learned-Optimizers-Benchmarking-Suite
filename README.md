# Learned-Optimizers-Benchmarking-Suite

This repo acts as a testbed for learned optimizers.

## Docker Setup

### Quick Start

```bash
docker compose down --rmi all -v     # Clean slate  
docker compose up --build -d         # Rebuild and start
```

This launches the following containers:

* `evaluation_suite`
* `evaluation_suite_alt`

### Rebuilding Specific Containers

1. **Stop and remove the target container:**

```bash
docker container ls
# Find the ID of containers named learned-optimizers-benchmarking-suite-db or learned-optimizers-benchmarking-suite-db2

docker stop <container_id>
docker container rm <container_id>
```

2. **Remove associated resources:**

```bash
# For db1:
docker image rm learned-optimizers-benchmarking-suite-db
docker volume rm learned-optimizers-benchmarking-suite_db_data

# For db2:
docker image rm learned-optimizers-benchmarking-suite-db2
docker volume rm learned-optimizers-benchmarking-suite_db2_data
```

3. **Rebuild:**

```bash
docker compose up --build -d db   # or db2
```

### Postgres Connection Details

* **Credentials**:
  `suite_user` / `71Vgfi4mUNPm`

* **Databases**:
  `imdbload`, `tpch`, `tpcds`

* **Ports**:
  `5468`, `5469`

**Connect using:**

```bash
psql -U suite_user -d <db> -h train.darelab.athenarc.gr -p <port>
```

### Monitoring

```bash
docker logs -f evaluation_suite
```

---

## Project Structure

The repository is organized as follows:

```
.
├── benchmark_scripts/              # Scripts to populate the database after startup
├── installation_scripts/          # Scripts to configure the database
├── workloads/                     # Benchmarks for training learned optimizers
├── experiments/                   # Contains test inputs, results, and analysis notebooks
│   └── experimentID/
│       └── benchmark/
│           └── runID/
│               └── queryID/       # e.g., experiment1/job/run1/29c
│   └── *.ipynb                    # Jupyter notebooks with analysis results
├── preprocessing/                 # Fork of `jobgen` for generating experiment inputs
├── optimizers/                    # Integrated learned query optimizers
│   ├── balsa/
│   ├── BaoForPostgreSQL/
│   ├── BASE/
│   ├── FASTgres-PVLDBv16/
│   ├── Lero-on-PostgreSQL/
│   ├── LOGER/
│   └── Neo/
└── models/                        # Stores model checkpoints from training
```

---

## Usage Notes

1. The `benchmark_scripts/` and `installation_scripts/` directories contain scripts that automatically run after container initialization to configure and populate the evaluation suite.

2. Refer to each optimizer’s directory under `optimizers/` for system-specific training and testing instructions.

3. To run experiments:

   * Train the optimizers using data from either the `workloads/` or `experiments/` directory, depending on experiment requirements.
   * Save the trained models in the `models/` directory.
   * Use the `preprocessing/` directory to generate experiment inputs.
   * Configure each optimizer's testing script to load the corresponding model from `models/`.
   * Execute the experiments using the generated inputs and analyze results via the Jupyter notebooks in `experiments/`.