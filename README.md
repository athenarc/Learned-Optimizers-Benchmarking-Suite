# Learned-Optimizers-Benchmarking-Suite
This repo acts as a testbed for learned optimizers providing out of the box tools for benchmarking various aspects of learned query optimization

## Docker Setup

### Quick Start

```bash
    docker compose down --rmi all -v     # Clean slate
    docker compose up --build -d         # Rebuild and start
```

Containers: `evaluation_suite` and `evaluation_suite_alt`.

### Rebuilding Specific Containers

1. Stop and remove target container:
```bash
docker container ls
# Find the id of containers named learned-optimizers-benchmarking-suite-db or learned-optimizers-benchmarking-suite-db2

docker stop <container_id>
docker container rm <container_id>
```

2. Remove associated resources:
```bash
# For db1:
docker image rm learned-optimizers-benchmarking-suite-db
docker volume rm learned-optimizers-benchmarking-suite_db_data

# For db2:
docker image rm learned-optimizers-benchmarking-suite-db2
docker volume rm learned-optimizers-benchmarking-suite_db2_data
```

3. Rebuild
```bash
docker compose up --build -d db   # or db2
```

### Postgres Connection Details

- **Credentials**: `suite_user`/`71Vgfi4mUNPm`
- **Databases**: `imdbload`, `tpch`,`tpcds`
- **Ports**: 5468,5469

Connect with:

```bash
psql -U suite_user -d <db> -h train.darelab.athenarc.gr -p <port>
```

### Monitoring

```bash
docker logs -f evaluation_suite
```