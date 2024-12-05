# Learned-Optimizers-Benchmarking-Suite
This repo acts as a testbed for learned optimizers providing out of the box tools for benchmarking various aspects of learned query optimization

## Docker Setup

### How to Run the Image

1. **Stop existing containers and remove volumes:**

    ```bash
    docker-compose down -v
    ```

2. **Build and start the container:**

    ```bash
    docker-compose up --build -d
    ```

   The container will be named `evaluation_suite`.

### Postgres Connection Details

- **User**: `suite_user`
- **Password**: `71Vgfi4mUNPm`
- **Database**: `suite_db`

Connect with:

```bash
psql -h localhost -U suite_user -d suite_db
```

### Debugging

View logs with:

```bash
docker logs evaluation_suite
```