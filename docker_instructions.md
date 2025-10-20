## Docker Environment Setup Guide

This guide provides instructions for building, managing, and populating the Docker-based PostgreSQL environments used for evaluating the Learned Query Optimizers (LQOs) in this testbed.

As mentioned in the main [README.md](README.md), our setup relies on one or more PostgreSQL instances to act as the execution backend for the optimizers. This Docker configuration simplifies the process by providing pre-configured, isolated database environments.

Prerequisites
- [Docker](https://docs.docker.com/get-started/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

---

### Step 1: Build and Start the Database Containers

The [docker-compose.yml](docker-compose.yml) file is configured to launch two identical PostgreSQL v12.5 containers. This allows you to run experiments in parallel or test different database configurations simultaneously.

The two container services are named:
- `evaluation_suite`
- `evaluation_suite_alt`

To begin the build and automated setup process, run:
```bash
docker compose up --build -d
```

### Step 2: Monitor the Setup Progress

You must monitor the container logs to know when the database population is complete.

**To view the real-time logs for the primary container, run:**
```bash
docker logs -f evaluation_suite
```

You will see a sequence of setup messages. The most time-consuming step will begin after you see this line:
```bash  
Running benchmark loaders...
```

The setup is complete when the script finishes its final configuration tasks. The log output will stop after messages similar to these appear:
```bash
Creating plperl and plperlu extensions...
Defining the clear_cache() function...
Defining the write_lero_card_file() function...
```

Once the log stream stops, the database is fully populated and ready for use.

---

### Step 3: Verify the Setup (Optional)

After the logs indicate the setup is finished, you can verify that the databases were created successfully by connecting to the container and listing them.

```bash
docker exec -it evaluation_suite /usr/local/pgsql/12.5/bin/psql -U suite_user -p 5468 -l
```
You should see databases like `imdbload`, `stats`, `ssb`, etc., in the output list.

---

## Managing the Environment

Here are common commands for managing the Docker environment after the initial setup.

**Checking Container Status**

To see which containers are running:
```bash
docker compose ps
```

**Stopping the Containers**

To stop the containers without deleting any data (the populated databases will be preserved):
```bash
docker compose stop
```

You can restart them later with `docker compose start`.

**Full Reset (Clean Slate)**

If you need to completely reset the environment and **re-run the entire automated population process from scratch**, use the following command.

**WARNING**: This action is irreversible and will delete all existing database data.
```bash
docker compose down --rmi all -v
```
After running this, you can start fresh by returning to **Step 1**.

---

## Next Steps

Your database backends are now fully configured and populated. You can proceed with training and evaluating the Learned Query Optimizers. Please refer back to the main [README.md](README.md) and the individual optimizer/experiment documentation for further instructions.