# AWS Glue Job Runner

This project provides a framework to run Glue jobs locally using a command-line interface.

## Project Structure

- **`run.py`**: The main script that runs the specified PySpark job as a subprocess.
- **`jobs/`**: Directory containing individual job scripts. Each job is expected to be a Python script.
- **`libs/`** & **`utils/`**: Directory or ZIP file containing any additional libraries needed for the jobs.
- **`Makefile`**: A file that simplifies running the `run.py` script with different job names and arguments.


## Prerequisites

- **Docker**: Ensure that Docker and docker compose is installed on your machine.

## Usage

### Running a Job

You can run a PySpark job using the `run.py` script directly or via the `Makefile`.

#### Directly Using `run.py`

```bash
make run JOB_NAME=aggregate_patients_etl JOB_ARGS='JOB_NAME=patients output_path=s3://<bucket-name>/health-helper/patients/processed/'
```

The job name should be the name of module inside the jobs directory, the run file will run jobs.job_name module in a subprocess.


### Adding New Jobs
To add a new job:

1. Create a new Python file in the `jobs/` directory.
2. Use `make run` command to execute the job.
