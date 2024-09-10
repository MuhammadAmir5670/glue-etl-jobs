import os
import sys
import time
import logging
import argparse
import subprocess

from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

def run_job_as_process(job_name, job_args):
    """
    Runs the specified job as a separate process.

    Parameters:
    - job_name: str
        The name of the job module to run.
    - job_args: dict
        Arguments to pass to the job module.
    """
    # Convert job_args dict to command-line arguments
    job_args_list = [f'--{key}={value}' for key, value in job_args.items()]

    # Build the command to run the job (python3 jobs/job_name with arguments)
    command = ['python3', os.path.join('jobs', job_name + '.py')] + job_args_list

    logging.info(f'Running command: {" ".join(command)}')

    start = time.time()

    # Run the job as a subprocess
    result = subprocess.run(command, env=os.environ, text=True)

    end = time.time()
    duration = end - start

    if result.returncode == 0:
        logging.info(f"Execution of job {job_name} took {duration} seconds")
        logging.info("Job output:\n", result.stdout)
    else:
        sys.exit(result.returncode)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a PySpark job')
    parser.add_argument(
        '--job', type=str, required=True, dest='job_name',
        help="The name of the job module you want to run. (ex: poc will run job on jobs.poc package)")
    parser.add_argument(
        '--job-args', nargs='*',
        help="Extra arguments to send to the PySpark job (example: --job-args template=manual-email1 foo=bar")

    args = parser.parse_args()
    logging.info(f"Called with arguments: {args}")

    environment = {
        'PYSPARK_JOB_ARGS': ' '.join(args.job_args) if args.job_args else ''
    }

    job_args = dict()
    if args.job_args:
        job_args_tuples = [arg_str.split('=') for arg_str in args.job_args]

        job_args = {a[0]: a[1] for a in job_args_tuples}

    logging.info(f'Running job {args.job_name}...\nenvironment is {environment}')

    os.environ.update(environment)

    run_job_as_process(args.job_name, job_args)
