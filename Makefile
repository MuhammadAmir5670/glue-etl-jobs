# Define default values for the arguments
JOB_NAME ?= example_job
JOB_ARGS ?= arg1=value1 arg2=value2

# The target to run the Python script
run:
	docker compose exec -it aws-glue python3 run.py --job $(JOB_NAME) --job-args $(JOB_ARGS)

# Help target to display usage information
help:
	@echo "Usage:"
	@echo "  make run JOB_NAME=<job_name> JOB_ARGS='<arg1=value1 arg2=value2 ...>'"
	@echo ""
	@echo "Example:"
	@echo "  make run JOB_NAME=my_job JOB_ARGS='arg1=foo arg2=bar'"
	@echo ""
	@echo "Targets:"
	@echo "  run     Run the specified job"
	@echo "  help    Show this help message"
