SPARK_VERSION="3.2.1"
HADOOP_VERSION="2.7"

## help: print this help message
.PHONY: help
help:
	@echo 'Usage:'
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' |  sed -e 's/^/ /'


.PHONY: spark-run
spark-run:
	@echo "Running spark cluster"
	@docker-compose -f infra/docker-compose/spark-cluster.yml up -d

.PHONY: spark-stop
spark-stop:
	@docker-compose -f infra/docker-compose/spark-cluster.yml down
	@echo "Stopped spark cluster"

.PHONY: install
install:
	poetry install 

.PHONY: poetry
poetry:
	@echo "Intalling poetry"
	@curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/install-poetry.py | python3.9 - 

.PHONY:test
test:
	poetry run pytest

.PHONY:run-job
run-job:
	poetry run python src/job.py