01-prep:
	python3 db_write_performance_benchmark/01_prep/repartition_data.py
02-start-experiment:
# 	python3 db_write_performance_benchmark/02_write_postgres/write_postgres_spark.py
	python3 db_write_performance_benchmark/02_write_postgres/write_postgres_polars.py

start-postgres:
	docker compose -f docker-compose-postgres.yaml up -d
