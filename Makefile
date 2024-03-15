01-prep:
	python3 db_write_performance_benchmark/01_prep/repartition_data.py
02-start-experiment:
	python3 db_write_performance_benchmark/02_write_postgres/write_postgres_spark.py
	python3 db_write_performance_benchmark/02_write_postgres/write_postgres_polars.py
# 	python3 db_write_performance_benchmark/02_write_postgres/write_postgres_pandas.py

run-trials:
	scripts/run.sh
visualize:
	python3 db_write_performance_benchmark/03_visualize_results/visualize_results.py

start-postgres:
	docker compose -f docker-compose-postgres.yaml up -d
