.PHONY: setup build full-refresh test-incremental scenarios docs docker-build docker-run clean

export DBT_PROFILES_DIR := .

setup:
	uv sync --locked

build:
	uv run dbt build

full-refresh:
	uv run dbt build --full-refresh

test-incremental:
	uv run ./scripts/test_incremental.sh

scenarios:
	uv run ./scripts/compare_scenarios.sh

docs:
	uv run dbt docs generate && uv run dbt docs serve

docker-build:
	docker build -t flix-marketing-spend .

docker-run:
	docker run --rm -v "$(PWD)/output:/app/output" flix-marketing-spend

clean:
	rm -rf target logs output/*.duckdb* output/daily_spend_eur.csv
