.PHONY: demo live-sample soak test lint typecheck clean

SAMPLE_SECONDS ?= 45
SOAK_HOURS ?= 72

demo:
	uv run exchange-collector demo

live-sample:
	uv run exchange-collector live-sample --seconds $(SAMPLE_SECONDS) --write-assets

soak:
	uv run exchange-collector soak --hours $(SOAK_HOURS)

test:
	uv run pytest -q

lint:
	uv run ruff check src tests

typecheck:
	uv run mypy src tests

clean:
	rm -rf data/*.db data/*.db-wal data/*.db-shm reports/* .mypy_cache .pytest_cache .ruff_cache
