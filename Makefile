.PHONY: help install produce run report all test clean compose-up compose-down produce-kafka

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

install:      ## Install dependencies
	pip install -r requirements.txt

produce:      ## Generate the event stream into data/raw/events.jsonl
	python -m streaming.cli produce

run:          ## Process the stream in micro-batches
	python -m streaming.cli run

report:       ## Redraw the charts in docs/images
	python -m streaming.cli report

all: produce run report   ## End to end, no Docker needed

test:         ## Run the test suite
	pytest -q

clean:
	rm -rf data/lake data/checkpoints data/raw reports/*.json docs/images/*.png

compose-up:   ## Start Kafka and the Kafka UI
	docker compose up -d

compose-down:
	docker compose down -v

produce-kafka: ## Produce to a running Kafka instead of a file
	KAFKA_BOOTSTRAP=localhost:9092 python -m streaming.cli produce
