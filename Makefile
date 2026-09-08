.PHONY: all test lint run clean

all: test lint

test:
	pytest tests/ -v 2>/dev/null || python3 -m unittest discover -s tests/ -v

lint:
	python3 -m py_compile app/*.py app/tools/*.py tests/*.py

run:
	python3 -m app.agent

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
