.PHONY: install up down seed validate run-a run-b run-c run-d debug-view init-sp clean

VENV = venv
PYTHON = $(VENV)/bin/python
PIP = $(VENV)/bin/pip

$(VENV)/bin/activate: requirements.txt
	python3 -m venv $(VENV)
	$(PIP) install -r requirements.txt
	touch $(VENV)/bin/activate

install: $(VENV)/bin/activate

up:
	docker-compose up -d
	@echo "Waiting for database to be ready..."
	@sleep 10

down:
	docker-compose down

seed: install
	$(PYTHON) main.py seed --amount 100000 --batch-size 1000

seed-full: install
	$(PYTHON) main.py seed --amount 10000000 --batch-size 2000

validate: install
	$(PYTHON) main.py validate

# Initialize Stored Procedures (needed if volume wasn't wiped)
init-sp: install
	$(PYTHON) apply_sp.py

run-a: install
	$(PYTHON) main.py run --mix A --time 60 --concurrency 8

run-b: install
	$(PYTHON) main.py run --mix B --time 60 --concurrency 8

run-c: install
	$(PYTHON) main.py run --mix C --time 60 --concurrency 8

run-d: install
	$(PYTHON) main.py run --mix D --time 60 --concurrency 8

debug-view: install
	$(PYTHON) debug_view.py

clean:
	rm -rf __pycache__ src/__pycache__
	rm -rf $(VENV)
