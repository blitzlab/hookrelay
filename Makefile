.PHONY: install up down logs test demo migrate

install:
	pip install -r requirements-dev.txt

up:
	docker compose up --build -d

down:
	docker compose down -v

logs:
	docker compose logs -f relay worker

migrate:
	python manage.py migrate

test:
	pytest

demo:
	bash scripts/demo.sh
