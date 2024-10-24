.PHONY: run down test

build:
	docker compose -f docker-compose.yml up --build

run:
	docker compose -f docker-compose.yml up

down:
	docker compose -f docker-compose.yml down -v
