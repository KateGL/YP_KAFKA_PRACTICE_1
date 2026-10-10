.PHONY: up down reset

up:
	docker compose -f docker-compose.yaml up --build -d

down:
	docker compose -f docker-compose.yaml down

reset:
	docker compose -f docker-compose.yaml down -v
