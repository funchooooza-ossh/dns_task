DOCKER_PATH := ./docker
ENV_PATH := ./environments
include ./environments/.env

.PHONY: run init migrate



prepare:
	@sh -c '\
		set -a; \
		export MAIN_ARGS="$(MAIN_ARGS)"; \
		. $(ENV_PATH)/.env; \
		envsubst < ./docker/docker-compose.base.yaml > ./docker/docker-compose.yaml;'


migrate:
	MAIN_ARGS="python cli.py migrate" $(MAKE) prepare
	docker compose -f $(DOCKER_PATH)/docker-compose.yaml up --force-recreate -d --build

run:
	MAIN_ARGS="python cli.py serve" $(MAKE) prepare
	docker compose -f $(DOCKER_PATH)/docker-compose.yaml up --force-recreate -d --build

logs:
	docker compose -f $(DOCKER_PATH)/docker-compose.yaml logs -f

