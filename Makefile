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
	docker compose -f $(DOCKER_PATH)/docker-compose.yaml exec backend python cli.py migrate

run:
	MAIN_ARGS="python cli.py serve" $(MAKE) prepare
	docker compose -f $(DOCKER_PATH)/docker-compose.yaml up --force-recreate -d --build

logs:
	docker compose -f $(DOCKER_PATH)/docker-compose.yaml logs -f

etl-populate-history:
	docker compose -f $(DOCKER_PATH)/docker-compose.yaml exec backend python etl/populate_history.py

etl-populate-products:
	docker compose -f $(DOCKER_PATH)/docker-compose.yaml exec backend python etl/populate_products.py

etl-populate-products-vol:
	docker compose -f $(DOCKER_PATH)/docker-compose.yaml exec backend python etl/populate_products_vol.py
etl-generate-logdays:
	docker compose -f $(DOCKER_PATH)/docker-compose.yaml exec backend python etl/generate_logdays.py

etl-generate-needs:
	docker compose -f $(DOCKER_PATH)/docker-compose.yaml exec backend python etl/generate_needs.py

etl-generate-limits:
	docker compose -f $(DOCKER_PATH)/docker-compose.yaml exec backend python etl/generate_shipment_and_limits.py

etl-populate: etl-populate-history etl-populate-products etl-generate-logdays etl-generate-needs etl-generate-limits etl-populate-products-vol
