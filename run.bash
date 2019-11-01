#!/bin/bash

COMPOSE_FILE="docker-compose.yaml"
CONTAINER="base"
docker-compose -f ${COMPOSE_FILE} run --rm ${CONTAINER} make${@:1}
