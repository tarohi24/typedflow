PREFIX=docker-compose run --rm app


lint:
	${PREFIX} flake8


test:
	${PREFIX} pytest
