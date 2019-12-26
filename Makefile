PREFIX=docker-compose run --rm app


lint: FORCE
	${PREFIX} flake8

test: FORCE
	${PREFIX} pytest

stub: FORCE
	${PREFIX} stubgen typedflow -o stub

FORCE:
