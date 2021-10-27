#!make
include scripts/envfile
export $(shell sed 's/=.*//' scripts/envfile)



run-data-pull:
	python3 main.py

run-db-query:
	python3 query.py



