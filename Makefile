build-conda:
	conda env create --file environment.yml

black:
	black proxystore

flake8:
	flake8 proxystore --count --show-source --statistics

pytest:
	pytest --cache-clear --cov=proxystore --cov-report term-missing proxystore 

test: black flake8 pytest
