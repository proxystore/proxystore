build-conda:
	conda env create --file environment.yml

flake8:
	flake8 proxystore --count --show-source --statistics

pytest:
	pytest --cache-clear --cov=proxystore proxystore 

test: flake8 pytest
