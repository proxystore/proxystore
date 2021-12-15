build-conda:
	conda env create --file environment.yml

black:
	black proxystore examples setup.py

flake8:
	flake8 . --count --show-source --statistics

pytest:
	pytest --cache-clear --cov=proxystore --cov-report term-missing \
		--ignore proxystore/store/test/test_globus.py proxystore 

test: black flake8 pytest

html:
	cd docs ; make html ; cd ..
