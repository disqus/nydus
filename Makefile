test:
	flake8 --ignore=E501,E225,E121,E123,E124,E125,E127,E128 --exit-zero nydus
	python setup.py test
