test:
	pip install flake8
	flake8 --exclude=migrations --ignore=E501,E225,E121,E123,E124,E125,E127,E128 --exit-zero nydus || exit 1
	python setup.py test

publish:
	python setup.py sdist bdist_wheel upload

.PHONY: test publish
