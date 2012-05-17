test:
	pep8 --exclude=migrations --ignore=E501,E225 nydus || exit 1
	pyflakes -x W nydus || exit 1
	coverage run --include=nydus/* setup.py test && \
	coverage html --omit=*/migrations/* -d cover
