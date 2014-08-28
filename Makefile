.PHONY: clean virtualenv upgrade test package dev run

PYENV = . env/bin/activate;
PYTHON = $(PYENV) python
PYTHON_TIMED = $(PYENV) time python

run: dev
	$(PYTHON) examples/example.py

package: env
	$(PYTHON) setup.py bdist_egg
	$(PYTHON) setup.py sdist

test: env dev
	$(PYENV) nosetests $(NOSEARGS)

dev: env/bin/activate dev_requirements.txt
	$(PYENV) pip install --process-dependency-links -e . -r dev_requirements.txt

clean:
	$(PYTHON) setup.py clean
	find . -type f -name "*.pyc" -exec rm {} \;

nuke: clean
	rm -rf *.egg *.egg-info env bin cover coverage.xml nosetests.xml

env virtualenv: env/bin/activate
env/bin/activate: requirements.txt setup.py
	test -f $@ || virtualenv --no-site-packages env
	$(PYENV) pip install --process-dependency-links -e . -r requirements.txt
	touch $@

upgrade: env
	$(PYENV) pip install --process-dependency-links -e . -r requirements.txt --upgrade
