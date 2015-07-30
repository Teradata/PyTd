Steps to release:

1) Commit all changes to GitHub:  https://github.com/teradata/PyTd

2) Tag the release

3) Release to PyPI:

python setup.py register -r pypi
python setup.py sdist upload -r pypi

4) Increment version in teradata/versions.py to next release version.
