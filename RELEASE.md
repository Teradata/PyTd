Steps to release:

1) Commit and push all changes to GitHub:  https://github.com/teradata/PyTd

2) Tag the release, E.g.

git tag -a v15.10.00.03 -m 'Release version 15.10.00.03'
git push origin --tags

3) Release to PyPI:

python setup.py register -r pypi
python setup.py sdist upload -r pypi

4) Increment version in teradata/versions.py to next release version.
