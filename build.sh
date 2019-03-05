rm -r build/
rm -r dist/
rm -r pyathena.egg-info/

# build package
python3 -m pip install --user --upgrade setuptools wheel
python3 setup.py sdist bdist_wheel

# Push to pypi.org repo
python3 -m twine upload dist/*

echo DONT FORGET TO COMMIT AND TAG
# Commit and Tag to github
#git tag v0.0.0
#git push --tags