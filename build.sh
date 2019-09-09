rm -r build/
rm -r dist/
rm -r pyathena.egg-info/

# build package
python3 -m pip install --user --upgrade setuptools wheel
python3 setup.py sdist bdist_wheel

# Push to pypi.org repo
python3 -m twine upload dist/*

VERSION=$(cat setup.py | grep "version=" | grep -o "v\d\d*.\d\d*.\d\d*" )

echo "DONT FORGET TO COMMIT, PUSH, AND TAG!!!!!!!!!!!!!"
# Commit and Tag to github
echo "        git commit -m 'bump version'"
echo "        git push"
echo "        git tag ${VERSION}"
echo "        git push --tags"