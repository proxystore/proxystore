#!/bin/bash

VERSION=$1

PS_VERSION=$(python -c "import proxystore; print(proxystore.__version__)")

# Get line from conf.py that is "release = 'x.x.x'"
DOC_VERSION=$(cat docs/conf.py | grep release)
# Convert to array
DOC_VERSION=($DOC_VERSION)
# Get third element
DOC_VERSION=${DOC_VERSION[2]}
# Remove quotes around version number
DOC_VERSION="${DOC_VERSION:1:${#DOC_VERSION}-2}"

if [[ $PS_VERSION == $VERSION ]]
then
    echo "Version requested matches package version: $VERSION"
else
    echo "[ERROR] Version mismatch. User request: $VERSION while package version is: $PS_VERSION"
    exit -1
fi

if [[ $DOC_VERSION == $VERSION ]]
then
    echo "Documentation version requested matches package version: $VERSION"
else
    echo "[ERROR] Version mismatch. User request: $VERSION while documentation version is: $DOC_VERSION"
    exit -1
fi

create_tag () {

    echo "Creating tag"
    git tag -a "v$VERSION" -m "ProxyStore $VERSION"

    echo "Pushing tag"
    git push origin --tags

}


release () {
    rm dist/*

    echo "======================================================================="
    echo "Starting clean builds"
    echo "======================================================================="
    python -m build

    echo "======================================================================="
    echo "Done with builds"
    echo "======================================================================="
    sleep 1
    echo "======================================================================="
    echo "Push to PyPi. This will require your username and password"
    echo "======================================================================="
    python -m twine upload dist/*
}


create_tag
release
