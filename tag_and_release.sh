#!/bin/bash

VERSION=$1

PS_VERSION=$(python -c "import proxystore; print(proxystore.__version__)")

if [[ $PS_VERSION == $VERSION ]]
then
    echo "Version requested matches package version: $VERSION"
else
    echo "[ERROR] Version mismatch. User request: $VERSION while package version is: $PS_VERSION"
    exit -1
fi


create_tag () {

    echo "Creating tag"
    git tag -a "$VERSION" -m "ProxyStore $VERSION"

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
