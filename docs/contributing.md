# Contributing

## Getting Started for Local Development

We recommend using Tox to setup the development environment. This will
create a new virtual environment with all of the required packages installed
and ProxyStore installed in editable mode.

```bash
$ git clone https://github.com/proxystore/proxystore
$ cd ProxyStore
$ tox --devenv venv -e py39
$ . venv/bin/activate
```

!!! warning

    Running Tox in a Conda environment is possible but it may conflict with
    Tox's ability to find the correct Python versions. E.g., if your
    Conda environment is Python 3.9, running `#!bash $ tox -e p38` may still use
    Python 3.9.

## Continuous Integration

ProxyStore uses [pre-commit](https://pre-commit.com/) and
[Tox](https://tox.wiki/en/latest/index.html) for continuous integration
(test, linting, etc.).

### Linting and Type Checking (pre-commit)

To use pre-commit, install the hook and then run against files.

```bash
$ pre-commit install
$ pre-commit run --all-files
```

### Tests (tox)

The entire CI workflow can be run with `#!bash $ tox`.
This will test against multiple versions of Python and can be slow.

Module-level unit-test are located in the `tests/` directory and its
structure is intended to match that of `proxystore/`.
E.g. the tests for `proxystore/store/cache.py` are located in
`tests/store/cache_test.py`; however, additional test files can be added
as needed. Tests should be narrowly focused and target a single aspect of the
code's functionality, tests should not test internal implementation details of
the code, and tests should not be dependent on the order in which they are run.

Code that is useful for building tests but is not a test itself belongs in the
`testing/` directory.

```bash
# Run all tests in tests/
$ tox -e py39
# Run a specific test
$ tox -e py39 -- tests/factory_test.py::test_lambda_factory
```

Many of the tests are asyncio tests.
The asyncio default event loop is used by default, but uvloop can be used
instead by passing `--use-uvloop` to pytest.

### Tests (docker)

The test suite mocks certain third-party programs that cannot be installed via
pip (e.g., Margo, UCX, Redis). For Margo and UCX, a Docker image is provided
at
[proxystore/images](https://github.com/proxystore/images)
with the software pre-built and Python installed. The tox environments
`py{version}-dim` can be run in the container to validate changes against
the un-mocked software.

```bash
$ docker pull ghcr.io/proxystore/proxystore-dim:nightly
$ docker run --rm -it --network host -v /path/to/proxystore:/proxystore proxystore-dim
# Inside container
$ tox -e py310-dim
```

## Docs

If code changes require an update to the documentation (e.g., for function
signature changes, new modules, etc.), the documentation can be built using
MKDocs.

```bash
# Manually
$ pip install -e .[docs]
$ mkdocs build --strict  # Build only to site/index.html
$ mkdocs server          # Serve locally

# With tox
$ tox -e docs
```

Docstrings are automatically generated, but it is recommended to check the
generated docstrings to make sure details/links/etc. are correct.

## Style Guide

The Python code and docstring format mostly follows Google's
[Python Style Guide](https://google.github.io/styleguide/pyguide.html),
but the pre-commit config is the authoritative source for code format
compliance.

**Nits:**

* Avoid imports in `__init__.py` (reduces the likelihood of circular imports).
* Prefer pure functions where possible.
* Define all class attributes inside `__init__` so all attributes are visible
  in one place. Attributes that are defined later can be set as `None`
  as a placeholder.
* Prefer f-strings (`#!python f'name: {name}`) over string format
  (`#!python 'name: {}'.format(name)`). Never use the `%` operator.
* Prefer [typing.NamedTuple][] over [collections.namedtuple][].
* Use lower-case and no punctuation for log messages, but use upper-case and
  punctuation for exception values.
  ```python
  logger.info(f'new connection opened to {address}')
  raise ValueError('Name must contain alphanumeric characters only.')
  ```
* Document all exceptions that may be raised by a function in the docstring.

## Issues

We use GitHub issues to report problems, request and track changes, and discuss
future ideas.
If you open an issue for a specific problem, please follow the template guides.

## Pull Requests

We use the standard GitHub contribution cycle where all contributions are
made via pull requests (including code owners!).

1. Fork the repository and clone to your local machine.
2. Create local changes.
    - Changes should conform to the style and testing guidelines, referenced
      above.
    - Preferred commit message format ([source](https://cbea.ms/git-commit/)):
        * separate subject from body with a blank line,
        * limit subject line to 50 characters,
        * capitalize first word of subject line,
        * do not end the subject line with a period,
        * use the imperative mood for subject lines,
        * include related issue numbers at end of subject line,
        * wrap body at 72 characters, and
        * use the body to explain what/why rather than how.
      Example: `Fix concurrency bug in Store (#42)`
3. Push commits to your fork.
    - Please squash commits fixing mistakes to keep the git history clean.
      For example, if commit "b" follows commit "a" and only fixes a small typo
      from "a", please squash "a" and "b" into a single, correct commit.
      This keeps the commit history readable and easier to search through when
      debugging (e.g., git blame/bisect).
4. Open a pull request in this repository.
    - The pull request should include a description of the motivation for the
      PR and included changes. A PR template is provided to guide this process.


## Release Instructions

1. Choose the next version number, referred to as `{VERSION}` for the
   rest of the instructions. ProxyStore versioning follows semver
   (`major.minor.patch`) with optional [PEP-440](https://peps.python.org/pep-0440)
   pre-release/post-release/dev-release segments. Major/minor/patch numbers
   start at 0 and pre-release/post-release/dev-release segments start at 1.
2. Update the version in `pyproject.toml` to `{VERSION}`.
3. Commit and merge the version updates/changelogs into main.
4. Tag the release commit and push (typically this is the commit updating the
   version numbers).
   ```bash
   $ git tag -s v{VERSION} -m "ProxyStore v{VERSION}"
   $ git push origin v{VERSION}
   ```
   Note the version number is prepended by "v" for the tags so we can
   distinguish release tags from non-release tags.
5. Create a new release on GitHub using the tag. The title should be
   `ProxyStore v{VERSION}`.
6. **Official release:**
    1. Use the "Generate release notes" option and set the previous tag as the previous official release tag. E.g., for `v0.4.1`, the previous release tag should be `v0.4.0` and NOT `v0.4.1a1`.
    2. Add an "Upgrade Steps" section at the top (see previous releases for examples).
    3. Review the generated notes and edit as needed. PRs are organized by tag, but some PRs will be missing tags and need to be moved from the "Other Changes" section to the correct section.
    4. Select "Set as the latest release."
7. **Unnofficial release:** (alpha/dev builds)
    1. Do NOT generate release notes. The body can be along the lines of "Development pre-prelease for `V{VERSION}`."
    2. Leave the previous tag as "auto."
    3. Select "Set as a pre-release."
