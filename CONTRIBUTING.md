# Contributing to PixelQuery

Thanks for your interest in contributing! PixelQuery is an open source project
and we welcome contributions of all sizes — bug reports, documentation
improvements, new features, and reviews.

This document explains how to set up a development environment, the standards
we follow, and how to submit changes.

## Code of Conduct

By participating in this project, you agree to abide by our
[Code of Conduct](CODE_OF_CONDUCT.md). Please read it before interacting in
issues, pull requests, or discussions.

## Quick Links

- [Issue tracker](https://github.com/pixelquery/pixelquery/issues)
- [Pull requests](https://github.com/pixelquery/pixelquery/pulls)
- [Changelog](CHANGELOG.md)
- [Security policy](SECURITY.md)

## Ways to Contribute

- **Report a bug** — open an issue using the *Bug report* template. Include
  a minimal reproduction, expected vs. actual behavior, and your environment.
- **Request a feature** — open an issue using the *Feature request* template.
  Explain the use case and why existing functionality does not cover it.
- **Improve documentation** — fixes to the README, docstrings, examples, or
  this guide are always welcome.
- **Submit a pull request** — see the workflow below.

## Development Setup

### Prerequisites

- Python 3.11, 3.12, or 3.13
- Git

### Clone and install

```bash
git clone https://github.com/pixelquery/pixelquery.git
cd pixelquery

# Create a virtual environment (recommended)
python -m venv .venv
source .venv/bin/activate  # macOS/Linux
# .venv\Scripts\activate   # Windows

# Install in editable mode with all dev dependencies
pip install --upgrade pip
pip install -e ".[icechunk,dev]"
```

### Install pre-commit hooks

PixelQuery uses pre-commit hooks for formatting and linting. Install them
once after cloning:

```bash
pre-commit install
```

The hooks run automatically on `git commit`. To run them on the full
repository at any time:

```bash
pre-commit run --all-files
```

## Running Tests

The full test suite uses `pytest`:

```bash
pytest
```

Run with coverage report:

```bash
pytest --cov=pixelquery --cov-report=term-missing
```

Run only fast tests (skip integration / slow):

```bash
pytest -m "not slow and not integration"
```

Run a single test file or test:

```bash
pytest tests/test_my_module.py
pytest tests/test_my_module.py::TestMyClass::test_my_case
```

## Linting and Type Checking

```bash
# Lint
ruff check .

# Auto-fix what can be fixed
ruff check . --fix

# Format
ruff format .

# Type check
mypy pixelquery/
```

CI runs these on every pull request. Local `pre-commit` already covers `ruff`,
so the commands above are mostly useful when you want to run them on demand.

## Pull Request Workflow

1. **Fork** the repository on GitHub.
2. **Create a branch** from `main` with a descriptive name:
   ```bash
   git checkout -b fix/scene-cache-eviction
   git checkout -b feat/sentinel2-product-profile
   ```
3. **Make focused commits** — one logical change per commit. Use
   [Conventional Commits](https://www.conventionalcommits.org/) prefixes
   when possible:
   - `feat:` for new features
   - `fix:` for bug fixes
   - `docs:` for documentation changes
   - `refactor:` for non-behavioral code restructuring
   - `test:` for test-only changes
   - `chore:` for tooling, build, or dependency changes
4. **Add or update tests** for the change. New features and bug fixes should
   come with tests that fail without the change and pass with it.
5. **Update `CHANGELOG.md`** under the `## [Unreleased]` section, in the
   appropriate category (`Added`, `Changed`, `Fixed`, `Deprecated`,
   `Removed`, `Security`).
6. **Run the checks locally** before pushing:
   ```bash
   pre-commit run --all-files
   pytest
   mypy pixelquery/
   ```
7. **Push and open a pull request** against `main`. Fill in the PR template.
8. **Address review feedback**. Push additional commits to your branch — do
   not force-push during review unless asked, as it makes the diff harder
   to follow.

### What CI checks

Every pull request runs:

- `ruff check` and `ruff format --check`
- `pytest` on Ubuntu and macOS, against Python 3.11 and 3.12
- `mypy` type checking
- Package build (`python -m build`) and `twine check` validation
- Coverage upload to Codecov (one matrix slot)

A pull request must pass all required checks before it can be merged.

## Coding Guidelines

- **Style** — `ruff` is the source of truth. Run `ruff format` before
  committing.
- **Type hints** — public functions and class methods should have type
  annotations. PixelQuery ships a `py.typed` marker, so user-facing types
  are part of the public API.
- **Docstrings** — public APIs need a one-line summary and parameter/return
  descriptions. Examples are welcome.
- **Errors** — raise the appropriate exception from
  `pixelquery.core.exceptions` (`IngestionError`, `QueryError`,
  `ValidationError`, `TransactionError`) rather than generic `Exception`.
  Avoid bare `except:` clauses.
- **Imports** — keep heavy imports (`geopandas`, `rasterio`, `icechunk`)
  inside functions or behind lazy `__getattr__` when they are only needed
  on a subset of code paths. PixelQuery aims for a fast cold-start.
- **No new top-level dependencies without discussion** — if a feature needs
  a new dependency, open an issue first to discuss whether it belongs in
  the base install, an extras group, or as an optional runtime import.

## Releasing

Releases are cut by maintainers. The current process:

1. Ensure `CHANGELOG.md` `[Unreleased]` section is up to date.
2. Move `[Unreleased]` entries under a new `[X.Y.Z] - YYYY-MM-DD` heading.
3. Bump `version` in `pyproject.toml`.
4. Tag the commit: `git tag vX.Y.Z && git push origin vX.Y.Z`.
5. The `release.yml` workflow handles PyPI publish (via Trusted Publishing)
   and GitHub Release creation automatically.

## Questions

If something is unclear, open an issue with the *Question* template, or
start a thread in
[GitHub Discussions](https://github.com/pixelquery/pixelquery/discussions).

Thanks again for contributing!
