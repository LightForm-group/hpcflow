# Change Log

## [0.1.7] - 2020.05.12

### Changed

- Removed a bunch of debugging `print`s.

## [0.1.6] - 2020.05.11

### Fixed

- Specify `six` version to ensure successful `dropbox` installation.

## [0.1.3], [0.1.4], [0.1.5] - 2020.05.07

### Packaging issues

- Added `__init__.py` to `archive` sub-package and nested sub-packages.
- Fixed a typo in the console scripts that was causing installation to fail.

## [0.1.2] - 2020.05.06

### Fixed

- Fixed issue where submission would fail if a directory that is not the current working directory was passed to `make_workflow` or `submit_workflow`.

## [0.1.1] - 2019.06.14

### Added

- Added `author_email`, `long_description` and more `classifiers` to `setup.py` script.

## [0.1.0] - 2019.06.14

- Initial functioning release. Not released on PyPI.
