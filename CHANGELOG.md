# Change Log

## [0.1.12] - 2020.12.16

### Fixed

- Fix `kill` for partial iterations and stats jobs.

## [0.1.11] - 2020.08.25

### Fixed

- Add missing allowed profile key `parallel_modes`, whose absence was resulting in failure when submitting from a profile file.
- Fix iterations.

## [0.1.10] - 2020.07.07

### Fixed

- Use correct YAML library in `profiles.py`
- Fix alternate scratch
- Allow using the same alternate scratch in different command groups

## [0.1.9] - 2020.06.09

### Changed

- Improved Dropbox authorization flow.

## [0.1.8] - 2020.06.09

### Changed

- Latest dev branch merge

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
