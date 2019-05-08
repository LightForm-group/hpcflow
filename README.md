---
title: Quick start guide to `hpcflow`
author: Adam J. Plowman
date: April 2019
---

## Intended usage

- Package is to be installed on the CSF with `pip install --user hpcflow`
- A console entry point `submit` will be installed, which can be used within a directory that contains at least one file like `<submission_filename_fmt>`, as specified in the configuration file.
- Store profiles by default in in `~/.hpcflow/profiles` (maybe allow changing `.hpcflow` directory with an environment variable)
- Store configuration file by default in `~/.hpcflow/config.yml`
- If a `<directory_list_filename_fmt>` file is found in the submission spec directory, job *arrays* are submitted

## Commands
- `hpcflow --version`
- `hpcflow submit` OR `submit`
  - Submit all job specs in the current working directory
- `hpcflow submit <spec>` OR `submit <spec>`
  - Submit a named job spec
- `hpcflow install-example <name>`
  - Install an example set of profiles from the `examples` directory (files with the same name will be overwritten).
- `hpcflow add-inputs <name> <dirname>`: 
  - Add an example set of input files to be used with the example profile the `examples` directory
  - This involves merging `_variable_lookup.yml` and `_archive_locations.yml` from the example into the user's profile directory
- `hpcflow write-cmd <job_name>`:
  - Write the command file for a given jobscript. This script is invoked within jobscripts at execution-time and is not expected to be invoked by the user. The `write-cmd` process involves opening the JSON representation of the profile set and resolving variables for the set of commands within a given command group.
- `hpcflow show-stats`
  - Show statistics like CPU walltime for the profile set in the current directory.
- `hpcflow clean`
  - Remove all `hpcflow`-generated files from the current directory (use confirmation?)
- `hpcflow hfstat` OR `hfstat`
  - Show status of running tasks and how many completed tasks within this directory

### Interacting with the local database

- Of the above commands, the following interact with the local database:
    - `submit`
    - `write-cmd`
    - `show-stats`
    - `hfstat`
- Invoking any of these commands should therefore set up the relevant database connection.
- Only `submit` should invoke the `create_all(engine)` method, all other commands should fail if no database exists. 

## Job specifications (specs) and profiles

### Allowed keys

- `profile`:
  - The name of a profile file (without its extension, if it exists) in the profiles directory from which this specification should inherit.
- `options`:
  - Options to be passed directly to the jobscript
- `command_groups`:
  - Groupings of command to be executed within the jobscripts
  - `commands`:
    - List of commands to execute within one jobscript
    - `parallel`:
      - `variables`:
        - If `True`, and variables have more than one value, the command set will be executed within a job array (i.e in ~parallel). If `False`, and variables have more than one value, the command set will be executed within a `for` loop in the jobscript. This can only be set to `True` if the number of variable values is known at submission time.
- `variables`:
  - Definitions of variables that can appear in the commands of `command_groups`.

## Job submission "dimensions"

1. Scheduler using e.g. SGE or directly executed
2. [If scheduled] Job array versus single job with some sort of loop
3. Parallel versus serial job execution

## Job arrays

- There are two parameters that dictate when job arrays are used versus a loop:
  - `job_array`
  - `job_array_variables`
- Both parameters can be set at the profile level.

### Conditions on setting job array parameters
- For a given profile, if `job_array_variables` is set to `True`, then `job_array` must also be set to `True`.
- For a given profile, if `job_array` is set to `True`, then `job_array` must be set to `True` for all subsequent profiles. (May be relaxed in future.)

## Variables
- Variables are defined in the `variables` dict of the spec file.

### Allowed keys

- `file_regex`:
  - `pattern`:
    - A valid regex pattern that matches one or more file names.
  - `group`:
    - The regex match group index to extract from the regex pattern. Default is `0`.
  - `type`:
    - Determines what data type the regex match group should be cast to; one of `str`, `int`, `float` or `bool`. Default is `str`.
  - `subset`:
    - A list of values of type `file_regex --> type` to include.
- `data`:
  - A list of data to be formatted in `value`. Specify `data` or `file_regex` (or neither), but not both.                          |
- `value`:
  - The formatted values of the variable. This is a string that may include other variables (using the `<<variable_name>>` syntax) and, if either `file_regex` or `data` is specified, at least one Python-like format specifier must be included. If multiple Python-like format specifier is included, the same value will appear in all instances of that format specifier. Default is `{:s}`.


### Variable lookups
- The `_variable_lookup.yml` file provides a place to store frequently used variables.

### Variable templates
- The `_variable_lookup.yml` file also includes variable templates, which allows a simple parametrisation of variable generation.
