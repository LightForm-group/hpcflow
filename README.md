# hpcflow

## Installation

### Package installation

#### Install with `pip`

`hpcflow` is a Python package that can be installed from PyPI by typing:

`pip install --user hpcflow`

### Set up

Some console entry points (scripts/executables) will be added to your system as part of the package installation. These are:

1. `hpcflow`, and all of its sub-commands (found by typing `hpcflow --help` at the command line)
2. `hfmake`, which is an alias for `hpcflow make`.
3. `hfsub`, which is an alias for `hpcflow submit`.
4. `hfstat`, which is an alias for `hpcflow stat`.

Global settings are stored in a YAML file named `_config.yml`, which is stored in the `hpcflow` data directory. If it does not exist, the data directory is generated whenever `hpcflow` is run. By default, the data directory is placed under the user's home directory, as: `~/.hpcflow`, but this can be customised using the environment variable: `HPCFLOW_DATA_DIR`.

Inheritable YAML profile files can be added to the data directory under a sub-directory named `profiles`, i.e. at `~/.hpcflow/profiles/` if the data directory is located in the default place.

## Workflow parametrisation

There are four equivalent methods to generate a new `Workflow`, all of which are accessible via the API, and three of which are accessible via the CLI:

1. YAML profile files (API and CLI) -- *recommended*
2. JSON file (API and CLI)
3. JSON string (API and CLI)
4. Python `dict` (API only)

In general, instantiation of a `Workflow` requires two parts: a list of command groups, and a list of variable definitions that are referenced within the command groups. Each command group can have the following keys. More details can be found in the docstring of the `CommandGroup` constructor (`hpcflow.models.CommandGroup.__init__`).

|     Key      | Required |                                      Description                                       | 
| ------------ | -------- | -------------------------------------------------------------------------------------- |
| `commands`   | ✅        | List of commands to execute within one jobscript.                                      |
| `exec_order` | -        | Execution order of this command group.                                                 |
| `sub_order`  | -        | Indexes a command group that has sibling command groups with the same execution order. |
| `options`    | -        | Scheduler options to be passed directly to the jobscript.                                        |
| `directory` | - | The working directory for this command group. |
| `modules` | - | List of modules to load. |
| `job_array` | - | Whether this command group is submitted as a job array. |
| `profile_name` | - | If this command group was submitted as part of a job profile file, this is the `profile_name` of that job profile. |
| `profile_order` | - | If this command group was submitted as part of a job profile file, this is the `profile_order` of that job profile. |

### Workflow channels

If a command group shares its execution order index (`exec_order`) with other command groups, this set of command groups must be distinguished using the `sub_order` key. We use the term "channel" to denote a distinct `sub_order` index. So, if, for a given execution order, there are two command groups (with `sub_order`s of `0` and `1`), we say the Workflow has two channels at that execution order. The maximum number of channels in the Workflow is the number of channels for the initial command groups (i.e. with `exec_order=0`). Channels may merge together in subsequent command groups, but can never split apart.

TODO: rename `sub_order` key to `channel`? Is there any value in maintaining these two terms?

### Generating a `Workflow` with YAML profile files (recommended)

Using profile files is a convenient way to use `hpcflow` from the command line, since you can set up a hierarchy of common, reusable profile files that are inheritable. For instance, in your working directory (where, for example, your simulation input files reside), you may have two profiles: `run.yml` and `process.yml`. Perhaps the software you need to use to run the simulations (say, `DAMASK`) is also required for the processing of results. In this case you could add to your `hpcflow` data directory a common profile, say `damask.yml`, which includes things like loading the correct modules for DAMASK. Then in both of the profiles in your working directory (`run.yml` and `process.yml`), you can import/inherit from the common `DAMASK` profile by adding the YAML line: `inherit: damask`.

There are two additional settings that need to be specified when using YAML profile files instead of passing the workflow `dict` directly to the `Workflow` constructor. These are the `profile_name` and the `profile_order` parameters. These two settings can optionally be encoded in the file names of the profile files themselves. The format of profile file names is customisable, using the `profile_filename_fmt` key in the configuration file (`_config.yml`). When `hpcflow` searches for profiles in the working directory, the file names must match this format. All profiles must follow this format, including inheritable profiles that are contained in the `hpcflow` data directory. If either of `profile_name` or `profile_order` are note encoded in the profile file names, then they must be specified within the profile itself.

#### Specifying task ranges

Using the command-line interface, the command `hpcflow submit` (or `hfsub`) has an option `-t` (or `--task-ranges`), which allows us to specify which tasks should be submitted. If the `-t` option is omitted, then all tasks will be submitted. If the `-t` option is specified, then it must be a list of task ranges, where each task range has the format `n[-m[:s]]`, where `n` is the first task index to submit, `m` (optional) is the last, and `s` (optional) is the step size. The number of task ranges specified must be equal to the number of channels in the Workflow.

If multiple channels merge into one command group, the channel of the resulting command group will be the lowest of the channels of the parent command groups.

#### Submit-time versus run-time

There are two times of importance in the life cycle of a Workflow command group. To enable automation of the Workflow, all command groups are generally submitted at the same time, with holding rules that prevent their execution until (part of) the parent command group has completed.

#### Profile settings

|      Name       | Required |                                                    Description                                                     | Filename encodable? |
| --------------- | -------- | ------------------------------------------------------------------------------------------------------------------ | ------------------- |
| `profile_name`  | ️️✅    | A short name for the profile. For instance, `run` or `process`.                                                    | ✅                  |
| `profile_order` | ✅       | The relative execution order of the profile.                                                                       | ✅                  |
| `options`       | -        | Common scheduler options to be passed directly to the jobscripts for each command group belonging to this profile. | -                   |
| `directory`     | -        | The working directory for all command groups belonging to this profile.                                            | -                   |
| `variable_definitions` | - | Definitions for variables that appear in the command group commands. | - |
| `variable_scope` | - | Variable scope within the variable lookup file for this job profile  | - |
| `modules` | - | List of modules to load. |
| `job_array` | - | Whether this command group is submitted as a job array. |

Note that the `options`, `directory`, `modules` and `job_array` keys may be specified at the *profile* level in addition to the *command group* level. This is a useful convenience. If these keys are also specified within a command group, the command group keys take precedence.

The code associated with generating `Workflow`s from YAML profile files is mostly found in the `hpcflow.profiles.py` module.

### Generating a `Workflow` with a JSON file, JSON string or from a Python `dict`

Since JSON objects closely match the structure of Python `dict`s, these cases are all similar.

## Command-line interface

Here is a list of `hpcflow` (sub-)commands. Type `<command> --help` to show the options/arguments for a given sub-command.

|       Command       | Alias | Implemented |                                               Description                                                |
| ------------------- | ----- | ----------- | -------------------------------------------------------------------------------------------------------- |
| `hcpflow --help`    | -     | ✅          | Show the command-line help.                                                                              |
| `hpcflow --version` | -     | ️️️️️️✅    | Show the version of `hpcflow`.                                                                           |
| `hpcflow --info`    | -     | ❌           | Show information about the current `hpcflow` installation, including where the data directory is located. |
| `hpcflow --config`  | -     | ❌           | Show the contents and location of the `hpcflow` global configuration file.                               |
| `hpcflow make`      |`hfmake`| ✅           | Generate a Workflow. | 
| `hpcflow submit`    |`hfsub` | ✅           | Generate a Workflow if it doesn't exist and then submit (write jobscripts and execute) all command groups in the current working directory. | 
|`hpcflow install-example` | - | ❌           | Install an example set of profiles from the `examples` directory (files with the same name will be overwritten). |
| `hpcflow add-inputs` | - | ❌           | Add an example set of input files to be used with the example profile the `examples` directory. This involves merging `_variable_lookup.yml` and `_archive_locations.yml` from the example into the user's profile directory. | 
| `hpcflow write-cmd` | - | ✅           | Write the command file for a given jobscript. This script is invoked within jobscripts at execution-time and is not expected to be invoked by the user. The `write-cmd` process involves opening the JSON representation of the profile set and resolving variables for the set of commands within a given command group. |
| `hpcflow show-stats` | - | ❌           | Show statistics like CPU walltime for the profile set in the current directory. |
| `hpcflow clean` | - | ✅           | Remove all `hpcflow`-generated files from the current directory (use confirmation?). |
| `hpcflow stat` | `hfstat` | ❌           | Show status of running tasks and how many completed tasks within this directory. |
| `hpcflow kill` | `hfkill` | ❌           | Kill one or more jobscripts associated with a workflow. |
| `hpcflow archive` | - | ✅           | Archive the working directory of a given command group. |

### Commands that interact with the local database

- Of the above commands, the following interact with the local database:
    - `hpcflow make` (or `hfmake`)
    - `hpcflow submit` (or `hfsub`)
    - `hpcflow write-cmd`
    - `hpcflow show-stats`
    - `hpcflow stat` (or `hfstat`)
- Invoking any of these commands should therefore set up the relevant database connection.
- Only `hpcflow make` and `hpcflow submit` may invoke the `create_all(engine)` method, all other commands should fail if no database exists.

## Other notes:

- If using Dropbox archiving, make sure, if necessary, a proxy is correctly configured to allow the machine to communicate with the outside world.
- If using Windows, Windows must be at least version 1703, and switched to "Developer mode" (reason is creating a symlink; this could be disabled, but we also need developer mode for WSL I think.)

## Database schema

Here is a [link](https://app.sqldbm.com/MySQL/Share/zcKfw4XqIlAxd5gBe-VZtEGFrngIE8md_DYjF4jNYw0) to view the local SQLite database schema using the `sqldbm.com` web app.

## Glossary

**PyPI**: Python package index --- a public repository for Python packages and the default source location when packages are installed with `pip install <package_name>`.

**API**: Application programming interface --- an interface that allows other Python packages to conveniently interact with this package (`hpcflow`).

**CLI**: Command-line interface --- the interface that allows us to interact with `hpcflow` from the command line (i.e. shell).

**YAML**: YAML Ain't Markup Language (pronounced to rhyme with "camel") --- a human-readable data-serialisation language, commonly used for configuration files. It is a superset of JSON.

**JSON**: JavaScript Object Notation (pronounced like the male name "Jason") --- a human-readable data-serialisation language.
