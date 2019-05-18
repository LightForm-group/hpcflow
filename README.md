## Installation

### Package installation

`hpcflow` can be installed from PyPI as follows:

`pip install --user hpcflow`

### Set up

Some console entry points (scripts/executables) will be added to your system as part of the package installation. These are:

1. `hpcflow`, and all of its sub-commands (found by typing `hpcflow --help` at the command line)
2. `submit`, which is an alias of `hpcflow submit`.
3. TODO: `hfstat`, which is an alias of `hpcflow hfstat`.

Global settings are stored in a YAML file named `_config.yml`, which is stored in the `hpcflow` data directory. If it does not exist, the data directory is generated whenever `hpcflow` is run. By default the data directory is placed under the user's home directory, as: `~/.hpcflow`, but this can be customised using the environment variable: `HPCFLOW_DATA_DIR`.

Inheritable YAML profile files can be added to the data directory under a sub-directory named `profiles`, i.e. at `~/.hpcflow/profiles/` if the data directory is located in the default place.

## Making a new `Workflow`

There are four equivalent methods to generate a new `Workflow`, all of which are accessible via the API, and three of which are accessible via the CLI:

1. YAML profile files (API and CLI)
2. JSON file (API and CLI)
3. JSON string (API and CLI)
4. Python `dict` (API only)

### Using profile files to make a new `Workflow`

Using profile files is a convenient way to use `hpcflow` from the command line, since you can set up a hierarchy of common, reusable profile files that are inheritable. For instance, in your working directory (where, for example, your simulation input files reside), you may have two profiles: `run.yml` and `process.yml`. Perhaps the software you need to use to run the simulations (say, `DAMASK`) is also required for the processing of results. In this case you could add to your `hpcflow` data directory a common profile, say `damask.yml`, which includes things like loading the correct modules for DAMASK. Then in both of the profiles in your working directory (`run.yml` and `process.yml`), you can import/inherit from the common `DAMASK` profile by adding the YAML line: `inherit: damask`.

The code associated with generating `Workflow`s from YAML profile files is mostly found in the `hpcflow.profiles.py` module.


## Command-line interface

Here is a list of `hpcflow` (sub-)commands. Type `hpcflow <command> --help` to show the options/arguments for a given sub-command.

|       Command       | Alias | Implemented |                                               Description                                                |
| ------------------- | ----- | ----------- | -------------------------------------------------------------------------------------------------------- |
| `hcpflow --help`    | -     | ✔️          | Show the command-line help.                                                                              |
| `hpcflow --version` | -     | ️️️️️️✔️    | Show the version of `hpcflow`.                                                                           |
| `hpcflow --info`    | -     | ❌           | Show information about the current `hpcflow` installation, including where the data directory is located. |
| `hpcflow --config`  | -     | ❌           | Show the contents and location of the `hpcflow` global configuration file.                               |
| `hpcflow make`      |`hfmake`| ❌           | Generate a Workflow. | 
| `hpcflow submit`    |`hfsub` | ❌           | Generate a Workflow if it doesn't exist and then submit (write jobscripts and execute) all command groups in the current working directory. | 
|`hpcflow install-example` | - | ❌           | Install an example set of profiles from the `examples` directory (files with the same name will be overwritten). |
| `hpcflow add-inputs` | - | ❌           | Add an example set of input files to be used with the example profile the `examples` directory. This involves merging `_variable_lookup.yml` and `_archive_locations.yml` from the example into the user's profile directory. | 
| `hpcflow write-cmd` | - | ❌           | Write the command file for a given jobscript. This script is invoked within jobscripts at execution-time and is not expected to be invoked by the user. The `write-cmd` process involves opening the JSON representation of the profile set and resolving variables for the set of commands within a given command group. |
| `hpcflow show-stats` | - | ❌           | Show statistics like CPU walltime for the profile set in the current directory. |
| `hpcflow clean` | - | ❌           | Remove all `hpcflow`-generated files from the current directory (use confirmation?). |
| `hpcflow stat` | `hfstat` | ❌           | Show status of running tasks and how many completed tasks within this directory. |

### Commands that interact with the local database

- Of the above commands, the following interact with the local database:
    - `hpcflow submit`
    - `hpcflow write-cmd`
    - `hpcflow show-stats`
    - `hpcflow hfstat`
- Invoking any of these commands should therefore set up the relevant database connection.
- Only `hpcflow submit` should invoke the `create_all(engine)` method, all other commands should fail if no database exists. 

*[PyPI]: Python package index --- a public repository for Python packages.
*[API]: Application programming interface --- an interface that allows other Python packages to conveniently interact with this package (`hpcflow`).
*[CLI]: Command-line interface --- the interface that allows us to interact with `hpcflow` from the command line (i.e. shell).
*[YAML]: YAML Ain't Markup Language (pronounced to rhyme with "camel") --- a human-readable data-serialisation language, commonly used for configuration files. It is a superset of JSON.
*[JSON]: JavaScript Object Notation (pronounced like the male name "Jason") --- a human-readable data-serialisation language.
