"""`hpcflow.cli.py`

Module that exposes a command line interface for `hpcflow`.
"""

from pathlib import Path

import click

from hpcflow import __version__


@click.group()
@click.version_option(version=__version__)
def cli():
    pass


if __name__ == '__main__':
    cli()
