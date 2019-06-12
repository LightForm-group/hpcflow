"""`hpcflow.nesting.py`"""

from enum import Enum


class NestingType(Enum):
    """Class to represent the possible nesting types of a command group."""
    hold = 'hold'
    nest = 'nest'
