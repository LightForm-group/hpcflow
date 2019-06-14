"""`hpcflow.archive.archive.py`

This module contains a database model class for the archiving capabilities.

"""

import shutil
from datetime import datetime
from pathlib import Path
from pprint import pprint
from shutil import ignore_patterns
from time import sleep

from sqlalchemy import (Table, Column, Integer, DateTime, ForeignKey, String,
                        UniqueConstraint)
from sqlalchemy.orm import relationship, Session
from sqlalchemy.exc import IntegrityError, OperationalError

from hpcflow import CONFIG
from hpcflow.base_db import Base
from hpcflow.copytree import copytree_multi

archive_is_active = Table(
    'archive_is_active',
    Base.metadata,
    Column(
        'archive_id',
        Integer,
        ForeignKey('archive.id'),
        primary_key=True
    ),
    Column(
        'directory_value_id',
        Integer,
        ForeignKey('var_value.id'),
        primary_key=True
    ),
)


class Archive(Base):
    """Class to represent an archive location."""

    __tablename__ = 'archive'
    __table_args__ = (
        UniqueConstraint('path', 'host', name='archive_location'),
    )

    id_ = Column('id', Integer, primary_key=True)
    name = Column(String(255))
    path = Column(String(255))
    host = Column(String(255))

    command_groups = relationship('CommandGroup', back_populates='archive')
    directories_archiving = relationship(
        'VarValue',
        secondary=archive_is_active
    )

    def __init__(self, name, path, host=''):

        self.name = name
        self.path = path
        self.host = host

    def execute(self, directory_value, exclude):
        """Execute the archive process of a given working directory.

        Parameters
        ----------
        directory_value : VarValue
        exclude : list of str

        """

        root_dir = self.command_groups[0].workflow.directory
        src_dir = root_dir.joinpath(directory_value.value)
        dst_dir = Path(self.path).joinpath(directory_value.value)

        session = Session.object_session(self)

        wait_time = 10
        blocked = True
        while blocked:
            print('{} Archiving blocked...'.format(datetime.now()), flush=True)
            session.refresh(self)

            if directory_value in self.directories_archiving:
                sleep(wait_time)
            else:
                try:
                    self.directories_archiving.append(directory_value)
                    session.commit()
                    blocked = False

                except IntegrityError:
                    # Another process has already set `directories_archiving`
                    session.rollback()
                    sleep(wait_time)

                except OperationalError:
                    # Database is likely locked.
                    session.rollback()
                    sleep(wait_time)

                if not blocked:
                    # Need to ensure the block is released if copying fails:
                    try:
                        self._copy(src_dir, dst_dir, exclude)
                    except shutil.Error as err:
                        print('Archive copying error: {}'.format(err))

                    self.directories_archiving.remove(directory_value)
                    session.commit()

    def _copy(self, src_dir, dst_dir, exclude):
        """Do the actual copying.

        TODO: does copytree overwrite all files or just copy
        non-existing files?

        TODO: later (safely) copy the database to archive as well?

        """

        ignore = [CONFIG['hpcflow_directory']] + (exclude or [])

        if ignore:
            ignore_func = ignore_patterns(*ignore)
        else:
            ignore_func = None

        start = datetime.now()
        copytree_multi(str(src_dir), str(dst_dir), ignore=ignore_func)
        end = datetime.now()
        copy_seconds = (end - start).total_seconds()
        print('Archive took {} seconds'.format(copy_seconds), flush=True)
