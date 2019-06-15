"""`hpcflow.archive.archive.py`

This module contains a database model class for the archiving capabilities.

"""

import shutil
import enum
from datetime import datetime
from pathlib import Path
from pprint import pprint
from shutil import ignore_patterns
from time import sleep

from sqlalchemy import (Table, Column, Integer, DateTime, ForeignKey, String,
                        UniqueConstraint, Enum)
from sqlalchemy.orm import relationship, Session
from sqlalchemy.exc import IntegrityError, OperationalError

from hpcflow import CONFIG
from hpcflow.archive.cloud.cloud import CloudProvider
from hpcflow.archive.cloud.errors import CloudProviderError, CloudCredentialsError
from hpcflow.archive.errors import ArchiveError
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
    cloud_provider = Column(Enum(CloudProvider))

    command_groups = relationship('CommandGroup', back_populates='archive')
    directories_archiving = relationship(
        'VarValue',
        secondary=archive_is_active
    )
    workflow = relationship('Workflow', back_populates='root_archive',
                            uselist=False)

    def __init__(self, name, path, host='', cloud_provider=''):

        self.name = name
        self.path = path
        self.host = host
        self.cloud_provider = CloudProvider(cloud_provider)

    def execute(self, exclude):
        """Execute the archive process with no lock.

        Parameters
        ----------
        exclude : list of str

        """

        src_dir = self.workflow.directory
        dst_dir = self.path
        self._copy(src_dir, dst_dir, exclude)

    def execute_with_lock(self, directory_value, exclude):
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
                    self._copy(src_dir, dst_dir, exclude)
                    self.directories_archiving.remove(directory_value)
                    session.commit()

    def _copy(self, src_dir, dst_dir, exclude):
        """Do the actual copying.

        Need to ensure this function catches all exceptions, so the block is
        released if copying fails.

        TODO: does copytree overwrite all files or just copy
        non-existing files?

        TODO: later (safely) copy the database to archive as well?

        """

        ignore = [CONFIG['hpcflow_directory']] + (exclude or [])
        start = datetime.now()

        try:

            if self.cloud_provider != 'null':
                try:
                    self.cloud_provider.upload(src_dir, dst_dir, ignore)
                except (CloudProviderError, CloudCredentialsError,
                        ArchiveError) as err:
                    raise ArchiveError(err)
            else:
                if ignore:
                    ignore_func = ignore_patterns(*ignore)
                else:
                    ignore_func = None
                try:
                    copytree_multi(str(src_dir), str(
                        dst_dir), ignore=ignore_func)
                except shutil.Error as err:
                    raise ArchiveError(err)

        except ArchiveError as err:
            print('Archive copying error: {}'.format(err))

        end = datetime.now()
        copy_seconds = (end - start).total_seconds()
        print('Archive to "{}" took {} seconds'.format(
            self.name, copy_seconds), flush=True)
