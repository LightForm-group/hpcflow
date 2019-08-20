"""`hpcflow.archive.archive.py`

This module contains a database model class for the archiving capabilities.

"""

import shutil
import enum
import time
from datetime import datetime
from pathlib import Path
from pprint import pprint
from shutil import ignore_patterns
from time import sleep

from sqlalchemy import (Table, Column, Integer, DateTime, ForeignKey, String,
                        UniqueConstraint, Enum, Boolean)
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


class RootDirectoryName(enum.Enum):

    parent = 'parent'
    datetime = 'datetime'
    null = ''


class Archive(Base):
    """Class to represent an archive location."""

    __tablename__ = 'archive'
    __table_args__ = (
        UniqueConstraint('path', 'host', 'cloud_provider',
                         name='archive_location'),
    )

    id_ = Column('id', Integer, primary_key=True)
    name = Column(String(255))
    _path = Column('path', String(255))
    host = Column(String(255))
    cloud_provider = Column(Enum(CloudProvider))
    root_directory_name = Column(Enum(RootDirectoryName))
    root_directory_increment = Column(Boolean)

    command_groups = relationship('CommandGroup', back_populates='archive')
    directories_archiving = relationship('VarValue', secondary=archive_is_active)
    workflow = relationship('Workflow', back_populates='root_archive', uselist=False)

    def __init__(self, name, path, host='', cloud_provider='', root_directory_name='',
                 root_directory_increment=True):

        self.name = name
        self._path = path
        self.host = host
        self.cloud_provider = CloudProvider(cloud_provider)
        self.root_directory_name = RootDirectoryName(root_directory_name)
        self.root_directory_increment = root_directory_increment

        if not self.check_exists(self.path):
            raise ValueError('Archive path "{}" does not exist.'.format(self.path))

    @property
    def path(self):
        return Path(self._path)

    def check_exists(self, directory):
        'Check if a given directory exists on the Archive.'
        if not self.host:
            if self.cloud_provider != CloudProvider.null:
                exists = self.cloud_provider.check_exists(directory)
            else:
                exists = directory.is_dir()
        else:
            raise NotImplementedError()

        return exists

    def get_archive_dir(self, workflow):
        'This should be called once per unique workflow Archive.'

        if self.root_directory_name != RootDirectoryName.null:

            if self.root_directory_name == RootDirectoryName.parent:
                archive_dir = workflow.directory.stem
            elif self.root_directory_name == RootDirectoryName.datetime:
                archive_dir = time.strftime('%Y-%m-%d-%H%M')

            if self.check_exists(self.path.joinpath(archive_dir)):

                if self.root_directory_increment:
                    count = 0
                    max_count = 10
                    while self.check_exists(self.path.joinpath(archive_dir)):
                        count += 1
                        if count > max_count:
                            msg = ('Maximum iteration reached ({}) in searching for '
                                   'available archive directory.'.format(max_count))
                            raise RuntimeError(msg)
                        archive_dir = archive_dir + '_1'
                else:
                    msg = ('Archive directory "{}" already exists.')
                    raise ValueError(msg.format(archive_dir))

        else:
            archive_dir = ''

        return archive_dir

    def execute(self, exclude, archive_dir):
        """Execute the archive process with no lock. Used for root archive.

        Parameters
        ----------
        exclude : list of str

        """

        self._copy(self.workflow.directory, self.path.joinpath(archive_dir), exclude)

    def execute_with_lock(self, directory_value, exclude, archive_dir):
        """Execute the archive process of a given working directory.

        Parameters
        ----------
        directory_value : VarValue
        exclude : list of str

        """

        root_dir = self.command_groups[0].workflow.directory
        src_dir = root_dir.joinpath(directory_value.value)
        dst_dir = self.path.joinpath(archive_dir, directory_value.value)

        session = Session.object_session(self)

        sleep_time = 10
        context = 'Archive.execute_with_lock'
        block_msg = ('{{}} {}: Archiving blocked. Sleeping for {} '
                     'seconds'.format(context, sleep_time))
        unblock_msg = ('{{}} {}: Archiving available. Archiving.'.format(context))

        blocked = True
        while blocked:

            session.refresh(self)

            if directory_value in self.directories_archiving:
                print(block_msg.format(datetime.now()), flush=True)
                sleep(sleep_time)
            else:
                try:
                    self.directories_archiving.append(directory_value)
                    session.commit()
                    blocked = False

                except IntegrityError:
                    # Another process has already set `directories_archiving`
                    session.rollback()
                    print(block_msg.format(datetime.now()), flush=True)
                    sleep(sleep_time)

                except OperationalError:
                    # Database is likely locked.
                    session.rollback()
                    print(block_msg.format(datetime.now()), flush=True)
                    sleep(sleep_time)

                if not blocked:
                    print(unblock_msg.format(datetime.now()), flush=True)
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

            if self.cloud_provider != CloudProvider.null:
                try:
                    self.cloud_provider.upload(src_dir, dst_dir, ignore)
                except (CloudProviderError, CloudCredentialsError, ArchiveError) as err:
                    raise ArchiveError(err)
            else:
                if ignore:
                    ignore_func = ignore_patterns(*ignore)
                else:
                    ignore_func = None
                try:
                    copytree_multi(str(src_dir), str(dst_dir), ignore=ignore_func)
                except shutil.Error as err:
                    raise ArchiveError(err)

        except ArchiveError as err:
            print('Archive copying error: {}'.format(err))

        end = datetime.now()
        copy_seconds = (end - start).total_seconds()
        print('Archive to "{}" took {} seconds'.format(
            self.name, copy_seconds), flush=True)
