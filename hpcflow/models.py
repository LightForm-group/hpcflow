"""`hpcflow.models.py`"""


import re
import os
import enum
from datetime import datetime
from math import ceil, floor
from pathlib import Path
from pprint import pprint
from subprocess import run, PIPE
from time import sleep

from sqlalchemy import (Column, Integer, DateTime, JSON, ForeignKey, Boolean,
                        Enum, String, select, Float)
from sqlalchemy.orm import relationship, deferred, Session, reconstructor
from sqlalchemy.exc import IntegrityError, OperationalError

from hpcflow.config import Config as CONFIG
from hpcflow._version import __version__
from hpcflow.archive.archive import Archive, TaskArchiveStatus
from hpcflow.base_db import Base
from hpcflow.archive.cloud.cloud import CloudProvider
from hpcflow.nesting import NestingType
from hpcflow.scheduler import SunGridEngine
from hpcflow.utils import coerce_same_length, zeropad, format_time_delta, get_random_hex, datetime_to_dict, timedelta_to_dict
from hpcflow.validation import validate_task_multiplicity
from hpcflow.variables import (
    select_cmd_group_var_names, select_cmd_group_var_definitions,
    extract_variable_names, resolve_variable_values, UnresolvedVariableError
)

SCHEDULER_MAP = {
    'sge': SunGridEngine,
}


class IterationStatus(enum.Enum):

    pending = 'pending'
    active = 'active'
    complete = 'complete'


class Workflow(Base):
    """Class to represent a Workflow."""

    __tablename__ = 'workflow'

    id_ = Column('id', Integer, primary_key=True)
    create_time = Column(DateTime)
    pre_commands = Column(JSON)
    _directory = Column('directory', String(255))
    root_archive_id = Column(Integer, ForeignKey('archive.id'), nullable=True)
    root_archive_excludes = Column(JSON, nullable=True)
    root_archive_directory = Column(String(255), nullable=True)
    _profile_files = Column('profile_files', JSON, nullable=True)
    loop = Column(JSON)
    parallel_modes = Column(JSON, nullable=True)

    command_groups = relationship(
        'CommandGroup',
        back_populates='workflow',
        order_by='CommandGroup.exec_order',
    )
    submissions = relationship('Submission', back_populates='workflow')
    variable_definitions = relationship('VarDefinition', back_populates='workflow')
    root_archive = relationship('Archive', back_populates='workflow', uselist=False)
    iterations = relationship(
        'Iteration', back_populates='workflow', order_by='Iteration.order_id')

    def __init__(self, directory, command_groups, var_definitions=None,
                 pre_commands=None, archives=None, root_archive_idx=None,
                 root_archive_excludes=None, profile_files=None, loop=None,
                 parallel_modes=None):
        """Method to initialise a new Workflow.

        Parameters
        ----------
        directory : str or Path
            Directory in which the Workflow resides.
        command_groups : list of dict
            List of dictionaries that each represent a command group.
        var_definitions : dict, optional
            Dictionary whose keys are variable names and values are
            dictionaries that define variable definitions. By default, set to
            `None`, in which case it is assumed there are no variable
            references in any of the command groups.
        pre_commands : list of str
            List of commands to execute on creation of the Workflow.
        archives : list of dict
            List of dicts representing archive locations. Each dict in
            `command_groups` may contain keys `archive_idx` (which is an
            index into `archives`) and `archive_excludes` (which is a list
            of glob patterns to ignore when archiving). Each item in `archives`
            contains the following keys:
                name : str
                host : str
                path : str
        root_archive_idx : int
            Index into `archives` that sets the root archive for the workflow.
        root_archive_excludes : list of str
            File patterns to exclude from the root archive.
        profile_files : list of Path, optional
            If specified, the list of absolute file paths to the profile files used to
            generate this workflow.
        loop : dict, optional
            If specified, keys are:
                max_iterations : int
                    Maximum number of loop iterations to submit.
                groups : list of int, optional
                    Which command groups to include in iterations beyond the first. If not
                    specified, all command groups are included in the loop.
        parallel_modes : dict, optional
            If specified, (case-insensitive) keys are one or more of: 'MPI', 'OpenMP'.
            Each is a dict with allowed keys:
                env : list of str
                    Environment set up required for a given parallel mode.
                command : str
                    Command to prepend to any command group commands that use this
                    parallel mode.

        """

        if loop is None:
            loop = {
                'max_iterations': 1,
                'groups': [],
            }

        # Command group directories must be stored internally as variables:
        for idx, i in enumerate(command_groups):

            dir_var_value = '.'

            if 'directory' in i:

                var_names = extract_variable_names(
                    i['directory'], CONFIG.get('variable_delimiters'))
                if len(var_names) > 1:
                    raise NotImplementedError()
                elif not var_names:
                    # Value is set but is not a variable
                    dir_var_value = i['directory'] or dir_var_value
                else:
                    # Value is already a variable; no action.
                    continue

            dir_var_defn_name = CONFIG.get('default_cmd_group_dir_var_name')

            command_groups[idx]['directory'] = '{1:}{0:}{2:}'.format(
                dir_var_defn_name,
                *CONFIG.get('variable_delimiters')
            )

            # Add new variable definition:
            var_definitions.update({
                dir_var_defn_name: {
                    'value': dir_var_value,
                }
            })

        self._directory = str(directory)
        self.profile_files = [i.relative_to(self.directory) for i in profile_files]
        self.create_time = datetime.now()
        self.pre_commands = pre_commands
        self.variable_definitions = [
            VarDefinition(name=k, **v) for k, v in var_definitions.items()
        ]

        # Generate Archive objects:
        archive_objs = []
        archive_dir_names = []
        if archives:
            for i in archives:
                arch_i = Archive(**i)
                archive_objs.append(arch_i)
                archive_dir_names.append(arch_i.get_archive_dir(self))

        if root_archive_idx is not None:
            self.root_archive = archive_objs[root_archive_idx]
            self.root_archive_excludes = root_archive_excludes
            self.root_archive_directory = archive_dir_names[root_archive_idx]

        cmd_groups = []
        for i in command_groups:

            dir_var_name = extract_variable_names(
                i['directory'], CONFIG.get('variable_delimiters'))[0]

            dir_var_defn = [i for i in self.variable_definitions
                            if i.name == dir_var_name][0]

            i.pop('directory')
            i.update({
                'directory_var': dir_var_defn,
            })
            arch_idx = i.pop('archive_idx', None)
            if arch_idx is not None:
                i.update({
                    'archive': archive_objs[arch_idx],
                    'archive_directory': archive_dir_names[arch_idx],
                })
            cmd_groups.append(CommandGroup(**i))

        self.command_groups = cmd_groups
        self.parallel_modes = parallel_modes

        self.loop = loop
        for i in range(self.loop['max_iterations']):
            self.iterations.append(Iteration(i))

        self.validate(archive_objs)
        self._execute_pre_commands()
        self.do_root_archive()

    def __repr__(self):
        out = ('{}('
               'id={}, '
               'directory={}, '
               'pre_commands={}, '
               'root_archive_id={}, '
               'loop={}'
               ')').format(
            self.__class__.__name__,
            self.id_,
            self.directory,
            self.pre_commands,
            self.root_archive_id,
            self.loop,
        )

        return out

    def get_variable_definition_by_name(self, variable_name):
        """Get the VarDefintion object using the variable name."""

        for i in self.variable_definitions:
            if i.name == variable_name:
                return i

        msg = ('Cannot find variable definition with '
               'name "{}"'.format(variable_name))
        raise ValueError(msg)

    @property
    def first_iteration(self):
        return self.iterations[0]

    @property
    def profile_files(self):
        if self._profile_files:
            return [Path(i) for i in self._profile_files]
        else:
            return []

    @profile_files.setter
    def profile_files(self, profile_files):
        if profile_files:
            self._profile_files = [str(i) for i in profile_files]

    @property
    def has_alternate_scratch(self):
        return bool(self.all_alternate_scratch)

    @property
    def all_alternate_scratch(self):
        out = list(set([i.alternate_scratch for i in self.command_groups
                        if i.alternate_scratch]))
        return out

    @property
    def directory(self):
        return Path(self._directory)

    def validate(self, archive_objs):
        cmd_group_list = []
        for i in self.command_groups:
            cmd_group_list.append({
                'is_job_array': i.is_job_array,
                'exec_order': i.exec_order,
                'nesting': i.nesting,
            })

        err = '[Workflow instantiation error]'
        cmd_group_list = validate_task_multiplicity(cmd_group_list, err)

        for i_idx, i in enumerate(cmd_group_list):
            cmd_group = self.command_groups[i_idx]
            cmd_group.is_job_array = i['is_job_array']
            cmd_group.exec_order = i['exec_order']
            cmd_group.nesting = i['nesting']

        # If using an Archive with a cloud provider, check access:
        for i in archive_objs:
            if i.cloud_provider != CloudProvider.null:
                msg = f'Checking access to cloud storage ({i.name})...'
                print(msg, end='', flush=True)
                i.cloud_provider.check_access()

    def add_submission(self, project, task_range=None):
        """Add a new submission to this Workflow.

        Parameters
        ----------
        project : Project
        task_ranges : list, optional
            If specified, must be a list of length equal to the number of
            channels in the Workflow. Each list element specifies which tasks
            to submit from each Workflow channel. Each element may be either a
            list, a string "all", or `None`. If an element is a string "all",
            all tasks within the specified channel will be submitted. If an
            element is `None`, no tasks within the specified channel will be
            submitted. If an element is a list, it must have either two or
            three elements; if it has two elements, these signify the first and
            last tasks, inclusively, to submit from that channel. By default,
            the task step size is one, but this can be chosen as a third list
            entry. By default, set to `None`, in which case all tasks from all
            channels are included.

        Notes
        -----
        We are temporarily restricting the number of channels to 1, since
        supporting multiple channels requires some more technical work. This
        restriction is enforced in the `validation.validate_task_multiplicity`
        function.

        Examples
        --------
        Submit all tasks from all channels:
        >>> workflow.add_submission()

        Submit tasks 1, 2, 3, 4 and 5 from the first and only channel:
        >>> workflow.add_submission([[1, 5]])

        Submit tasks 1 and 3 from the first channel, and tasks 2, 3 and 4 from
        the second channel:
        >>> workflow.add_submission([[1, 4, 2], [2, 4]])

        Submit all tasks from the first channel, and tasks 2 and 7 from the
        second channel:
        >>> workflow.add_submission(['all', (2, 7, 5)])

        Submit all tasks from the first channel and no tasks from the second
        channel:
        >>> workflow.add_submission(['all', None])


        What to do:
        -----------

        0.  Firstly, resolve variable values for the first command group.
        1.  Need to identify which command groups must have their
            var_multiplicity resolved at submit time, and raise if it cannot
            be done. For `is_job_array=False` command groups, var_multiplicity
            does not need to be known at submit-time, since the number of
            output tasks will be known (either one [for `nesting=hold`], or
            equal to number of input tasks [for `nesting=None`]).
        2.  To do this, organise command groups into scheduler groups,
            which are delineated by command groups with `nesting=hold`.
        3.  For each scheduler group, go through the command groups in order
            and resolve the `var_multiplicity` if it is required. This is not
            the same as actually resolving the variable values. And we don't
            need to do that at submit-time, except for the very first command
            group! (Or rather, since submit-time and run-time coincide for
            the first command group, we have the *opportunity* to resolve
            variable values for the first command group; in general, variable
            values in a given command group may depend on the commands run in
            a previous command group, so this cannot be done.)

        """

        # print('Workflow.add_submission: task_range: {}'.format(task_range), flush=True)

        submission = Submission(self, task_range)  # Generate CGSs and Tasks
        submission.write_submit_dirs(project.hf_dir)
        js_paths = submission.write_jobscripts(project.hf_dir)
        submission.submit_jobscripts(js_paths)

        return submission

    def get_num_channels(self, exec_order=0):
        """Get the number of command groups with a given execution order.

        Parameters
        ----------
        exec_order : int, optional
            The execution order at which to count command groups.

        Returns
        -------
        num_channels : int
            The number of command groups at the given execution order.

        """

        num_channels = 0
        for i in self.command_groups:
            if i.exec_order == exec_order:
                num_channels += 1

        return num_channels

    def _validate_task_ranges(self, task_ranges):
        """Validate task ranges.

        Parameters
        ----------
        task_ranges : list

        Returns
        -------
        task_ranges_valid : list

        """

        # Check length equal to num_channels:
        if len(task_ranges) != self.get_num_channels():
            msg = ('The number of task ranges specified must be equal to the '
                   'number of channels in the workflow, which is {}, but {} '
                   'task ranges were specified.')
            raise ValueError(msg.format(self.get_num_channels(),
                                        len(task_ranges)))

        task_range_msg = (
            'Each task range must be specified as either a list with two or '
            'three elements, representing the first and last task and '
            '(optionally) the step size, `None`, or the string "all".'
        )

        task_ranges_valid = []
        for i in task_ranges:

            # Validate:
            if isinstance(i, list):
                if len(i) not in [2, 3]:
                    raise ValueError(task_range_msg)
            elif i not in ['all', None]:
                raise ValueError(task_range_msg)

            task_range_i = i
            if i == 'all':
                # Replace "all" with [n, m, s]
                task_range_i = [1, -1, 1]

            elif isinstance(i, list) and len(i) == 2:
                # Add step size of 1:
                task_range_i += [1]

            if task_range_i[1] != -1:
                # For known number of tasks, check m >= n >= 1:
                if task_range_i[0] < 1:
                    msg = 'Starting task, `n`, must be >= 1.'
                    raise ValueError(msg)
                if task_range_i[1] < task_range_i[0]:
                    msg = 'Ending task, `m`, must be >= starting task, `n`.'
                    raise ValueError(msg)

            task_ranges_valid.append(task_range_i)

        return task_ranges_valid

    def _execute_pre_commands(self):

        for i in self.pre_commands:

            proc = run(i, shell=True, stdout=PIPE, stderr=PIPE)
            pre_cmd_out = proc.stdout.decode()
            pre_cmd_err = proc.stderr.decode()

    def do_root_archive(self):
        """Copy the workflow directory to the root archive location."""

        if self.root_archive:
            self.root_archive.execute(self.root_archive_excludes,
                                      self.root_archive_directory)

    def get_stats(self, jsonable=True, datetime_dicts=False):
        """Get task statistics for this workflow."""
        out = {
            'workflow_id': self.id_,
            'submissions': [i.get_stats(jsonable=jsonable, datetime_dicts=datetime_dicts)
                            for i in self.submissions]
        }
        return out

    def kill_active(self):
        """Kill any active scheduled jobs associated with the workflow."""

        kill_scheduler_ids = []
        for sub in self.submissions:
            for cg_sub in sub.command_group_submissions:
                for iteration in self.iterations:
                    cg_sub_iter = cg_sub.get_command_group_submission_iteration(iteration)
                    if cg_sub_iter:
                        if cg_sub_iter.scheduler_job_id is not None:
                            kill_scheduler_ids.append(cg_sub_iter.scheduler_job_id)
                        if cg_sub_iter.scheduler_stats_job_id is not None:
                            kill_scheduler_ids.append(cg_sub_iter.scheduler_stats_job_id)

        print('Need to kill: {}'.format(kill_scheduler_ids))
        del_cmd = ['qdel'] + [str(i) for i in kill_scheduler_ids]
        proc = run(del_cmd, stdout=PIPE, stderr=PIPE)
        qdel_out = proc.stdout.decode()
        qdel_err = proc.stderr.decode()
        print(qdel_out)


class CommandGroup(Base):
    """Class to represent a command group, which is roughly translated into a
    job script."""

    __tablename__ = 'command_group'

    id_ = Column('id', Integer, primary_key=True)
    workflow_id = Column(Integer, ForeignKey('workflow.id'))
    directory_variable_id = Column(Integer, ForeignKey('var_definition.id'))
    archive_id = Column(Integer, ForeignKey('archive.id'), nullable=True)

    name = Column(String(255), nullable=True)
    stats_name = Column(String(255), nullable=True)
    commands = Column(JSON)
    is_job_array = Column(Boolean)
    exec_order = Column(Integer)
    nesting = Column(Enum(NestingType), nullable=True)
    environment = Column(JSON, nullable=True)
    _scheduler = Column('scheduler', JSON)
    profile_name = Column(String(255), nullable=True)
    profile_order = Column(Integer, nullable=True)
    archive_excludes = Column(JSON, nullable=True)
    archive_directory = Column(String(255), nullable=True)
    _alternate_scratch = Column('alternate_scratch', String(255), nullable=True)
    stats = Column(Boolean)

    archive = relationship('Archive', back_populates='command_groups')
    workflow = relationship('Workflow', back_populates='command_groups')
    command_group_submissions = relationship('CommandGroupSubmission',
                                             back_populates='command_group')

    directory_variable = relationship('VarDefinition')

    _scheduler_obj = None

    def __repr__(self):
        out = (
            '{}('
            'commands={!r}, '
            'is_job_array={!r}, '
            'nesting={!r}'
            ')'
        ).format(
            self.__class__.__name__,
            self.commands,
            self.is_job_array,
            self.nesting,
        )
        return out

    def __init__(self, commands, directory_var, is_job_array=True,
                 exec_order=None, nesting=None, environment=None, scheduler=None,
                 profile_name=None, profile_order=None, archive=None,
                 archive_excludes=None, archive_directory=None, alternate_scratch=None,
                 stats=None, name=None, stats_name=None):
        """Method to initialise a new CommandGroup.

        Parameters
        ----------
        commands : list of dict
            List of dicts containing commands to execute.
        directory_var : VarDefinition
            The working directory for this command group. TODO...
        is_job_array : bool, optional
            If True, the command group is executed as a job array. True by
            default.
        exec_order : int, optional
            Execution order of this command relative to other command groups in
            the Workflow. By default, `None`.
        nesting : str, optional
            Either "nest" or "hold". This determines how the task multiplicity
            of this command group joins together with the task multiplicity of
            the previous command group (i.e. the command group with the lower
            execution order as determined by `exec_order`). If "nest", each
            task from the previous command group, once completed, will fork
            into multiple tasks in the current command group. If "hold", all
            tasks in the current command group will only begin once all tasks
            in the previous command group have completed. If `None`, the number
            of tasks in the previous and current command groups must match,
            since a given task in the current command group will only begin
            once its corresponding task in the previous command group has
            completed. By default, set to `None`.
        environment : list of str, optional
            List of commands to be run to set up the environment for the command group. By
            default set to `None`.
        scheduler : dict, optional
            Scheduler type and options to be passed directly to the scheduler. By default,
            `None`, in which case the DirectExecution scheduler is used and no additional
            options are passed.
        profile_name : str, optional
            If the command group was generated as part of a job profile file,
            the profile name should be passed here.
        profile_order : int, optional
            If the command group was generated as part of a job profile file,
            the profile order should be passed here.
        archive : Archive, optional
            The Archive object associated with this command group.
        archive_excludes : list of str
            List of glob patterns representing files that should be excluding
            when archiving this command group.
        archive_directory : str or Path, optional
            Name of the directory in which the archive for this command group will reside.
        alternate_scratch : str, optional
            Location of alternate scratch in which to run commands.

        TODO: document how `nesting` interacts with `is_job_array`.

        """

        self.commands = commands
        self.is_job_array = is_job_array
        self.exec_order = exec_order
        self.nesting = nesting
        self.environment = environment
        self.scheduler = scheduler
        self.directory_variable = directory_var
        self.profile_name = profile_name
        self.profile_order = profile_order
        self.stats = stats
        self.name = name
        self.stats_name = stats_name

        self.archive = archive
        self.archive_excludes = archive_excludes
        self.archive_directory = archive_directory

        self._alternate_scratch = alternate_scratch

        self.validate()

    @reconstructor
    def init_on_load(self):
        self.scheduler = self._scheduler

    def validate(self):

        # Check at least one command:
        if not self.commands:
            msg = 'At least one command must be specified.'
            raise ValueError(msg)

        self.nesting = NestingType[self.nesting] if self.nesting else None

        # Check alternate scratch exists
        if self.alternate_scratch:
            if not self.alternate_scratch.is_dir():
                msg = 'Alternate scratch "{}" is not an existing directory.'
                raise ValueError(msg.format(self.alternate_scratch))

    @staticmethod
    def get_command_lines(commands):
        """Get all lines in the commands list."""
        out = []
        for i in commands:
            if 'line' in i:
                out.append(i['line'])
            elif 'subshell' in i:
                out.extend(CommandGroup.get_command_lines(i['subshell']))
        return out

    @property
    def scheduler(self):
        return self._scheduler_obj

    @scheduler.setter
    def scheduler(self, scheduler):

        if 'name' not in scheduler:
            msg = 'Scheduler must have a name that is one of: {}'
            raise ValueError(msg.format(list(SCHEDULER_MAP.keys())))

        sch_name = scheduler['name']
        if sch_name not in SCHEDULER_MAP.keys():
            msg = 'Scheduler "{}" is not known.'.format(scheduler)
            raise ValueError(msg)

        sch_class = SCHEDULER_MAP[sch_name]
        self._scheduler_obj = sch_class(
            options=scheduler['options'],
            output_dir=scheduler['output_dir'],
            error_dir=scheduler['error_dir'],
        )
        self._scheduler = scheduler

    @property
    def alternate_scratch(self):
        if self._alternate_scratch:
            return Path(self._alternate_scratch)
        else:
            return None

    @property
    def variable_names(self):
        """Get those variable names associated with this command group."""

        var_names = select_cmd_group_var_names(
            self.get_command_lines(self.commands),
            self.directory_variable.value
        )
        return var_names

    @property
    def variable_definitions(self):
        """Get those variable definitions associated with this command group,
        excluding those that appear embedded within other variables."""

        var_names = self.variable_names
        var_defns = []
        for i in self.workflow.variable_definitions:
            if i.name in var_names:
                var_defns.append(i)

        return var_defns

    @property
    def variable_definitions_recursive(self):
        """Get those variable definitions associated with this command group,
        including those that appear embedded within other variables."""

        var_defns_dict = {
            i.name: {
                'data': i.data,
                'file_regex': i.file_regex,
                'file_contents': i.file_contents,
                'value': i.value,
            }
            for i in self.workflow.variable_definitions
        }

        cmd_group_var_defns = select_cmd_group_var_definitions(
            var_defns_dict,
            self.get_command_lines(self.commands),
            self.directory_variable.value,
        )

        var_defns = [
            i for i in self.workflow.variable_definitions
            if i.name in cmd_group_var_defns
        ]

        return var_defns


class VarDefinition(Base):
    """Class to represent a variable definition."""

    __tablename__ = 'var_definition'

    id_ = Column('id', Integer, primary_key=True)
    workflow_id = Column('workflow_id', Integer, ForeignKey('workflow.id'))

    name = Column(String(255))
    data = Column(JSON, nullable=True)
    file_regex = Column(JSON, nullable=True)
    file_contents = Column(JSON, nullable=True)
    value = Column(String(255), nullable=True)

    workflow = relationship('Workflow', back_populates='variable_definitions')
    variable_values = relationship(
        'VarValue',
        back_populates='variable_definition',
        order_by='VarValue.order_id',
    )

    def __repr__(self):
        out = ('{}('
               'name={!r}, '
               'data={!r}, '
               'file_regex={!r}, '
               'value={!r}'
               ')').format(
                   self.__class__.__name__,
                   self.name,
                   self.data,
                   self.file_regex,
                   self.value,
        )
        return out

    def __init__(self, name, data=None, file_regex=None, value=None, file_contents=None):

        self.name = name
        self.data = data
        self.file_regex = file_regex
        self.file_contents = file_contents
        self.value = value

    def is_base_variable(self):
        """Check if the variable depends on any other variables."""

        if extract_variable_names(self.value,
                                  CONFIG.get('variable_delimiters')):
            return False
        else:
            return True

    def get_dependent_variable_names(self):
        """Get the names of variables on which this variable depends."""
        return extract_variable_names(self.value,
                                      CONFIG.get('variable_delimiters'))

    def get_multiplicity(self, submission):
        """Get the value multiplicity of this variable for a given
        submission.

        TODO: this should first try to get multiplicity from values (as a
        function of cmd group directory?)

        """

        # First check if the variable is resolved.

        var_values = {}
        for i in self.variable_values:
            if i.submission == submission:
                if i.directory_value.value not in var_values:
                    var_values.update({i.directory_value.value: []})
                var_values[i.directory_value.value].append(i)

        var_lengths = {}
        for directory_path, var_vals in var_values.items():

            if var_vals:
                var_length = len(var_vals)

            else:
                var_length = None

                if self.data:
                    var_length = len(self.data)

                elif self.file_regex:

                    if 'subset' in self.file_regex:
                        var_length = len(self.file_regex['subset'])

                    elif 'expected_multiplicity' in self.file_regex:
                        var_length = self.file_regex['expected_multiplicity']

                elif self.file_contents:

                    if 'expected_multiplicity' in self.file_contents:
                        var_length = self.file_contents['expected_multiplicity']

                elif self.is_base_variable():
                    var_length = 1

                else:
                    raise ValueError('bad 3!')

            var_lengths.update({directory_path: var_length})

        return var_lengths

    def get_values(self, directory):
        """Get the values of this variable.

        TODO: refactor repeated code blocks.

        Parameters
        ----------
        directory : Path
            Directory within which to resolve variable.

        Raises
        ------
        UnresolvedVariableError
            If the variable...

        """

        vals = []

        if self.file_regex:

            if self.file_regex.get('is_dir'):

                for root, _, _ in os.walk(directory):
                    root_rel = Path(root).relative_to(directory).as_posix()

                    match = re.search(self.file_regex['pattern'], root_rel)
                    if match:
                        match_groups = match.groups()
                        if match_groups:
                            match = match_groups[self.file_regex['group']]
                            val_fmt = self.value.format(match)
                            vals.append(val_fmt)

            else:
                # Search files in the given directory
                for i in directory.iterdir():
                    match = re.search(self.file_regex['pattern'], i.name)
                    if match:
                        match_groups = match.groups()
                        if match_groups:
                            match = match_groups[self.file_regex['group']]
                            val_fmt = self.value.format(match)
                            vals.append(val_fmt)

        elif self.file_contents:

            path = Path(directory).joinpath(self.file_contents['path'])
            with path.open('r') as handle:
                for i in handle.readlines():
                    vals.append(i.strip())

        elif self.data:
            for i in self.data:
                vals.append(self.value.format(i))

        else:
            vals.append(self.value)

        if not vals:
            msg = ('Cannot resolve variable value with name: {}')
            raise UnresolvedVariableError(msg.format(self.name))

        vals = sorted(vals)

        return vals


class Submission(Base):
    """Class to represent the submission of (part of) a workflow."""

    __tablename__ = 'submission'

    id_ = Column('id', Integer, primary_key=True)
    order_id = Column(Integer)
    workflow_id = Column(Integer, ForeignKey('workflow.id'))
    submit_time = Column(DateTime)
    alt_scratch_dir_name = Column(String(255), nullable=True)

    workflow = relationship('Workflow', back_populates='submissions')
    command_group_submissions = relationship(
        'CommandGroupSubmission',
        back_populates='submission',
        order_by='CommandGroupSubmission.command_group_exec_order',
    )

    variable_values = relationship('VarValue', back_populates='submission')

    def __init__(self, workflow, task_range):

        self.submit_time = datetime.now()
        self.order_id = len(workflow.submissions)
        self.workflow = workflow

        # print('Submission.__init__: task_range: {}'.format(task_range), flush=True)

        self.resolve_variable_values(self.workflow.directory, self.first_iteration)

        cg_subs = []
        for i in self.workflow.command_groups:
            task_range = [1, -1, 1]  # TEMP
            cg_sub = CommandGroupSubmission(i, self, task_range)
            cg_subs.append(cg_sub)

        session = Session.object_session(self)
        session.commit()

        # `SchedulerGroup`s must be generated after `CommandGroupSubmission`s and
        # `resolve_variable_values`:
        self._scheduler_groups = self.get_scheduler_groups()

        if self.workflow.has_alternate_scratch:
            self._make_alternate_scratch_dirs()

        # `Task`s must be generated after `SchedulerGroup`s:
        cg_sub_iters = []
        for cg_sub in self.command_group_submissions:

            for iteration in self.workflow.iterations:

                if iteration.order_id > 0 and self.workflow.loop.get('groups'):
                    # For > first iteration, not all command groups need be run:
                    if cg_sub.command_group_exec_order not in self.workflow.loop['groups']:
                        continue

                cg_sub_iter = CommandGroupSubmissionIteration(iteration, cg_sub)
                cg_sub_iters.append(cg_sub_iter)

        session.commit()
        # `cg_sub_iter.num_outputs` requires all cg_sub_iters to be generated:
        for cg_sub_iter in cg_sub_iters:
            for task_num in range(cg_sub_iter.num_outputs):
                Task(cg_sub_iter, task_num)

        self.first_iteration.status = IterationStatus('active')

    @reconstructor
    def init_on_load(self):
        self._scheduler_groups = self.get_scheduler_groups()

    def _make_alternate_scratch_dirs(self):
        """Create a new directory on each alternate scratch for this submission."""

        alt_scratches = self.workflow.all_alternate_scratch

        # Find a suitable alternate scratch directory name for this submission:
        count = 0
        MAX_COUNT = 10
        hex_length = 10
        alt_dirname = get_random_hex(hex_length)
        while True:
            if all([not i.joinpath(alt_dirname).exists() for i in alt_scratches]):
                break
            alt_dirname = get_random_hex(hex_length)
            count += 1
            if count > MAX_COUNT:
                msg = ('Could not find a suitable alternate scratch directory name '
                       'in {} iterations.')
                raise RuntimeError(msg.format(MAX_COUNT))

        # Make alternate scratch "root" directories:
        for alt_scratch in alt_scratches:
            alt_scratch_root = alt_scratch.joinpath(alt_dirname)
            alt_scratch_root.mkdir(parents=False, exist_ok=False)

        self.alt_scratch_dir_name = alt_dirname

    def get_working_directories(self, iteration):
        dirs = []
        for cg_sub in self.command_group_submissions:
            cg_sub_iter = cg_sub.get_command_group_submission_iteration(iteration)
            for i in cg_sub_iter.get_directories():
                if i not in dirs:
                    dirs.append(i)
        return dirs

    @property
    def first_iteration(self):
        return self.workflow.first_iteration

    @property
    def scheduler_groups(self):
        return self._scheduler_groups

    def get_scheduler_groups(self):
        """Get scheduler groups for this workflow submission."""
        return SchedulerGroup.get_scheduler_groups(self)

    def get_scheduler_group_index(self, command_group_submission):
        """Get the position of a command group submission within the submission's
        scheduler groups.

        Parameters
        ----------
        command_group_submission : CommandGroupSubmission

        Returns
        -------
        tuple (int, int)
            First integer identifies which scheduler group. Second integer identifies
            the relative position of the command group within the scheduler group.

        """

        if command_group_submission not in self.command_group_submissions:
            msg = 'Command group submission {} is not part of the submission.'
            raise ValueError(msg.format(command_group_submission))

        for i in self.scheduler_groups:
            if i.has(command_group_submission):
                return (i.order_id, i.index(command_group_submission))

        msg = 'Command group submission {} is not part of the scheduler group.'
        raise ValueError(msg.format(command_group_submission))

    def get_scheduler_group(self, command_group_submission):

        sch_group_idx, _ = self.get_scheduler_group_index(command_group_submission)
        return self.scheduler_groups[sch_group_idx]

    def is_variable_resolved(self, variable_definition, iteration, directory_var_val=None):
        """Returns True if the passed variable_definition has been resolved
        for this Submission and iteration."""
        # Check the variable definition is part of the workflow:
        if variable_definition not in self.workflow.variable_definitions:
            msg = ('Passed variable_definition object is not in the '
                   ' workflow of this submission.')
            raise ValueError(msg)

        for i in self.variable_values:
            if i.variable_definition == variable_definition:
                if i.iteration == iteration:
                    if directory_var_val:
                        if i.directory_value == directory_var_val:
                            return True
                    else:
                        return True

        return False

    def resolve_variable_values(self, root_directory, iteration):
        """Attempt to resolve as many variable values in the Workflow as
        possible."""

        session = Session.object_session(self)

        # Loop through CommandGroupSubmissions in order:
        for i in self.workflow.command_groups:

            dir_var = i.directory_variable

            # VarValues representing the resolved command group working directories:
            cg_dirs_var_vals = []
            cg_dirs_var_vals_other_val = []
            for j in dir_var.variable_values:
                if j.iteration == iteration:
                    cg_dirs_var_vals.append(j)
                else:
                    cg_dirs_var_vals_other_val.append(j.value)

            if cg_dirs_var_vals:
                pass
                # print(('Submission.resolve_variable_values: found existing resolved '
                #        'directory variables: {}').format(cg_dirs_var_vals), flush=True)

            else:

                # print(('Submission.resolve_variable_values: trying to resolve directory '
                #        'variable values.'), flush=True)

                # Directory variable has not yet been resolved; try:
                try:
                    dir_var_vals_dat = dir_var.get_values(root_directory)
                    # print(('Submission.resolve_variable_values: found directories with '
                    #        'values: {}.'.format(dir_var_vals_dat)), flush=True)

                except UnresolvedVariableError:
                    # Move on to next command group:
                    continue

                dir_var_vals_dat_new = [
                    j for j in dir_var_vals_dat
                    if (j not in cg_dirs_var_vals_other_val or j == '.')
                ]

                # print(('Submission.resolve_variable_values: new directories are: '
                #        '{}.'.format(dir_var_vals_dat_new)), flush=True)

                # Add VarVals:
                for val_idx, val in enumerate(dir_var_vals_dat_new):
                    cg_dirs_var_vals.append(
                        VarValue(
                            value=val,
                            order_id=val_idx,
                            var_definition=dir_var,
                            submission=self,
                            iteration=iteration,
                        )
                    )

            var_defns_rec = i.variable_definitions_recursive

            # print(('Submission.resolve_variable_values: cg_dirs_var_vals: '
            #        '{}.'.format(cg_dirs_var_vals)), flush=True)

            for j in cg_dirs_var_vals:

                # print(('Submission.resolve_variable_values: dir var val: '
                #        '{}.'.format(j)), flush=True)

                var_vals_dat = resolve_variable_values(
                    var_defns_rec,
                    root_directory.joinpath(j.value)
                )

                # print(('Submission.resolve_variable_values: var_vals_dat: '
                #        '{}.'.format(var_vals_dat)), flush=True)

                for k, v in var_vals_dat.items():

                    # print(('Submission.resolve_variable_values: var_vals_dat k: '
                    #        '{}; v: {}.'.format(k, v)), flush=True)

                    vals_dat = v['vals']
                    var_defn = self.workflow.get_variable_definition_by_name(k)

                    # print(('Submission.resolve_variable_values: vals_dat '
                    #        '{}.'.format(vals_dat)), flush=True)
                    # print(('Submission.resolve_variable_values: var_defn '
                    #        '{}.'.format(var_defn)), flush=True)

                    if not self.is_variable_resolved(var_defn, iteration, j):

                        # print(('Submission.resolve_variable_values: {} not resolved...'.format(
                        #     var_defn)), flush=True)

                        for val_idx, val in enumerate(vals_dat):

                            # print(('Submission.resolve_variable_values: val: {}...'.format(
                            #     val)), flush=True)

                            VarValue(
                                value=val,
                                order_id=val_idx,
                                var_definition=var_defn,
                                submission=self,
                                iteration=iteration,
                                directory_value=j
                            )
                            session.commit()

    def write_submit_dirs(self, hf_dir):
        """Write the directory structure necessary for this submission."""

        # Ensure scheduler output and error directories exist, if specified:
        for cg_sub in self.command_group_submissions:
            root_dir = self.workflow.directory
            out_dir = root_dir.joinpath(cg_sub.command_group.scheduler.output_dir)
            err_dir = root_dir.joinpath(cg_sub.command_group.scheduler.error_dir)
            if not out_dir.is_dir():
                out_dir.mkdir()
            if not err_dir.is_dir():
                err_dir.mkdir()

        # Make the workflow directory if it does not exist:
        wf_path = hf_dir.joinpath('workflow_{}'.format(self.workflow_id))
        if not wf_path.exists():
            wf_path.mkdir()

        # Make the submit directory:
        submit_path = wf_path.joinpath('submit_{}'.format(self.order_id))
        submit_path.mkdir()

        num_dir_vals_first_iter = {}  # keys are cg_sub_idx
        for iteration in self.workflow.iterations:

            # Make the iteration directory for each iteration:
            iter_path = submit_path.joinpath('iter_{}'.format(iteration.order_id))
            iter_path.mkdir()

            for idx, i in enumerate(self.scheduler_groups):

                max_num_tasks = i.get_max_num_tasks(self.first_iteration)
                step_size = i.get_step_size(self.first_iteration)

                # Make the scheduler group directory for each scheduler group:
                sg_path = iter_path.joinpath('scheduler_group_{}'.format(idx))
                sg_path.mkdir()

                # Loop through cmd groups in this scheduler group:
                for cg_sub_idx, cg_sub in enumerate(i.command_group_submissions):

                    cg_sub_iter = cg_sub.get_command_group_submission_iteration(iteration)
                    if not cg_sub_iter:
                        continue

                    cg_sub_first_iter = cg_sub.get_command_group_submission_iteration(
                        self.first_iteration
                    )
                    num_dir_vals = cg_sub_first_iter.num_directories
                    all_dir_slots = [''] * max_num_tasks

                    # Distribute dirs over num_dir_slots:
                    for k in range(0, max_num_tasks, step_size[cg_sub_idx]):
                        dir_idx = round((k / max_num_tasks) * num_dir_vals)
                        all_dir_slots[k] = 'REPLACE_WITH_DIR_{}'.format(dir_idx)

                    wk_dirs_path = iter_path.joinpath('working_dirs_{}{}'.format(
                        cg_sub.command_group_exec_order, CONFIG.get('working_dirs_file_ext')))

                    # Make the working directory template files for each cmd group:
                    with wk_dirs_path.open('w') as handle:
                        for dir_path in all_dir_slots:
                            handle.write('{}\n'.format(dir_path))

                # Make the variable values directories for each scheduler group:
                var_values_path = sg_path.joinpath('var_values')
                var_values_path.mkdir()
                for j in range(1, max_num_tasks + 1):
                    j_fmt = zeropad(j, max_num_tasks)
                    vv_j_path = var_values_path.joinpath(j_fmt)
                    vv_j_path.mkdir()

    def write_jobscripts(self, hf_dir):

        wf_path = hf_dir.joinpath('workflow_{}'.format(self.workflow_id))
        submit_path = wf_path.joinpath('submit_{}'.format(self.order_id))
        js_paths = []
        js_stats_paths = []
        for cg_sub in self.command_group_submissions:
            js_paths_i = cg_sub.write_jobscript(dir_path=submit_path)
            js_paths.append(js_paths_i['jobscript'])
            js_stats_paths.append(js_paths_i['stats_jobscript'])

        return js_paths, js_stats_paths

    def submit_jobscripts(self, jobscript_paths):

        loop_groups = self.workflow.loop['groups']
        cmd_group_idx = range(len(self.workflow.command_groups))

        if loop_groups:

            pre_loop_idx = [i for i in cmd_group_idx if i < min(loop_groups)]
            post_loop_idx = [i for i in cmd_group_idx if i > max(loop_groups)]

            # List of tuples mapping jobscript path index (i.e. command group order id) to
            # iteration index:
            js_submissions = [(i, 0) for i in pre_loop_idx]

            for iteration in self.workflow.iterations:
                for i in loop_groups:
                    js_submissions.append((i, iteration.order_id))

            for i in post_loop_idx:
                js_submissions.append((i, 0))

        else:
            js_submissions = [(i, 0) for i in cmd_group_idx]

        sumbit_cmd = os.getenv('HPCFLOW_QSUB_CMD', 'qsub')
        last_submit_id = None
        js_paths, js_stat_paths = jobscript_paths

        for cg_sub_idx, iter_idx in js_submissions:

            iter_idx_var = 'ITER_IDX={}'.format(iter_idx)
            cg_sub = self.command_group_submissions[cg_sub_idx]
            iteration = self.workflow.iterations[iter_idx]
            cg_sub_iter = cg_sub.get_command_group_submission_iteration(iteration)
            js_path_i, js_stat_path_i = js_paths[cg_sub_idx], js_stat_paths[cg_sub_idx]

            qsub_cmd = [sumbit_cmd]

            if last_submit_id:

                # Add conditional submission:
                if iteration.order_id > 0:
                    hold_arg = '-hold_jid'
                elif cg_sub.command_group.nesting == NestingType('hold'):
                    hold_arg = '-hold_jid'
                else:
                    hold_arg = '-hold_jid_ad'

                qsub_cmd += [hold_arg, last_submit_id]

            qsub_cmd += ['-v', iter_idx_var]
            qsub_cmd.append(str(js_path_i))

            # Submit the jobscript:
            job_id_str = self.submit_jobscript(qsub_cmd, js_path_i, iteration)
            cg_sub_iter.scheduler_job_id = int(job_id_str)
            last_submit_id = job_id_str

            # Submit the stats jobscript:
            if js_stat_path_i:
                st_cmd = [sumbit_cmd, '-hold_jid_ad', last_submit_id, '-v', iter_idx_var]
                st_cmd.append(str(js_stat_path_i))

                job_id_str = self.submit_jobscript(st_cmd, js_stat_path_i, iteration)
                cg_sub_iter.scheduler_stats_job_id = int(job_id_str)
                last_submit_id = job_id_str

    def submit_jobscript(self, cmd, js_path, iteration):

        cwd = str(self.workflow.directory)
        proc = run(cmd, stdout=PIPE, stderr=PIPE, cwd=cwd)
        qsub_out = proc.stdout.decode().strip()
        qsub_err = proc.stderr.decode().strip()
        if qsub_out:
            print(qsub_out, flush=True)
        if qsub_err:
            print(qsub_err, flush=True)

        # Extract newly submitted job ID:
        pattern = r'[0-9]+'
        job_id_search = re.search(pattern, qsub_out)
        try:
            job_id_str = job_id_search.group()
        except AttributeError:
            msg = ('Could not retrieve the job ID from the submitted jobscript '
                   'found at {}. No more jobscripts will be submitted.')
            raise ValueError(msg.format(js_path))

        return job_id_str

    def get_stats(self, jsonable=True, datetime_dicts=False):
        """Get task statistics for this submission."""
        out = {
            'submission_id': self.id_,
            'command_group_submissions': [
                i.get_stats(jsonable=jsonable, datetime_dicts=datetime_dicts)
                for i in self.command_group_submissions]
        }
        return out


class CommandGroupSubmission(Base):
    """Class to represent the submission of a single command group."""

    __tablename__ = 'command_group_submission'

    id_ = Column('id', Integer, primary_key=True)
    command_group_id = Column(Integer, ForeignKey('command_group.id'))
    submission_id = Column(Integer, ForeignKey('submission.id'))
    task_start = Column(Integer)
    task_stop = Column(Integer)
    task_step = Column(Integer)
    commands_written = Column(Boolean)
    # _task_multiplicity = Column('task_multiplicity', Integer, nullable=True)

    command_group = relationship('CommandGroup',
                                 back_populates='command_group_submissions')

    submission = relationship('Submission', back_populates='command_group_submissions')

    command_group_exec_order = deferred(
        select([CommandGroup.exec_order]).where(
            CommandGroup.id_ == command_group_id))

    is_command_writing = relationship(
        'IsCommandWriting',
        uselist=False,
        cascade='all, delete, delete-orphan'
    )

    command_group_submission_iterations = relationship(
        'CommandGroupSubmissionIteration',
        back_populates='command_group_submission',
    )

    def __repr__(self):
        out = (
            '{}('
            'command_group={!r}, '
            'submission_id={!r})').format(
            self.__class__.__name__,
            self.command_group,
            self.submission_id,
        )
        return out

    def __init__(self, command_group, submission, task_range):

        # print('CommandGroupSubmission.__init__: task_range: {}'.format(task_range), flush=True)

        self.command_group = command_group
        self.submission = submission
        self.task_range = task_range

    @property
    def task_range(self):
        return (self.task_start, self.task_stop, self.task_step)

    @task_range.setter
    def task_range(self, task_range):
        self.task_start = task_range[0]
        self.task_stop = task_range[1]
        if len(task_range) == 3:
            self.task_step = task_range[2]
        else:
            self.task_step = 1

    @property
    def task_range_idx(self):
        return list(range(*self.task_range))

    @property
    def variable_values(self):

        var_values = []
        for i in self.command_group.variable_definitions:
            if i.variable_values:
                var_values.append(i)

        return var_values

    @property
    def num_submitted_tasks(self):
        """Get the number of submitted tasks based on the task range.

        Returns
        -------
        num : int
            If the number of tasks is as yet undetermined, `None` is returned.

        """

        if self.task_stop == -1:
            return None

        num = ceil((self.task_stop - (self.task_start - 1)) / self.task_step)

        return num

    @property
    def scheduler_group_index(self):
        """Get the position of this command group submission within the submission's
        scheduler groups.

        Returns
        -------
        tuple (int, int)
            First integer identifies which scheduler group. Second integer identifies
            the relative position of the command group within the scheduler group.

        """
        return self.submission.get_scheduler_group_index(self)

    @property
    def scheduler_group(self):
        """Get the scheduler group to which this command group belongs."""
        return self.submission.get_scheduler_group(self)

    def get_command_group_submission_iteration(self, iteration):

        for i in self.command_group_submission_iterations:
            if i.iteration == iteration:
                return i

    @property
    def alternate_scratch_dir(self):
        if self.command_group.alternate_scratch:
            return self.command_group.alternate_scratch.joinpath(
                self.submission.alt_scratch_dir_name)
        else:
            return None

    def get_var_definition_by_name(self, var_name):
        """"""

        for i in self.command_group.var_definitions:
            if i.name == var_name:
                return i

    def write_jobscript(self, dir_path):
        """Write the jobscript."""

        cg_sub_first_iter = self.get_command_group_submission_iteration(
            self.submission.first_iteration)

        js_path = self.command_group.scheduler.write_jobscript(
            dir_path=dir_path,
            workflow_directory=self.submission.workflow.directory,
            command_group_order=self.command_group_exec_order,
            max_num_tasks=self.scheduler_group.get_max_num_tasks(
                self.submission.first_iteration),
            task_step_size=cg_sub_first_iter.step_size,
            environment=self.command_group.environment,
            archive=self.command_group.archive is not None,
            alternate_scratch_dir=self.alternate_scratch_dir,
            command_group_submission_id=self.id_,
            name=self.command_group.name,
        )

        js_stats_path = None
        if self.command_group.stats:
            js_stats_path = self.command_group.scheduler.write_stats_jobscript(
                dir_path=dir_path,
                workflow_directory=self.submission.workflow.directory,
                command_group_order=self.command_group_exec_order,
                max_num_tasks=self.scheduler_group.get_max_num_tasks(
                    self.submission.first_iteration),
                task_step_size=cg_sub_first_iter.step_size,
                command_group_submission_id=self.id_,
                name=self.command_group.stats_name,
            )

        out = {
            'jobscript': js_path,
            'stats_jobscript': js_stats_path,
        }

        return out

    def write_runtime_files(self, project, task_idx, iter_idx):
        iteration = self.get_iteration(iter_idx)
        self.queue_write_command_file(project, task_idx, iteration)
        self.write_variable_files(project, task_idx, iteration)

    def queue_write_command_file(self, project, task_idx, iteration):
        """Ensure the command file for this command group submission is written, ready
        to be invoked by the jobscript, and also refresh the resolved variable values
        so that when the variable files are written, they are up to date."""

        session = Session.object_session(self)

        sleep_time = 5
        context = 'CommandGroupSubmission.write_cmd'
        block_msg = ('{{}} {}: Writing command file blocked. Sleeping for {} '
                     'seconds'.format(context, sleep_time))
        unblock_msg = ('{{}} {}: Commands not written and writing available. Writing '
                       'command file.'.format(context))
        written_msg = '{{}} {}: Command files already written.'.format(context)
        refresh_vals_msg = '{{}} {}: Refreshing resolved variable values.'.format(context)
        write_dirs_msg = ('{{}} {}: Writing working directory files for '
                          'iteration {}').format(context, iteration)
        write_as_msg = ('{{}} {}: Writing alternate scratch exclusion list for '
                        'task_idx {}.').format(context, task_idx)
        make_alt_msg = ('{{}} {}: Making alternate scratch working '
                        'directories.'.format(context))

        blocked = True
        while blocked:

            try:
                session.refresh(self)
            except OperationalError:
                # Database is likely locked.
                print(block_msg.format(datetime.now()), flush=True)
                sleep(sleep_time)
                continue

            if self.is_command_writing:
                print(block_msg.format(datetime.now()), flush=True)
                sleep(sleep_time)

            else:
                try:
                    self.is_command_writing = IsCommandWriting()
                    session.commit()
                    blocked = False

                except IntegrityError:
                    # Another process has already set `is_command_writing`
                    session.rollback()
                    print(block_msg.format(datetime.now()), flush=True)
                    sleep(sleep_time)

                except OperationalError:
                    # Database is likely locked.
                    session.rollback()
                    print(block_msg.format(datetime.now()), flush=True)
                    sleep(sleep_time)

                if not blocked:

                    if iteration.status == IterationStatus('pending'):
                        iteration.status = IterationStatus('active')

                    # This needs to happen once *per task* per CGS:
                    print(refresh_vals_msg.format(datetime.now()), flush=True)
                    self.submission.resolve_variable_values(project.dir_path, iteration)

                    # This needs to happen once *per task* per CGS (if it has AS):
                    if self.command_group.alternate_scratch:
                        print(write_as_msg.format(datetime.now()), flush=True)
                        task = self.get_task(task_idx, iteration)
                        self.write_alt_scratch_exclusion_list(project, task, iteration)

                    cg_sub_iter = self.get_command_group_submission_iteration(iteration)
                    if not cg_sub_iter.working_dirs_written:

                        # These need to happen once *per iteration* per CGS:

                        print(write_dirs_msg.format(datetime.now()), flush=True)
                        cg_sub_iter.write_working_directories(project)

                        if self.command_group.alternate_scratch:
                            print(make_alt_msg.format(datetime.now()), flush=True)
                            self.make_alternate_scratch_dirs(project, iteration)

                        cg_sub_iter.working_dirs_written = True

                    if not self.commands_written:
                        # This needs to happen once per CGS:
                        print(unblock_msg.format(datetime.now()), flush=True)
                        self.write_command_file(project)
                        self.commands_written = True
                    else:
                        print(written_msg.format(datetime.now()), flush=True)

                    self.is_command_writing = None
                    session.commit()

    def write_variable_files(self, project, task_idx, iteration):

        task = self.get_task(task_idx, iteration)
        var_vals_normed = task.get_variable_values_normed()

        print('CGS.write_variable_files: task: {}'.format(task), flush=True)
        print('CGS.write_variable_files: var_vals_normed: {}'.format(
            var_vals_normed), flush=True)

        max_num_tasks = self.scheduler_group.get_max_num_tasks(
            self.submission.first_iteration)

        var_values_task_dir = project.hf_dir.joinpath(
            'workflow_{}'.format(self.submission.workflow.id_),
            'submit_{}'.format(self.submission.order_id),
            'iter_{}'.format(iteration.order_id),
            'scheduler_group_{}'.format(self.scheduler_group_index[0]),
            'var_values',
            zeropad(task.scheduler_id, max_num_tasks),
        )

        for var_name, var_val_all in var_vals_normed.items():
            var_fn = 'var_{}{}'.format(var_name, CONFIG.get('variable_file_ext'))
            var_file_path = var_values_task_dir.joinpath(var_fn)
            with var_file_path.open('w') as handle:
                for i in var_val_all:
                    handle.write('{}\n'.format(i))

    @staticmethod
    def get_formatted_commands(commands, num_cores, parallel_modes, indent=''):

        # TODO: what about parallel mode env?
        delims = CONFIG.get('variable_delimiters')
        lns_cmd = []
        for i in commands:
            if 'line' in i:
                cmd_ln = indent
                para_mode = i.get('parallel_mode')
                if para_mode:
                    para_mode_config = parallel_modes.get(
                        para_mode.lower())  # todo raise on miss
                    para_command = para_mode_config.get('command')
                    if para_command:
                        cmd_ln += para_command.replace('<<num_cores>>', num_cores) + ' '
                line = i['line']
                for var_name in extract_variable_names(line, delims):
                    line = line.replace(delims[0] + var_name + delims[1], f'${var_name}')
                cmd_ln += line
                lns_cmd.append(cmd_ln)
            elif 'subshell' in i:
                sub_cmds = CommandGroupSubmission.get_formatted_commands(
                    i['subshell'],
                    num_cores,
                    parallel_modes,
                    indent=(indent+'\t'),
                )
                lns_cmd.extend([f'{indent}('] + sub_cmds + [f'{indent})'])

        return lns_cmd

    def write_command_file(self, project):

        lns_cmd = self.get_formatted_commands(
            self.command_group.commands,
            num_cores=self.command_group.scheduler.NUM_CORES_VAR,
            parallel_modes=self.command_group.workflow.parallel_modes,
            indent=('\t' if self.command_group.variable_definitions else ''),
        )

        lns_while_start = ['while true', 'do']
        lns_while_end = ['done \\']

        dt_stamp = datetime.now().strftime(r'%Y.%m.%d at %H:%M:%S')
        about_msg = ['# --- commands file generated by `hpcflow` (version: {}) '
                     'on {} ---'.format(__version__, dt_stamp)]

        max_num_tasks = self.scheduler_group.get_max_num_tasks(
            self.submission.first_iteration)

        lns_task_id_pad = [
            'MAX_NUM_TASKS={}'.format(max_num_tasks),
            'MAX_NUM_DIGITS="${#MAX_NUM_TASKS}"',
            'ZEROPAD_TASK_ID=$(printf "%0${MAX_NUM_DIGITS}d" $SGE_TASK_ID)',
        ]

        lns_read = []
        lns_fds = []

        for idx, i in enumerate(self.command_group.variable_definitions):

            fd_idx = idx + 3

            var_fn = 'var_{}{}'.format(i.name, CONFIG.get('variable_file_ext'))
            var_file_path = ('$ITER_DIR/scheduler_group_{}/var_values'
                             '/$ZEROPAD_TASK_ID/{}').format(
                                 self.scheduler_group_index[0], var_fn)

            lns_read.append('\tread -u{} {} || break'.format(fd_idx, i.name))

            if idx > 0:
                lns_fds[-1] += ' \\'

            lns_fds.append('\t{}< {}'.format(fd_idx, var_file_path))

        lns_cmd_print = ['printf "Running command: \\"{}\\"\\n" >> $LOG_PATH 2>&1'.format(
            i.strip('\t').replace('"', r'\\\\\"')) for i in lns_cmd]

        if self.command_group.variable_definitions:
            lns_cmd_print = ['\t{}'.format(i) for i in lns_cmd_print]
            cmd_lns = (about_msg + [''] +
                       lns_task_id_pad + [''] +
                       lns_while_start + [''] +
                       lns_read + [''] +
                       lns_cmd_print + [''] +
                       lns_cmd + [''] +
                       lns_while_end +
                       lns_fds + [''])
        else:
            cmd_lns = (about_msg + [''] +
                       lns_cmd_print + [''] +
                       lns_cmd + [''])

        cmd_lns = '\n'.join(cmd_lns)

        cmd_path = project.hf_dir.joinpath(
            'workflow_{}'.format(self.submission.workflow.id_),
            'submit_{}'.format(self.submission.order_id),
            'cmd_{}{}'.format(self.command_group_exec_order, CONFIG.get('jobscript_ext')),
        )
        with cmd_path.open('w') as handle:
            handle.write(cmd_lns)

    def write_alt_scratch_exclusion_list(self, project, task, iteration):
        """Write alternate scratch exclusion files (for e.g. rsync)"""

        # List of Paths to exclude, relative to `self.submission.workflow.directory`:
        excluded_paths = [
            Path(CONFIG.get('hpcflow_directory'))] + self.submission.workflow.profile_files

        out_dir = Path(self.command_group.scheduler.output_dir)
        err_dir = Path(self.command_group.scheduler.error_dir)
        if out_dir not in excluded_paths:
            excluded_paths.append(out_dir)
        if err_dir not in excluded_paths:
            excluded_paths.append(err_dir)

        working_dir_path = Path(task.get_working_directory_value())
        alt_scratch_exclusions = []
        for exc_path in excluded_paths:
            try:
                exc_path.relative_to(working_dir_path)
            except ValueError:
                continue
            alt_scratch_exclusions.append(exc_path)

        exc_list_path = project.hf_dir.joinpath(
            'workflow_{}'.format(self.submission.workflow.id_),
            'submit_{}'.format(self.submission.order_id),
            'iter_{}'.format(iteration.order_id),
            '{}_{}_{}{}'.format(
                CONFIG.get('alt_scratch_exc_file'),
                self.command_group_exec_order,
                task.order_id,
                CONFIG.get('alt_scratch_exc_file_ext'),
            ),
        )

        working_dir_abs = self.submission.workflow.directory.joinpath(working_dir_path)
        about = (
            '# Alternate scratch exclusion list. Patterns are relative '
            'to task #{} working directory:\n'
            '#   "{}"\n\n'
        )
        with exc_list_path.open('w') as handle:
            handle.write(about.format(task.order_id, working_dir_abs))
            for exc_path in alt_scratch_exclusions:
                handle.write(str(exc_path) + '\n')

    def make_alternate_scratch_dirs(self, project, iteration):
        """Generate task working directories on the alternate scratch."""

        # Get task working directories:
        cg_sub_iter = self.get_command_group_submission_iteration(iteration)
        working_dirs = [task.get_working_directory() for task in cg_sub_iter.tasks]

        alt_scratch_root = self.command_group.alternate_scratch.joinpath(
            self.submission.alt_scratch_dir_name)

        for working_dir in working_dirs:
            if working_dir.value == '.':
                # Already made "root" dir.
                continue
            alt_scratch_w_dir = alt_scratch_root.joinpath(working_dir.value)
            alt_scratch_w_dir.mkdir(parents=True, exist_ok=True)

    def get_iteration(self, iter_idx):
        for i in self.submission.workflow.iterations:
            if i.order_id == iter_idx:
                return i

    def get_task(self, task_idx, iteration):
        cg_sub_iter = self.get_command_group_submission_iteration(iteration)
        for i in cg_sub_iter.tasks:
            if i.order_id == task_idx and i.iteration == iteration:
                return i

    def set_task_start(self, task_idx, iter_idx):
        context = 'CommandGroupSubmission.set_task_start'
        msg = '{{}} {}: Task index {} started.'.format(context, task_idx)
        start_time = datetime.now()
        print(msg.format(start_time), flush=True)
        iteration = self.get_iteration(iter_idx)
        task = self.get_task(task_idx, iteration)
        task.start_time = start_time
        print('task: {}'.format(task))

    def set_task_end(self, task_idx, iter_idx):
        context = 'CommandGroupSubmission.set_task_end'
        msg = '{{}} {}: Task index {} ended.'.format(context, task_idx)
        end_time = datetime.now()
        print(msg.format(end_time), flush=True)
        iteration = self.get_iteration(iter_idx)
        task = self.get_task(task_idx, iteration)
        task.end_time = end_time
        print('task: {}'.format(task))

    def do_archive(self, task_idx, iter_idx):
        """Archive the working directory associated with a given task in this command
        group submission."""

        # Adding a small delay increases the chance that `Task.is_archive_required` will
        # be False (and so save some time overall), in the case where all tasks start at
        # roughly the same time:
        sleep(10)

        iteration = self.get_iteration(iter_idx)
        task = self.get_task(task_idx, iteration)
        self.command_group.archive.execute_with_lock(task)

    def get_stats(self, jsonable=True, datetime_dicts=False):
        """Get task statistics for this command group submission."""
        out = {
            'command_group_submission_id': self.id_,
            'command_group_id': self.command_group.id_,
            'commands': self.command_group.commands,
            'name': self.command_group.name,
            'tasks': [task.get_stats(jsonable=jsonable, datetime_dicts=datetime_dicts)
                      for cgsub_iter in self.command_group_submission_iterations
                      for task in cgsub_iter.tasks
                      if task.iteration.status != IterationStatus('pending')]
        }
        return out

    def get_scheduler_stats(self, task_idx, iter_idx):

        # Get scheduler job ID and scheduler task ID:
        iteration = self.get_iteration(iter_idx)
        cg_sub_iter = self.get_command_group_submission_iteration(iteration)
        scheduler_job_id = cg_sub_iter.scheduler_job_id
        task = self.get_task(task_idx, iteration)
        task_id = task.scheduler_id

        info = self.command_group.scheduler.get_scheduler_stats(scheduler_job_id, task_id)

        if 'MB' in info['maxvmem']:
            maxvmem = float(info['maxvmem'].split('MB')[0])
        elif 'GB' in info['maxvmem']:
            maxvmem = float(info['maxvmem'].split('GB')[0])
        hostname = info['hostname']
        wallclock = int(info['ru_wallclock'].split('s')[0])

        task.memory = maxvmem
        task.hostname = hostname
        task.wallclock = wallclock


class VarValue(Base):
    """Class to represent the evaluated value of a variable."""

    __tablename__ = 'var_value'

    id_ = Column('id', Integer, primary_key=True)
    var_definition_id = Column(
        Integer,
        ForeignKey('var_definition.id'),
    )
    submission_id = Column(Integer, ForeignKey('submission.id'))
    value = Column(String(255))
    order_id = Column(Integer)
    directory_value_id = Column('directory_value_id', Integer, ForeignKey('var_value.id'))
    iteration_id = Column(Integer, ForeignKey('iteration.id'))

    variable_definition = relationship('VarDefinition', back_populates='variable_values')
    submission = relationship('Submission', back_populates='variable_values')
    directory_value = relationship('VarValue', uselist=False, remote_side=id_)
    iteration = relationship('Iteration', uselist=False)

    def __init__(self, value, order_id, var_definition, submission, iteration,
                 directory_value=None):

        self.value = value
        self.order_id = order_id
        self.iteration = iteration
        self.variable_definition = var_definition
        self.submission = submission
        self.directory_value = directory_value

    def __repr__(self):
        out = (
            '{}('
            'variable_name={}, '
            'value={}, '
            'order_id={}, '
            'iteration={}, '
            'directory={}'
            ')').format(
                self.__class__.__name__,
                self.variable_definition.name,
                self.value,
                self.order_id,
                self.iteration,
                self.directory_value.value if self.directory_value else None,
        )
        return out


class IsCommandWriting(Base):
    """Class to represent active writing of a command file."""

    __tablename__ = 'is_command_writing'

    command_group_submission_id = Column(
        Integer,
        ForeignKey('command_group_submission.id'),
        primary_key=True,
        unique=True
    )


class Task(Base):
    """Class to represent a single task."""

    __tablename__ = 'task'

    id_ = Column('id', Integer, primary_key=True)
    order_id = Column(Integer, nullable=False)
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    memory = Column(Float)
    hostname = Column(String(255))
    wallclock = Column(Integer)
    archive_status = Column(Enum(TaskArchiveStatus), nullable=True)
    _archive_start_time = Column('archive_start_time', DateTime, nullable=True)
    _archive_end_time = Column('archive_end_time', DateTime, nullable=True)
    archived_task_id = Column(Integer, ForeignKey('task.id'), nullable=True)

    command_group_submission_iteration_id = Column(
        Integer, ForeignKey('command_group_submission_iteration.id'))

    command_group_submission_iteration = relationship(
        'CommandGroupSubmissionIteration', back_populates='tasks', uselist=False)

    archived_task = relationship('Task', uselist=False, remote_side=id_)

    def __init__(self, command_group_submission_iteration, order_id):
        self.order_id = order_id
        self.command_group_submission_iteration = command_group_submission_iteration
        self.start_time = None
        self.end_time = None

        if self.command_group_submission_iteration.command_group_submission.command_group.archive:
            self.archive_status = TaskArchiveStatus('pending')

    @property
    def iteration(self):
        return self.command_group_submission_iteration.iteration

    def __repr__(self):
        out = (
            '{}('
            'order_id={}, '
            'command_group_submission_iteration_id={}, '
            'start_time={}, '
            'end_time={}'
            ')').format(
                self.__class__.__name__,
                self.order_id,
                self.command_group_submission_iteration_id,
                self.start_time,
                self.end_time,
        )
        return out

    @property
    def duration(self):
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        else:
            return None

    @property
    def scheduler_id(self):
        """Get the task ID, as understood by the scheduler."""
        num_tasks = self.command_group_submission_iteration.num_outputs
        step_size = self.command_group_submission_iteration.step_size
        scheduler_range = range(1, 1 + (num_tasks * step_size), step_size)
        scheduler_id = scheduler_range[self.order_id]

        return scheduler_id

    @property
    def archive_start_time(self):
        if self.archived_task:
            # Archive for this task was handled by another task with the same working dir:
            return self.archived_task.archive_start_time
        else:
            return self._archive_start_time

    @archive_start_time.setter
    def archive_start_time(self, start_time):
        self._archive_start_time = start_time

    @property
    def archive_end_time(self):
        if self.archived_task:
            # Archive for this task was handled by another task with the same working dir:
            return self.archived_task.archive_end_time
        else:
            return self._archive_end_time

    @archive_end_time.setter
    def archive_end_time(self, end_time):
        self._archive_end_time = end_time

    @property
    def archive_duration(self):
        if self.archive_start_time and self.archive_end_time:
            return self.archive_end_time - self.archive_start_time
        else:
            return None

    def get_working_directory(self):
        """Get the "working directory" of this task."""
        dir_vals = self.command_group_submission_iteration.get_directories()
        dirs_per_task = len(dir_vals) / \
            self.command_group_submission_iteration.num_outputs
        dir_idx = floor(self.order_id * dirs_per_task)
        working_dir = dir_vals[dir_idx]

        return working_dir

    def get_working_directory_value(self):
        return self.get_working_directory().value

    def get_stats(self, jsonable=True, datetime_dicts=False):
        """Get statistics for this task."""
        out = {
            'task_id': self.id_,
            'order_id': self.order_id,
            'scheduler_id': self.scheduler_id,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'duration': self.duration,
            'archive_start_time': self.archive_start_time,
            'archive_end_time': self.archive_end_time,
            'archive_duration': self.archive_duration,
            'archived_task_id': self.archived_task_id,
            'memory': self.memory,
            'hostname': self.hostname,
            'wallclock': self.wallclock,
            'working_directory': self.get_working_directory_value(),
            'archive_status': self.archive_status,
            'iteration': self.iteration.order_id,
        }

        if datetime_dicts:
            if self.duration:
                out['duration'] = timedelta_to_dict(out['duration'])
            if self.archive_duration:
                out['archive_duration'] = timedelta_to_dict(out['archive_duration'])
            if self.start_time:
                out['start_time'] = datetime_to_dict(out['start_time'])
            if self.end_time:
                out['end_time'] = datetime_to_dict(out['end_time'])
            if self.archive_start_time:
                out['archive_start_time'] = datetime_to_dict(out['archive_start_time'])
            if self.archive_end_time:
                out['archive_end_time'] = datetime_to_dict(out['archive_end_time'])

        if jsonable:

            if not datetime_dicts:

                if self.duration:
                    out['duration'] = format_time_delta(out['duration'])
                if self.archive_duration:
                    out['archive_duration'] = format_time_delta(out['archive_duration'])

                dt_fmt = r'%Y.%m.%d %H:%M:%S'

                if self.start_time:
                    out['start_time'] = out['start_time'].strftime(dt_fmt)
                if self.end_time:
                    out['end_time'] = out['end_time'].strftime(dt_fmt)
                if self.archive_start_time:
                    out['archive_start_time'] = out['archive_start_time'].strftime(dt_fmt)
                if self.archive_end_time:
                    out['archive_end_time'] = out['archive_end_time'].strftime(dt_fmt)

            if self.archive_status:
                out['archive_status'] = self.archive_status.value

        return out

    def get_same_directory_tasks(self):
        """Get a list of other Tasks within the same command group that share the same
        working directory and iteration."""
        same_dir_tasks = []
        for i in self.command_group_submission_iteration.tasks:
            if i is self:
                continue
            elif i.iteration == self.iteration:
                if i.get_working_directory() is self.get_working_directory():
                    same_dir_tasks.append(i)

        print('Task.get_same_directory_tasks: same_dir_tasks: {}'.format(same_dir_tasks),
              flush=True)

        return same_dir_tasks

    def is_archive_required(self):
        """Check if archive of this task is required. It is not required if a different
        task in the same command group submission with the same working directory begun
        its own archive after the commands of this command completed."""

        if not self.end_time:
            msg = ('`Task.is_archive_required` should not be called unit the task has '
                   'completed; {} has not completed.'.format(self))
            raise RuntimeError(msg)

        for i in self.get_same_directory_tasks():
            print('Checking if other task {} archived started after this task '
                  '({}) finished.'.format(i, self), flush=True)
            if i.archive_start_time:
                if i.archive_start_time > self.end_time:
                    self.archived_task = i
                    return False

        return True

    def get_variable_values(self):
        """Get the values of variables that are resolved in this task's working
        directory.

        Returns
        -------
        var_vals : dict of (str: list of str)
            Keys are the variable definition name and values are list of variable
            values as strings.

        """

        task_directory = self.get_working_directory()
        cg_sub = self.command_group_submission_iteration.command_group_submission
        sub_var_vals = cg_sub.submission.variable_values
        cmd_group_var_names = cg_sub.command_group.variable_names
        var_vals = {}

        print('Task.get_variable_values: sub_var_vals:', flush=True)
        pprint(sub_var_vals)

        print('Task.get_variable_values: cmd_group_var_names:', flush=True)
        pprint(cmd_group_var_names)

        for i in sub_var_vals:
            if i.directory_value == task_directory:
                var_defn_name = i.variable_definition.name
                if var_defn_name in cmd_group_var_names:
                    if var_defn_name in var_vals:
                        var_vals[var_defn_name].append(i.value)
                    else:
                        var_vals.update({var_defn_name: [i.value]})

        return var_vals

    def get_variable_values_normed(self):
        """Get the values of variables that are resolved in this task's working
        directory, where all variable values have the same, normalised multiplicity.

        Returns
        -------
        var_vals_normed : dict of (str: list of str)
            Keys are the variable definition name and values are list of variable
            values as strings. The list of variable values is the same length for
            each variable definition name.

        """

        var_vals = self.get_variable_values()
        if not var_vals:
            return {}

        only_names, only_vals = zip(*var_vals.items())
        only_vals_uniform = coerce_same_length(list(only_vals))

        cg_sub = self.command_group_submission_iteration.command_group_submission
        if cg_sub.command_group.is_job_array:
            val_idx = self.order_id % len(only_vals_uniform[0])
            only_vals_uniform = [[i[val_idx]] for i in only_vals_uniform]

        var_vals_normed = dict(zip(only_names, only_vals_uniform))

        return var_vals_normed


class Iteration(Base):
    """Class to represent a workflow iteration."""

    __tablename__ = 'iteration'

    id_ = Column('id', Integer, primary_key=True)
    workflow_id = Column(Integer, ForeignKey('workflow.id'))
    order_id = Column(Integer)
    status = Column(Enum(IterationStatus), default=IterationStatus('pending'))

    workflow = relationship('Workflow', back_populates='iterations', uselist=False)
    command_group_submission_iterations = relationship(
        'CommandGroupSubmissionIteration',
        back_populates='iteration',
    )

    def __init__(self, order_id):
        self.order_id = order_id

    def __repr__(self):
        out = (
            '{}('
            'id={}, '
            'workflow_id={}, '
            'order_id={}'
            ')'
        ).format(
            self.__class__.__name__,
            self.id_,
            self.workflow_id,
            self.order_id,
        )
        return out


class CommandGroupSubmissionIteration(Base):

    __tablename__ = 'command_group_submission_iteration'

    id_ = Column('id', Integer, primary_key=True)
    working_dirs_written = Column(Boolean, default=False)
    iteration_id = Column(Integer, ForeignKey('iteration.id'))
    scheduler_job_id = Column(Integer, nullable=True)
    scheduler_stats_job_id = Column(Integer, nullable=True)
    command_group_submission_id = Column(
        Integer, ForeignKey('command_group_submission.id'))

    iteration = relationship(
        'Iteration',
        back_populates='command_group_submission_iterations',
        uselist=False,
    )
    command_group_submission = relationship(
        'CommandGroupSubmission',
        back_populates='command_group_submission_iterations',
    )
    tasks = relationship('Task', back_populates='command_group_submission_iteration')

    def __init__(self, iteration, command_group_submission):
        self.iteration = iteration
        self.command_group_submission = command_group_submission

    def __repr__(self):
        out = (
            '{}('
            'iteration_id={}, '
            'command_group_submission_id={}, '
            'scheduler_job_id={}, '
            'scheduler_stats_job_id={}'
            ')'
        ).format(
            self.__class__.__name__,
            self.iteration_id,
            self.command_group_submission_id,
            self.scheduler_job_id,
            self.scheduler_stats_job_id,
        )
        return out

    def get_directory_values(self):

        dir_vals = [i.value for i in self.get_directories()]
        return dir_vals

    def get_directories(self):
        """Get the directory variable values associated with this command group
        submission and iteration."""

        dir_vars_all = self.command_group_submission.command_group.directory_variable.variable_values
        # Get only those with correct submission and iteration

        dirs = []
        for i in dir_vars_all:
            if i.iteration == self.iteration:
                if i.submission == self.command_group_submission.submission:
                    dirs.append(i)

        # dirs = [i for idx, i in enumerate(dirs) if idx in self.task_range_idx]

        return dirs

    @property
    def num_directories(self):
        return len(self.get_directories())

    def get_task_multiplicity(self):
        """Get the number of tasks associated with this command group submission."""

        # TODO: move get_task_multiplicity to CommandGroupSubmissionIteration !

        dirs = self.get_directory_values()

        sub = self.command_group_submission.submission

        var_lengths = {}
        for directory in dirs:
            var_lengths.update({directory: {}})
            for i in self.command_group_submission.command_group.variable_definitions:
                var_lengths_i = i.get_multiplicity(sub)  # as a func of dir
                for var_dir, num in var_lengths_i.items():
                    if var_dir == directory:
                        var_lengths[directory].update({i.name: num})

        var_lengths_combined = {}
        for directory, var_nums in var_lengths.items():
            if var_nums:
                uniq_lens = set(var_nums.values())
                num_uniq_lens = len(uniq_lens)
                if num_uniq_lens == 1:
                    combined_len = min(uniq_lens)
                elif num_uniq_lens == 2:
                    if min(uniq_lens) != 1:
                        raise ValueError('bad 4!')
                    combined_len = max(uniq_lens)
                else:
                    raise ValueError('bad 5!')
            else:
                combined_len = 1

            var_lengths_combined.update({directory: combined_len})

        return var_lengths_combined

    @property
    def num_outputs(self):
        """Get the number of outputs for this command group submission."""
        iteration = self.command_group_submission.submission.workflow.first_iteration
        return self.command_group_submission.scheduler_group.get_num_outputs(iteration)[
            self.command_group_submission.scheduler_group_index[1]]

    @property
    def step_size(self):
        """Get the scheduler step size for this command group submission."""
        iteration = self.command_group_submission.submission.workflow.first_iteration
        return self.command_group_submission.scheduler_group.get_step_size(iteration)[
            self.command_group_submission.scheduler_group_index[1]]

    @property
    def num_tasks(self):
        return len(self.tasks)

    def write_working_directories(self, project):
        """Replace lines in the working_dirs files with actual directory paths."""

        dir_vals = self.get_directories()

        cg_sub = self.command_group_submission

        wk_dirs_path = project.hf_dir.joinpath(
            'workflow_{}'.format(cg_sub.submission.workflow.id_),
            'submit_{}'.format(cg_sub.submission.order_id),
            'iter_{}'.format(self.iteration.order_id),
            'working_dirs_{}{}'.format(
                cg_sub.command_group_exec_order, CONFIG.get('working_dirs_file_ext')),
        )

        with wk_dirs_path.open() as handle:
            file_lns = handle.readlines()

        for idx, i in enumerate(file_lns):
            new_val = i.strip()
            if 'REPLACE_WITH_DIR_' in i:
                dir_idx = int(i.split('REPLACE_WITH_DIR_')[1])
                new_val = dir_vals[dir_idx].value
            file_lns[idx] = new_val

        with wk_dirs_path.open('w') as handle:
            for i in file_lns:
                handle.write(i + '\n')


class SchedulerGroup(object):
    """Class to represent a collection of consecutive command group submissions that have
    the same scheduler task range."""

    def __init__(self, order_id, command_groups_submissions):

        self.order_id = order_id
        self.command_group_submissions = command_groups_submissions

    def __repr__(self):
        out = ('{}('
               'order_id={}, '
               'command_group_submissions={}, '
               ')').format(
            self.__class__.__name__,
            self.order_id,
            self.command_group_submissions,
        )
        return out

    def get_max_num_tasks(self, iteration):
        return max(self.get_num_outputs(iteration))

    def get_step_size(self, iteration):
        return [int(self.get_max_num_tasks(iteration) / i)
                for i in self.get_num_outputs(iteration)]

    def get_num_outputs(self, iteration):

        num_outs = 1
        num_outs_prev = num_outs
        num_outs_all = []

        # Get num_outputs for all previous cg subs in this scheduler group
        for idx, cg_sub in enumerate(self.command_group_submissions):

            # print('SchedulerGroup.get_num_outputs: cg_sub idx: {}'.format(idx), flush=True)

            # print('SchedulerGroup.get_num_outputs: cg_sub_iters: ')
            # pprint(cg_sub.command_group_submission_iterations)

            cg_sub_iter = None
            for i in cg_sub.command_group_submission_iterations:
                if i.iteration == iteration:
                    cg_sub_iter = i
                    break
            if not cg_sub_iter:
                raise ValueError('Could not find CommandGroupSubmissionIteration object.')

            # Number of outputs depend on task multiplicity, `is_job_array` and `nesting`
            is_job_array = cg_sub.command_group.is_job_array
            nesting = cg_sub.command_group.nesting

            # print('SchedulerGroup.get_num_outputs: is_job_array: {}'.format(
            #     is_job_array), flush=True)
            # print('SchedulerGroup.get_num_outputs: nesting: {}'.format(nesting), flush=True)

            if nesting == NestingType('nest'):  # or first_cmd_group:
                num_outs = num_outs_prev
            elif nesting == NestingType('hold'):
                num_outs = 1
            elif nesting is None:
                num_outs = 1

            if is_job_array:
                # if nesting in [NestingType('hold'), None]:
                #     num_outs *= cg_sub.num_directories
                #     print('SchedulerGroup._get_num_outputs: cg_sub.num_directories: {}'.format(
                #         cg_sub.num_directories), flush=True)

                # cg_sub.task_multiplicity is a dict of directory keys
                num_outs *= sum(cg_sub_iter.get_task_multiplicity().values())

                # print('SchedulerGroup.get_num_outputs: cg_sub_iter.task_multiplicity: {}'.format(
                #     cg_sub_iter.get_task_multiplicity()), flush=True)

            # print('SchedulerGroup.get_num_outputs: num_outs: {}'.format(num_outs), flush=True)

            num_outs_all.append(num_outs)
            num_outs_prev = num_outs

        # print('SchedulerGroup.get_num_outputs: num_outs_all: {}'.format(
        #     num_outs_all), flush=True)

        return num_outs_all

    def has(self, command_group_submission):
        return command_group_submission in self.command_group_submissions

    def index(self, command_group_submission):
        if not self.has(command_group_submission):
            msg = '{} is not in the scheduler group.'
            raise ValueError(msg.format(command_group_submission))
        return self.command_group_submissions.index(command_group_submission)

    @classmethod
    def get_scheduler_groups(cls, submission):
        """Split the command group submissions up into scheduler groups."""

        cmd_groups_split = []
        sch_group_idx = 0

        for cg_sub in submission.command_group_submissions:

            if cg_sub.command_group.nesting == NestingType('hold'):
                sch_group_idx += 1
            if len(cmd_groups_split) == sch_group_idx + 1:
                cmd_groups_split[sch_group_idx].append(cg_sub)
            else:
                cmd_groups_split.append([cg_sub])

        return [cls(idx, i) for idx, i in enumerate(cmd_groups_split)]
