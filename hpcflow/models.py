"""`hpcflow.models.py`"""

import shutil
import re
import os
from datetime import datetime
from pathlib import Path
from math import ceil, floor
from pprint import pprint
from subprocess import run, PIPE
from time import sleep
from datetime import datetime
from shutil import ignore_patterns

from sqlalchemy import (
    Table, Column, Integer, DateTime, JSON, ForeignKey, Boolean, Enum, String,
    select, UniqueConstraint
)
from sqlalchemy.orm import relationship, deferred, Session
from sqlalchemy.exc import IntegrityError, OperationalError

from hpcflow import CONFIG
from hpcflow.base_db import Base
from hpcflow.nesting import NestingType
from hpcflow.validation import validate_task_multiplicity
from hpcflow.variables import (
    select_cmd_group_var_names, select_cmd_group_var_definitions,
    extract_variable_names, resolve_variable_values, UnresolvedVariableError
)
from hpcflow.utils import coerce_same_length, zeropad
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


class Workflow(Base):
    """Class to represent a Workflow."""

    __tablename__ = 'workflow'

    id_ = Column('id', Integer, primary_key=True)
    create_time = Column(DateTime)
    pre_commands = Column(JSON)
    _directory = Column(String(255))

    command_groups = relationship(
        'CommandGroup',
        back_populates='workflow',
        order_by='CommandGroup.exec_order',
    )
    submissions = relationship('Submission', back_populates='workflow')
    variable_definitions = relationship('VarDefinition',
                                        back_populates='workflow')

    def __init__(self, directory, command_groups, var_definitions=None,
                 pre_commands=None, archives=None):
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
            of glob patterns to ignore when archiving).
        """

        # Command group directories must always be variables:
        cmd_group_dir_vars = []
        for idx, i in enumerate(command_groups):

            dir_var_value = '{}'

            if 'directory' in i:

                var_names = extract_variable_names(
                    i['directory'], CONFIG['variable_delimiters'])
                if len(var_names) > 1:
                    raise NotImplementedError()
                elif not var_names:
                    # Value is set but is not a variable
                    dir_var_value = i['directory'] or dir_var_value
                else:
                    # Value is already a variable; no action.
                    continue

            dir_var_defn_name = CONFIG['default_cmd_group_dir_var_name']

            command_groups[idx]['directory'] = '{1:}{0:}{2:}'.format(
                dir_var_defn_name,
                *CONFIG['variable_delimiters']
            )

            # Add new variable definition:
            var_definitions.update({
                dir_var_defn_name: {
                    'value': dir_var_value,
                }
            })

        self.variable_definitions = [
            VarDefinition(name=k, **v) for k, v in var_definitions.items()
        ]

        # Generate Archive objects:
        if archives:
            archive_objs = [Archive(**kwargs) for kwargs in archives]

        cmd_groups = []
        for i in command_groups:

            dir_var_name = extract_variable_names(
                i['directory'], CONFIG['variable_delimiters'])[0]

            dir_var_defn = [i for i in self.variable_definitions
                            if i.name == dir_var_name][0]

            i.pop('directory')
            i.update({
                'directory_var': dir_var_defn,
            })
            arch_idx = i.pop('archive_idx', None)
            if arch_idx is not None:
                i.update({
                    'archive': archive_objs[arch_idx]
                })
            cmd_groups.append(CommandGroup(**i))

        self.command_groups = cmd_groups
        self.create_time = datetime.now()
        self.pre_commands = pre_commands
        self._directory = str(directory)

        self.validate()

        self._execute_pre_commands()

    def get_variable_definition_by_name(self, variable_name):
        """Get the VarDefintion object using the variable name."""

        for i in self.variable_definitions:
            if i.name == variable_name:
                return i

        msg = ('Cannot find variable definition with '
               'name "{}"'.format(variable_name))
        raise ValueError(msg)

    @property
    def directory(self):
        return Path(self._directory)

    def validate(self):
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

    def get_scheduler_groups(self, submission):
        """Resolve the scheduler groups for the Workflow.

        Command groups that belong to the same scheduler group form a
        consecutive subset of the Workflow command groups, and must be
        submitted to the scheduler with the same task range (but potentially
        different task step sizes) in order to use "chunking". This function
        firstly order command groups into scheduler groups, and secondly
        resolves required the task range for each scheduler group.

        Parameters
        ----------
        submission : Submission

        Returns
        -------
        scheduler_groups : list of dict
            List of length equal to the number of command groups, with keys:
                scheduler_group_idx : int
                task_range : list of length three of int

        """

        # First group command groups into scheduler groups:
        scheduler_groups = {
            'command_groups': [],
            'max_num_tasks': [],
        }
        sch_group_cmd_groups = []
        sch_group_idx = 0
        for i_idx, i in enumerate(self.command_groups):

            if i.nesting == NestingType('hold'):
                sch_group_idx += 1

            scheduler_groups['command_groups'].append({
                'scheduler_group_idx': sch_group_idx,
            })

            if len(sch_group_cmd_groups) == sch_group_idx + 1:
                sch_group_cmd_groups[sch_group_idx].append(i_idx)
            else:
                sch_group_cmd_groups.append([i_idx])

        # Now resolve the required task range for each command group:
        for sch_group_idx, cmd_groups in enumerate(sch_group_cmd_groups):

            # Num. of output tasks for each cmd group in this scheduler group:
            num_out_all = []

            for idx, cg_idx in enumerate(cmd_groups):

                # submission.command_group_submissions are ordered by
                # command group exec_order, so we can index directly:
                cg_sub = submission.command_group_submissions[cg_idx]

                prev_num_outs = num_out_all[-1][1] if num_out_all else None
                num_outs = cg_sub.get_num_outputs(prev_num_outs)
                num_out_all.append((cg_idx, num_outs))

            max_num_out = max([i[1] for i in num_out_all])
            scheduler_groups['max_num_tasks'].append(max_num_out)

            for cg_idx, num_outs in num_out_all:

                scheduler_groups['command_groups'][cg_idx].update({
                    'task_step_size': int(max_num_out / num_outs),
                })

        return scheduler_groups

    def add_submission(self, project, task_ranges=None):
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

        sub = Submission()

        # Add the new submission to this Workflow:
        self.submissions.append(sub)

        cmd_group_subs = []
        for idx, i in enumerate(self.command_groups):
            cg_sub = CommandGroupSubmission()
            i.command_group_submissions.append(cg_sub)
            sub.command_group_submissions.append(cg_sub)
            cmd_group_subs.append(cg_sub)

        sub.resolve_variable_values(self.directory)

        sch_groups = self.get_scheduler_groups(sub)

        submit_dir = sub.write_submit_dirs(
            self.id_, project.hf_dir, sch_groups)

        # Write a jobscript for each command group submission:
        js_paths = []
        for idx, i in enumerate(cmd_group_subs):
            js_kwargs = sch_groups['command_groups'][idx]
            js_kwargs.update({
                'max_num_tasks': sch_groups['max_num_tasks'][
                    js_kwargs['scheduler_group_idx']]
            })
            js_paths.append(
                i.write_jobscript(**js_kwargs, dir_path=submit_dir)
            )

        last_submit_id = None

        sumbit_cmd = os.getenv('HPCFLOW_QSUB_CMD', 'qsub')

        assert len(js_paths) == len(cmd_group_subs)
        for idx, (js_path, cg_sub) in enumerate(zip(js_paths, cmd_group_subs)):

            qsub_cmd = [sumbit_cmd]

            if idx > 0:

                # Add conditional submission:
                if cg_sub.command_group.nesting == NestingType('hold'):
                    hold_arg = '-hold_jid'
                else:
                    hold_arg = '-hold_jid_ad'

                qsub_cmd += [hold_arg, last_submit_id]

            qsub_cmd.append(str(js_path))
            proc = run(qsub_cmd, stdout=PIPE, stderr=PIPE)
            qsub_out = proc.stdout.decode()
            qsub_err = proc.stderr.decode()

            print('qsub_out: {}'.format(qsub_out))

            # Extract newly submitted job ID:
            pat = r'[0-9]+'
            job_id_search = re.search(pat, qsub_out)
            try:
                job_id_str = job_id_search.group()

            except AttributeError:
                msg = ('Could not retrieve the job ID from the submitted '
                       'jobscript found at {}. No more jobscripts will be '
                       'submitted.')
                raise ValueError(msg.format(js_path))

            last_submit_id = job_id_str

        return sub

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


class CommandGroup(Base):
    """Class to represent a command group, which is roughly translated into a
    job script."""

    __tablename__ = 'command_group'

    id_ = Column('id', Integer, primary_key=True)
    workflow_id = Column(Integer, ForeignKey('workflow.id'))
    directory_variable_id = Column(Integer, ForeignKey('var_definition.id'))
    archive_id = Column(Integer, ForeignKey('archive.id'), nullable=True)

    commands = Column(JSON)
    is_job_array = Column(Boolean)
    exec_order = Column(Integer)
    nesting = Column(Enum(NestingType), nullable=True)
    modules = Column(JSON, nullable=True)
    scheduler_options = Column(JSON, nullable=True)
    profile_name = Column(String(255), nullable=True)
    profile_order = Column(Integer, nullable=True)
    archive_excludes = Column(JSON, nullable=True)

    archive = relationship('Archive', back_populates='command_groups')
    workflow = relationship('Workflow', back_populates='command_groups')
    command_group_submissions = relationship('CommandGroupSubmission',
                                             back_populates='command_group')

    directory_variable = relationship('VarDefinition')

    def __init__(self, commands, directory_var, is_job_array=True,
                 exec_order=None, nesting=None, modules=None,
                 scheduler_options=None, profile_name=None,
                 profile_order=None, archive=None, archive_excludes=None):
        """Method to initialise a new CommandGroup.

        Parameters
        ----------
        commands : list of str
            List of commands to sequentially execute.
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
        modules : list of str, optional
            List of modules that are to be loaded using the `module load`
            command, in order to ensure successful execution of the commands.
            By default set to `None`, such that no modules are loaded.
        scheduler_options : dict, optional
            Options to be passed directly to the scheduler. By default, `None`,
            in which case no options are passed to the scheduler.
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

        TODO: document how `nesting` interacts with `is_job_array`.

        """

        scheduler_options.update({'cwd': ''})

        self.commands = commands
        self.is_job_array = is_job_array
        self.exec_order = exec_order
        self.nesting = nesting
        self.modules = modules
        self.scheduler_options = scheduler_options
        self.directory_variable = directory_var
        self.profile_name = profile_name
        self.profile_order = profile_order

        self.archive = archive
        self.archive_excludes = archive_excludes

        self.validate()

    def validate(self):

        # Check at least one command:
        if not self.commands:
            msg = 'At least one command must be specified.'
            raise ValueError(msg)

        # Check non-empty commands exist:
        for cmd_idx, cmd in enumerate(self.commands):
            if not cmd:
                msg = 'Command #{} is empty; a command must be specified.'
                raise ValueError(msg.format(cmd_idx))

        self.nesting = NestingType[self.nesting] if self.nesting else None

    @property
    def variable_names(self):
        """Get those variable names associated with this command group."""

        var_names = select_cmd_group_var_names(
            self.commands, self.directory_variable.value)
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
                'value': i.value,
            }
            for i in self.workflow.variable_definitions
        }

        cmd_group_var_defns = select_cmd_group_var_definitions(
            var_defns_dict,
            self.commands,
            self.directory_variable.value,
        )

        var_defns = [
            i for i in self.workflow.variable_definitions
            if i.name in cmd_group_var_defns
        ]

        return var_defns

    @property
    def cmd_var_names(self):
        """Get those variables definitions whose names appear directly in
        the commands.

        This excludes variable definitions that are "sub-variables".

        TODO: delete this? This is the same as self.variable_names but this
        excludes any directory variables.

        """

        cmd_var_names = []
        for i in self.commands:
            cmd_var_names.extend(
                extract_variable_names(i, CONFIG['variable_delimiters'])
            )

        return cmd_var_names


class VarDefinition(Base):
    """Class to represent a variable definition."""

    __tablename__ = 'var_definition'

    id_ = Column('id', Integer, primary_key=True)
    workflow_id = Column('workflow_id', Integer, ForeignKey('workflow.id'))

    name = Column(String(255))
    data = Column(JSON, nullable=True)
    file_regex = Column(JSON, nullable=True)
    value = Column(String(255), nullable=True)

    workflow = relationship('Workflow', back_populates='variable_definitions')
    variable_values = relationship(
        'VarValue', back_populates='variable_definition')

    def __init__(self, name, data=None, file_regex=None, value=None):

        self.name = name
        self.data = data
        self.file_regex = file_regex
        self.value = value

    def is_base_variable(self):
        """Check if the variable depends on any other variables."""

        if extract_variable_names(self.value,
                                  CONFIG['variable_delimiters']):
            return False
        else:
            return True

    def get_dependent_variable_names(self):
        """Get the names of variables on which this variable depends."""
        return extract_variable_names(self.value,
                                      CONFIG['variable_delimiters'])

    def get_multiplicity(self, submission):
        """Get the value multiplicity of this variable for a given
        submission.

        TODO: this should first try to get multiplicity from values (as a
        function of cmd group directory?)

        """

        var_length = None

        if self.data:
            var_length = len(self.data)

        elif self.file_regex:

            if 'subset' in self.file_regex:
                var_length = len(self.file_regex['subset'])

            elif 'expected_multiplicity' in self.file_regex:
                var_length = self.file_regex['expected_multiplicity']

        elif self.is_base_variable():
            var_length = 1

        else:
            raise ValueError('bad 3!')

        return var_length

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

        elif self.data:
            for i in self.data:
                vals.append(self.value.format(i))

        else:
            vals.append(self.value)

        if not vals:
            msg = ('Cannot resolve variable value with name: {}')
            raise UnresolvedVariableError(msg.format(self.name))

        return vals


class Submission(Base):
    """Class to represent the submission of (part of) a workflow."""

    __tablename__ = 'submission'

    id_ = Column('id', Integer, primary_key=True)
    workflow_id = Column(Integer, ForeignKey('workflow.id'))
    submit_time = Column(DateTime)

    workflow = relationship('Workflow', back_populates='submissions')
    command_group_submissions = relationship(
        'CommandGroupSubmission',
        back_populates='submission',
        order_by='CommandGroupSubmission.command_group_exec_order',
    )

    variable_values = relationship('VarValue', back_populates='submission')

    def __init__(self):

        self.submit_time = datetime.now()

    def get_variable_values_by_name(self, variable_name):
        """Get a list of VarValues for a given variable name.

        TODO: this gets for all submissions?
        """

        var_defn = self.workflow.get_variable_definition_by_name(variable_name)

        return var_defn.variable_values()

    def get_variable_values(self, variable_definition):
        """Get the values of a given variable definition for this
        submission."""

        var_vals = []
        for i in self.variable_values:
            if i.variable_definition == variable_definition:
                var_vals.append(i)

        return var_vals

    def is_variable_name_resolved(self, variable_name):
        """Returns True if the variable name refers to a variable that has
        been resolved."""

        var_defn = self.workflow.get_variable_definition_by_name(variable_name)

        return self.is_variable_resolved(var_defn)

    def is_variable_resolved(self, variable_definition,
                             directory_var_val=None):
        """Returns True if the passed variable_definition has been resolved
        for this Submission."""
        # Check the variable definition is part of the workflow:
        if variable_definition not in self.workflow.variable_definitions:
            msg = ('Passed variable_definition object is not in the '
                   ' workflow of this submission.')
            raise ValueError(msg)

        for i in self.variable_values:
            if i.variable_definition == variable_definition:
                if directory_var_val:
                    if i.directory_value == directory_var_val:
                        return True
                else:
                    return True

        return False

    def resolve_variable_values(self, root_directory):
        """Attempt to resolve as many variable values in the Workflow as
        possible."""

        session = Session.object_session(self)

        # Loop through CommandGroupSubmissions in order:
        for i in self.command_group_submissions:

            # VarValues representing the command group directories:
            cg_dirs_var_vals = []

            dir_var = i.command_group.directory_variable
            if not dir_var.variable_values:

                # Directory variable has not yet been resolved; try:
                try:
                    dir_var_vals_dat = dir_var.get_values(root_directory)
                except UnresolvedVariableError:
                    # Move on to next command group:
                    continue

                # Add VarVals:
                for val_idx, val in enumerate(dir_var_vals_dat):
                    cg_dirs_var_vals.append(
                        VarValue(val, val_idx, dir_var, self)
                    )
            else:
                cg_dirs_var_vals = dir_var.variable_values

            var_defns_rec = i.command_group.variable_definitions_recursive

            for j in cg_dirs_var_vals:

                var_vals_dat = resolve_variable_values(
                    var_defns_rec, Path(j.value))

                for k, v in var_vals_dat.items():

                    vals_dat = v['vals']
                    var_defn = self.workflow.get_variable_definition_by_name(k)

                    if not self.is_variable_resolved(var_defn, j):
                        for val_idx, val in enumerate(vals_dat):
                            VarValue(val, val_idx, var_defn,
                                     self, directory_value=j)
                            session.commit()

    def write_submit_dirs(self, workflow_id, hf_dir, scheduler_groups):
        """Write the directory structure necessary for this submission."""

        wf_path = hf_dir.joinpath('workflow_{}'.format(workflow_id))
        if not wf_path.exists():
            wf_path.mkdir()

        submit_path = wf_path.joinpath('submit_{}'.format(self.id_))
        submit_path.mkdir()

        num_sch_groups = len(scheduler_groups['max_num_tasks'])
        for i in range(num_sch_groups):
            sg_path = submit_path.joinpath('scheduler_group_{}'.format(i))
            sg_path.mkdir()

            max_tasks_i = scheduler_groups['max_num_tasks'][i]

            # loop through cmd groups in this scheduler group:
            for cmd_group_idx, j in enumerate(scheduler_groups['command_groups']):

                if j['scheduler_group_idx'] == i:

                    cg_sub = self.command_group_submissions[cmd_group_idx]
                    dir_vals = cg_sub.directory_values
                    all_dir_slots = [''] * max_tasks_i
                    # Distribute dirs over num_dir_slots:
                    for k in range(0, max_tasks_i, j['task_step_size']):
                        dir_idx = floor((k / max_tasks_i) * len(dir_vals))
                        all_dir_slots[k] = dir_vals[dir_idx]

                    wk_dirs_path = submit_path.joinpath(
                        'working_dirs_{}{}'.format(
                            cmd_group_idx, CONFIG['working_dirs_file_ext']))

                    with wk_dirs_path.open('w') as handle:
                        for dir_path in all_dir_slots:
                            handle.write('{}\n'.format(dir_path))

            var_values_path = sg_path.joinpath('var_values')
            var_values_path.mkdir()

            for j in range(max_tasks_i):
                j_fmt = zeropad(j + 1, max_tasks_i + 1)
                vv_j_path = var_values_path.joinpath(j_fmt)
                vv_j_path.mkdir()

        return submit_path


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

    command_group = relationship('CommandGroup',
                                 back_populates='command_group_submissions')

    submission = relationship('Submission',
                              back_populates='command_group_submissions')

    command_group_exec_order = deferred(
        select([CommandGroup.exec_order]).where(
            CommandGroup.id_ == command_group_id))

    is_command_writing = relationship(
        'IsCommandWriting',
        uselist=False,
        cascade='all, delete, delete-orphan'
    )

    @property
    def variable_values(self):

        var_values = []
        for i in self.command_group.variable_definitions:
            if i.variable_values:
                var_values.append(i)

        return var_values

    def get_variable_values_normed(self):
        """Returns a list of dict whose keys are variable names."""

        if not self.all_variables_resolved:
            msg = ('Not all variable have been resolved for this command group'
                   ' submission; cannot get normed variable values.')
            raise ValueError(msg)

        all_var_vals_dat = []

        var_vals_dat_ii = {}
        for i in self.command_group.variable_definitions:

            var_vals_i = self.submission.get_variable_values(i)

            cg_dirs = self.command_group.directory_variable.variable_values
            for j in var_vals_i:
                val_dir_val = j.directory_value

                for k in cg_dirs:

                    if val_dir_val == k:

                        if val_dir_val.value in var_vals_dat_ii:

                            if i.name in var_vals_dat_ii[val_dir_val.value]:

                                var_vals_dat_ii[val_dir_val.value][i.name].append(
                                    j.value)
                            else:
                                var_vals_dat_ii[val_dir_val.value].update(
                                    {i.name: [j.value]})
                        else:
                            var_vals_dat_ii.update({
                                val_dir_val.value: {i.name: [j.value]}
                            })

        var_vals_normed_all = []

        for cg_dir, var_vals in var_vals_dat_ii.items():

            only_names = []
            only_vals = []
            for k, v in var_vals.items():
                only_vals.append(v)
                only_names.append(k)

            only_vals_uniform = coerce_same_length(only_vals)

            for i in list(map(list, zip(*only_vals_uniform))):

                var_vals_normed = {
                    'directory': cg_dir,
                    'vals': {}
                }

                for j, k in zip(only_names, i):
                    var_vals_normed['vals'].update({j: k})

                var_vals_normed_all.append(var_vals_normed)

        return var_vals_normed_all

    @property
    def all_variables_resolved(self):
        """Returns True if all variables associated with this command group
        submission have been resolved."""

        for i in self.command_group.variable_definitions:
            if not self.submission.is_variable_resolved(i):
                return False

        return True

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
    def directory_values(self):
        """Get the directories associated with this command group 
        submission."""

        dir_vals = [i.value for i in self.directories]
        return dir_vals

    @property
    def directories(self):
        """Get the directory variable values associated with this command group 
        submission.

        TODO: do we need to order these?

        """

        dir_vars_all = self.command_group.directory_variable.variable_values
        # Get only those with correct submission
        dirs = []
        for i in dir_vars_all:
            if i.submission == self.submission:
                dirs.append(i)

        return dirs

    def get_var_definition_by_name(self, var_name):
        """"""

        for i in self.command_group.var_definitions:
            if i.name == var_name:
                return i

    def get_task_multiplicity(self):
        """Get the number of tasks associated with this command group
        submission."""

        task_multi_all = {}
        for i in self.command_group.variable_definitions:
            var_length = i.get_multiplicity(self.submission)
            task_multi_all.update({i.name: var_length})

        uniq_lens = set(task_multi_all.values())
        num_uniq_lens = len(uniq_lens)
        if num_uniq_lens == 1:
            task_multi = min(uniq_lens)
        elif num_uniq_lens == 2:
            if min(uniq_lens) != 1:
                raise ValueError('bad 4!')
            task_multi = max(uniq_lens)
        else:
            raise ValueError('bad 5!')

        return task_multi

    def get_num_outputs(self, num_inputs=None):
        """Get the number of output tasks of this command group.

        Parameters
        ----------
        num_inputs : int, optional
            Number of input tasks to this command group. In other words, the
            number of output tasks of the command group with the next-lowest
            `exec_order`. If None, assume this is the first command group.

        Returns
        -------
        num_outputs : int

        TODO: problem here; if first command group in the scheduler group,
        not necessarily the first command group of the submission (so vars
        might not be resolved). For all comman groups, should try to see if
        vars are resolved actually; checking the directory var should be 
        included. Try to get num var values by checking for resolved values
        and other things (expected multiplicity, etc).

        So I guess this function should not make any assumptions about
        whether var values are resolved or not; it should just try and see for
        itself (and so it shouldn't matter which command group it is.) So I 
        guess we can keep `num_inputs` to be None if it's the first cmd group
        in a scheduler group, because in all such cases, the number of outputs
        is unrelated to the number of inputs.

        TODO: logical refactoring.

        """

        if self.all_variables_resolved:

            if self.command_group.is_job_array:
                var_vals = self.get_variable_values_normed()
                num_outputs = len(var_vals)

            elif self.command_group.is_job_array == False:

                if self.command_group.nesting == NestingType('nest'):
                    num_outputs = num_inputs
                elif self.command_group.nesting == NestingType('hold'):
                    num_outputs = 1
                elif self.command_group.nesting is None:
                    num_outputs = 1

            else:
                num_outputs = num_inputs

        else:

            if self.command_group.nesting is None:

                if num_inputs:
                    num_outputs = num_inputs
                    return num_outputs

            if self.command_group.is_job_array == False:
                # Not job array:

                if self.command_group.nesting == NestingType('nest'):
                    num_outputs = num_inputs
                elif self.command_group.nesting == NestingType('hold'):
                    num_outputs = 1
                elif self.command_group.nesting is None:
                    num_outputs = 1

            else:

                # Job array; need to find task multiplicity:
                task_multi = self.get_task_multiplicity()

                if self.command_group.nesting == NestingType('nest'):
                    num_outputs = num_inputs * task_multi
                elif self.command_group.nesting == NestingType('hold'):
                    num_outputs = task_multi
                elif self.command_group.nesting is None:
                    num_outputs = task_multi

        return num_outputs

    def write_jobscript(self, task_step_size, max_num_tasks,
                        scheduler_group_idx, dir_path):
        """Write the jobscript."""

        js_ext = CONFIG.get('jobscript_ext', '')
        js_name = 'js_{}'.format(self.command_group_exec_order)
        js_fn = js_name + js_ext
        js_path = dir_path.joinpath(js_fn)

        cmd_name = 'cmd_{}'.format(self.command_group_exec_order)
        cmd_fn = cmd_name + js_ext
        cmd_path = dir_path.joinpath(cmd_fn)

        submit_dir_relative = dir_path.relative_to(
            self.submission.workflow.directory).as_posix()

        wk_dirs_path = ('${{SUBMIT_DIR}}'
                        '/working_dirs_{}{}').format(
                            self.command_group_exec_order,
                            CONFIG['working_dirs_file_ext']
        )

        cmd_group = self.command_group

        shebang = ['#!/bin/bash --login']

        sge_opts = ['#$ -{} {}'.format(k, v).strip()
                    for k, v in sorted(cmd_group.scheduler_options.items())]

        arr_opts = [
            '#$ -t 1-{}:{}'.format(max_num_tasks, task_step_size)
        ]

        define_dirs = [
            'ROOT_DIR=`pwd`',
            'SUBMIT_DIR=$ROOT_DIR/{}'.format(submit_dir_relative),
            'INPUTS_DIR=`sed -n "${{SGE_TASK_ID}}p" {}`'.format(wk_dirs_path),
            'LOG_PATH=$SUBMIT_DIR/log_{}.$SGE_TASK_ID'.format(
                cmd_group.exec_order)
        ]

        write_cmd_exec = [
            ('hpcflow write-cmd -d `pwd` {0:} > '
             '$LOG_PATH 2>&1').format(self.id_),
        ]

        loads = ['module load {}'.format(i)
                 for i in sorted(cmd_group.modules)]

        cmd_exec = [
            'cd $INPUTS_DIR',
            '. $SUBMIT_DIR/{}'.format(cmd_fn)
        ]

        arch_lns = []
        if self.command_group.archive:
            arch_lns = [
                ('hpcflow archive -d $ROOT_DIR -t $SGE_TASK_ID {0:} >> '
                 '$LOG_PATH 2>&1'.format(self.id_)),
                ''
            ]

        js_lines = (shebang + [''] +
                    sge_opts + [''] +
                    arr_opts + [''] +
                    define_dirs + [''] +
                    write_cmd_exec + [''] +
                    loads + [''] +
                    cmd_exec + [''] +
                    arch_lns)

        # Write jobscript:
        with js_path.open('w') as handle:
            handle.write('\n'.join(js_lines))

        return js_path

    def write_cmd(self, project):
        """Write all files necessary to execute the commands of this command
        group."""

        session = Session.object_session(self)

        if not self.commands_written:

            sleep_time = 5
            blocked = True
            while blocked:

                print('{} Writing command file blocked...'.format(
                    datetime.now()), flush=True)

                session.refresh(self)

                if self.is_command_writing:
                    sleep(sleep_time)

                else:
                    try:
                        self.is_command_writing = IsCommandWriting()
                        session.commit()
                        blocked = False

                    except IntegrityError:
                        # Another process has already set `is_command_writing`
                        session.rollback()
                        sleep(sleep_time)

                    except OperationalError:
                        # Database is likely locked.
                        session.rollback()
                        sleep(sleep_time)

                    if not blocked:

                        if not self.commands_written:

                            self._write_cmd(project)
                            self.commands_written = True

                        self.is_command_writing = None
                        session.commit()

    def _write_cmd(self, project):

        sub = self.submission
        sub.resolve_variable_values(project.dir_path)

        if not self.all_variables_resolved:
            msg = ('Not all variables values have been resolved for this command '
                   'group; cannot write command files.')
            raise ValueError(msg)

        var_vals = self.get_variable_values_normed()

        sch_groups = sub.workflow.get_scheduler_groups(sub)
        sch_group_cmd_group = sch_groups['command_groups'][
            self.command_group_exec_order]
        sch_group_idx = sch_group_cmd_group['scheduler_group_idx']
        task_step_size = sch_group_cmd_group['task_step_size']

        sub_dir = project.hf_dir.joinpath(
            'workflow_{}'.format(sub.workflow.id_),
            'submit_{}'.format(sub.id_)
        )

        sch_group_dir = sub_dir.joinpath(
            'scheduler_group_{}'.format(sch_group_idx))

        self.write_variable_files(var_vals, task_step_size, sch_group_dir)
        self.write_cmd_file(sch_group_idx, sub_dir)

    def write_variable_files(self, var_vals, task_step_size, sch_group_dir):

        task_multi = self.get_task_multiplicity()
        var_group_len = 1 if self.command_group.is_job_array else task_multi

        task_idx = 0
        var_vals_file_dat = {}
        for idx in range(0, len(var_vals), var_group_len):

            var_vals_sub = [i for i_idx, i in enumerate(var_vals)
                            if i_idx in range(idx, idx + var_group_len)]

            var_vals_file_dat.update({
                task_idx: {}
            })

            for i in var_vals_sub:

                for var_name, var_val in i['vals'].items():

                    if var_name in var_vals_file_dat[task_idx]:
                        var_vals_file_dat[task_idx][var_name].append(var_val)
                    else:
                        var_vals_file_dat[task_idx].update({
                            var_name: [var_val]
                        })

            task_idx += task_step_size

        for task_idx, var_vals_file in var_vals_file_dat.items():

            var_vals_dir = sch_group_dir.joinpath(
                'var_values', str(task_idx + 1))

            for var_name, var_val_all in var_vals_file.items():

                var_fn = 'var_{}{}'.format(
                    var_name, CONFIG['variable_file_ext'])
                var_file_path = var_vals_dir.joinpath(var_fn)

                with var_file_path.open('w') as handle:
                    for i in var_val_all:
                        handle.write('{}\n'.format(i))

    def write_cmd_file(self, scheduler_group_idx, dir_path):

        lns_while_start = [
            'while true',
            'do'
        ]

        delims = CONFIG['variable_delimiters']
        lns_cmd = [
            '\t' + i.replace(delims[0], '${').replace(delims[1], '}')
            for i in self.command_group.commands
        ]

        lns_while_end = [
            'done \\'
        ]

        lns_read = []
        lns_fds = []

        for idx, i in enumerate(self.command_group.variable_definitions):

            fd_idx = idx + 3

            var_fn = 'var_{}{}'.format(i.name, CONFIG['variable_file_ext'])
            var_file_path = ('$SUBMIT_DIR/scheduler_group_{}/var_values'
                             '/$SGE_TASK_ID/{}').format(
                scheduler_group_idx, var_fn)

            lns_read.append('\tread -u{} {} || break'.format(fd_idx, i.name))

            if idx > 0:
                lns_fds[-1] += ' \\'

            lns_fds.append('\t{}< {}'.format(fd_idx, var_file_path))

        cmd_lns = (lns_while_start + [''] +
                   lns_read + [''] +
                   lns_cmd + [''] +
                   lns_while_end +
                   lns_fds + [''])

        cmd_lns = '\n'.join(cmd_lns)

        cmd_fn = 'cmd_{}{}'.format(self.command_group_exec_order,
                                   CONFIG['jobscript_ext'])
        cmd_path = dir_path.joinpath(cmd_fn)
        with cmd_path.open('w') as handle:
            handle.write(cmd_lns)

    def archive(self, task_idx):
        """Archive the working directory associated with a given task in this
        command group submission."""

        sub = self.submission
        scheduler_groups = sub.workflow.get_scheduler_groups(sub)

        sch_group = scheduler_groups['command_groups'][
            self.command_group_exec_order]

        task_step_size = sch_group['task_step_size']

        max_num_tasks = scheduler_groups['max_num_tasks'][
            sch_group['scheduler_group_idx']]

        dir_idx = floor(
            ((task_idx - 1) / max_num_tasks) * len(self.directories))

        dir_val = self.directories[dir_idx]

        exclude = self.command_group.archive_excludes
        self.command_group.archive.execute(dir_val, exclude)


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
    directory_value_id = Column('directory_value_id', Integer,
                                ForeignKey('var_value.id'))

    variable_definition = relationship(
        'VarDefinition', back_populates='variable_values')
    submission = relationship('Submission', back_populates='variable_values')
    directory_value = relationship('VarValue', uselist=False, remote_side=id_)

    def __init__(self, value, order_id, var_definition, submission,
                 directory_value=None):

        self.value = value
        self.order_id = order_id
        self.variable_definition = var_definition
        self.submission = submission
        self.directory_value = directory_value


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

        """

        # TODO: later (safely) copy the database to archive as well?
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


class IsCommandWriting(Base):
    """Class to represent active writing of a command file."""

    __tablename__ = 'is_command_writing'

    command_group_submission_id = Column(
        Integer,
        ForeignKey('command_group_submission.id'),
        primary_key=True,
        unique=True
    )


class Project(object):

    DB_URI = 'sqlite:///{}/workflows.db'

    def __init__(self, dir_path, clean=False):

        self.dir_path = Path(dir_path or '').resolve()
        self.hf_dir = self.dir_path.joinpath(CONFIG['hpcflow_directory'])
        self.db_uri = Project.DB_URI.format(self.hf_dir.as_posix())

        if clean:
            self.clean()

        if not self.hf_dir.exists():
            self.hf_dir.mkdir()

    def clean(self):
        if self.hf_dir.exists():
            shutil.rmtree(str(self.hf_dir))
