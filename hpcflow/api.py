"""`hpcflow.api.py`

This module contains the application programming interface (API) to `hpcflow`,
and includes functions that are called by the command line interface (CLI; in
`hpcflow.cli.py`).

"""

from pathlib import Path
from time import sleep
from datetime import datetime
import json


from beautifultable import BeautifulTable
from sqlalchemy.exc import OperationalError

from hpcflow.config import Config
from hpcflow.init_db import init_db
from hpcflow.models import Workflow, CommandGroupSubmission
from hpcflow.profiles import parse_job_profiles, prepare_workflow_dict
from hpcflow.project import Project
from hpcflow.archive.cloud.cloud import CloudProvider


def make_workflow(dir_path=None, profile_list=None, json_file=None, json_str=None,
                  workflow_dict=None, clean=False, config_dir=None):
    """Generate a new Workflow and add it to the local database.

    Parameters
    ----------
    dir_path : str or Path, optional
        The directory in which the Workflow will be generated. By default, this
        is the working (i.e. invoking) directory.
    profile_list : list of (str or Path), optional
        List of YAML profile file paths to use to construct the Workflow. By
        default, and if `json_file` and `json_str` and not specified, all
        YAML files in the `dir_path` directory that match the profile
        specification format in the global configuration will be parsed as
        Workflow profiles. If not None, only those profiles listed will be
        parsed as Workflow profiles.
    json_file : str or Path, optional
        Path to a JSON file that represents a Workflow. By default, set to
        `None`.
    json_str : str, optional
        JSON string that represents a Workflow. By default, set to `None`.
    workflow_dict : dict, optional
        Dict representing the workflow to generate.
    clean : bool, optional
        If True, all existing hpcflow data will be removed from `dir_path`.
        Useful for debugging.

    Returns
    -------
    workflow_id : int
        The insert ID of the Workflow object in the local database.

    Notes
    -----
    Specify only one of `profile_list`, `json_file` or `json_str`.

    """

    opts = [profile_list, json_file, json_str, workflow_dict]
    not_nones = sum([i is not None for i in opts])
    if not_nones > 1:
        msg = ('Specify only one of `profile_list`, `json_file`, `json_str` or '
               '`workflow_dict`.')
        raise ValueError(msg)

    project = Project(dir_path, config_dir, clean=clean)

    if json_str:
        workflow_dict = json.loads(json_str)

    elif json_file:
        json_file = Path(json_file).resolve()
        with Path(json_file).open() as handle:
            workflow_dict = json.load(handle)

    elif profile_list:
        profile_list = [Path(i).resolve() for i in profile_list]
        # Get workflow from YAML profiles:
        workflow_dict = parse_job_profiles(project.dir_path, profile_list)

    else:
        workflow_dict = prepare_workflow_dict(workflow_dict)

    Session = init_db(project, check_exists=False)
    session = Session()

    workflow = Workflow(directory=project.dir_path, **workflow_dict)

    session.add(workflow)
    session.commit()

    workflow_id = workflow.id_
    session.close()

    return workflow_id


def submit_workflow(workflow_id, dir_path=None, task_range='all', config_dir=None):
    """Submit (part of) a previously generated Workflow.

    Parameters
    ----------
    workflow_id : int
        The ID of the Workflow to submit, as in the local database.
    dir_path : str or Path, optional
        The directory in which the Workflow exists. By default, this is the working (i.e.
        invoking) directory.
    task_range : (list of int) or str, optional
        Specify which tasks from the initial command group to submit. If a list, it must
        have either two or three elements; if it has two elements, these signify the first
        and last tasks, inclusively, to submit. By default, the task step size is one, but
        this can be chosen as a third list entry. If a string "all", all tasks are
        submitted.
    """

    # TODO: do validation of task_ranges here? so models.workflow.add_submission
    #   always receives a definite `task_ranges`? What about if the number is
    #   indeterminate at submission time?

    if task_range == 'all' or task_range is None:
        task_range = [1, -1, 1]
    if len(task_range) == 2:
        task_range.append(1)

    # print('api.submit_workflow: task_range: {}'.format(task_range), flush=True)

    project = Project(dir_path, config_dir)
    Session = init_db(project, check_exists=True)
    session = Session()

    workflow = session.query(Workflow).get(workflow_id)
    submission = workflow.add_submission(project, task_range)

    session.commit()

    submission_id = submission.id_
    session.close()

    return submission_id


def get_workflow_ids(dir_path=None, config_dir=None):
    """Get the IDs of existing Workflows.

    Parameters
    ----------
    dir_path : str or Path, optional
        The directory in which the Workflows exist. By default, this is the
        working (i.e. invoking) directory.

    Returns
    -------
    workflow_ids : list of int
        List of IDs of Workflows.

    """

    project = Project(dir_path, config_dir)
    Session = init_db(project, check_exists=True)
    session = Session()

    workflow_ids = [i.id_ for i in session.query(Workflow.id_)]

    session.close()

    return workflow_ids


def clean(dir_path=None, config_dir=None):
    """Clean the directory of all content generated by `hpcflow`."""

    project = Project(dir_path, config_dir)
    project.clean()


def write_runtime_files(cmd_group_sub_id, task_idx, iter_idx, dir_path=None,
                        config_dir=None):
    """Write the commands files for a given command group submission.

    Parameters
    ----------
    cmd_group_sub_id : int
        ID of the command group submission for which a command file is to be
        generated.
    task_idx : int, optional
        Task ID. What is this for???
    dir_path : str or Path, optional
        The directory in which the Workflow will be generated. By default, this
        is the working (i.e. invoking) directory.

    """

    project = Project(dir_path, config_dir)
    Session = init_db(project, check_exists=True)
    session = Session()

    cg_sub = session.query(CommandGroupSubmission).get(cmd_group_sub_id)
    cg_sub.write_runtime_files(project, task_idx, iter_idx)

    session.commit()
    session.close()


def set_task_start(cmd_group_sub_id, task_idx, iter_idx, dir_path=None, config_dir=None):

    project = Project(dir_path, config_dir)
    Session = init_db(project, check_exists=True)
    session = Session()

    cg_sub = session.query(CommandGroupSubmission).get(cmd_group_sub_id)

    sleep_time = 5
    block_msg = (f'{{}} api.set_task_start: Database locked. Sleeping for {sleep_time} '
                 f'seconds')

    blocked = True
    while blocked:
        try:
            session.refresh(cg_sub)
            cg_sub.set_task_start(task_idx, iter_idx)
            session.commit()
            blocked = False
        except OperationalError:
            # Database is likely locked.
            session.rollback()
            print(block_msg.format(datetime.now()), flush=True)
            sleep(sleep_time)

    session.close()


def set_task_end(cmd_group_sub_id, task_idx, iter_idx, dir_path=None, config_dir=None):

    project = Project(dir_path, config_dir)
    Session = init_db(project, check_exists=True)
    session = Session()

    cg_sub = session.query(CommandGroupSubmission).get(cmd_group_sub_id)

    sleep_time = 5
    block_msg = (f'{{}} api.set_task_end: Database locked. Sleeping for {sleep_time} '
                 f'seconds')

    blocked = True
    while blocked:
        try:
            session.refresh(cg_sub)
            cg_sub.set_task_end(task_idx, iter_idx)
            session.commit()
            blocked = False
        except OperationalError:
            # Database is likely locked.
            session.rollback()
            print(block_msg.format(datetime.now()), flush=True)
            sleep(sleep_time)

    session.close()


def archive(cmd_group_sub_id, task_idx, iter_idx, dir_path=None, config_dir=None):
    """Initiate an archive of a given task.

    Parameters
    ----------
    cmd_group_sub_id : int
        ID of the command group submission for which an archive is to be
        started.
    task_idx : int
        The task index to be archived (or rather, the task whose working directory
        will be archived).
    dir_path : str or Path, optional
        The directory in which the Workflow will be generated. By default, this
        is the working (i.e. invoking) directory.

    """

    project = Project(dir_path, config_dir)
    Session = init_db(project, check_exists=True)
    session = Session()

    cg_sub = session.query(CommandGroupSubmission).get(cmd_group_sub_id)
    cg_sub.do_archive(task_idx, iter_idx)

    session.commit()
    session.close()


def root_archive(workflow_id, dir_path=None, config_dir=None):
    """Archive the root directory of the Workflow."""

    project = Project(dir_path, config_dir)
    Session = init_db(project, check_exists=True)
    session = Session()

    workflow = session.query(Workflow).get(workflow_id)
    workflow.do_root_archive()

    session.commit()
    session.close()


def get_scheduler_stats(cmd_group_sub_id, task_idx, iter_idx, dir_path=None,
                        config_dir=None):
    """Scrape completed task information from the scheduler.

    Parameters
    ----------
    cmd_group_sub_id : int
        ID of the command group submission for which an archive is to be
        started.
    task_idx : int
        The task index to be archived (or rather, the task whose working directory
        will be archived).
    dir_path : str or Path, optional
        The directory in which the Workflow will be generated. By default, this
        is the working (i.e. invoking) directory.

    """

    project = Project(dir_path, config_dir)
    Session = init_db(project, check_exists=True)
    session = Session()

    cg_sub = session.query(CommandGroupSubmission).get(cmd_group_sub_id)
    cg_sub.get_scheduler_stats(task_idx, iter_idx)

    session.commit()
    session.close()


def get_stats(dir_path=None, workflow_id=None, jsonable=True, datetime_dicts=False,
              config_dir=None):
    """Get task statistics (as a JSON-like dict) for a project."""

    project = Project(dir_path, config_dir)
    Session = init_db(project, check_exists=True)
    session = Session()

    all_workflow_ids = [i.id_ for i in session.query(Workflow.id_)]

    if not all_workflow_ids:
        msg = 'No workflows exist in directory: "{}"'
        raise ValueError(msg.format(project.dir_path))

    elif workflow_id:

        if workflow_id not in all_workflow_ids:
            msg = 'No workflow with ID "{}" was found in directory: "{}"'
            raise ValueError(msg.format(workflow_id, project.dir_path))

        workflow_ids = [workflow_id]

    else:
        workflow_ids = all_workflow_ids

    workflows = [session.query(Workflow).get(i) for i in workflow_ids]
    stats = [i.get_stats(jsonable=jsonable, datetime_dicts=datetime_dicts)
             for i in workflows]

    session.close()

    return stats


def get_formatted_stats(dir_path=None, workflow_id=None, max_width=100,
                        show_task_end=False, config_dir=None):
    """Get task statistics formatted like a table."""

    stats = get_stats(dir_path, workflow_id, jsonable=True, config_dir=config_dir)

    out = ''
    for workflow in stats:
        out += 'Workflow ID: {}\n'.format(workflow['workflow_id'])
        for submission in workflow['submissions']:
            out += 'Submission ID: {}\n'.format(submission['submission_id'])
            for cmd_group_sub in submission['command_group_submissions']:
                out += 'Command group submission ID: {}\n'.format(
                    cmd_group_sub['command_group_submission_id'])
                out += 'Commands:\n'
                for cmd in cmd_group_sub['commands']:
                    out += '\t{}\n'.format(cmd)
                task_table = BeautifulTable(max_width=max_width)
                task_table.set_style(BeautifulTable.STYLE_BOX)
                task_table.row_separator_char = ''
                headers = [
                    'It.',
                    '#',
                    'SID',
                    'Dir.',
                    'Start',
                    'Duration',
                    'Archive',
                    'memory',
                    'hostname',
                ]
                if show_task_end:
                    headers = headers[:5] + ['End'] + headers[5:]
                task_table.column_headers = headers
                task_table.column_alignments['Duration'] = BeautifulTable.ALIGN_RIGHT

                for task in cmd_group_sub['tasks']:
                    row = [
                        task['iteration'],
                        task['order_id'],
                        task['scheduler_id'],
                        task['working_directory'],
                        task['start_time'] or 'pending',
                        task['duration'] or '-',
                        task['archive_status'] or '-',
                        task['memory'] or '-',
                        task['hostname'] or '-',
                    ]
                    if show_task_end:
                        row = row[:5] + [task['end_time'] or '-'] + row[5:]
                    task_table.append_row(row)

                out += str(task_table) + '\n\n'

    return out


def save_stats(save_path, dir_path=None, workflow_id=None, config_dir=None):
    """Save task statistics as a JSON file."""

    stats = get_stats(dir_path, workflow_id, jsonable=True, config_dir=config_dir)

    save_path = Path(save_path)
    with save_path.open('w') as handle:
        json.dump(stats, handle, indent=4, sort_keys=True)


def kill(dir_path=None, workflow_id=None, config_dir=None):
    """Delete jobscripts associated with a given workflow."""

    project = Project(dir_path, config_dir)
    Session = init_db(project, check_exists=True)
    session = Session()

    all_workflow_ids = [i.id_ for i in session.query(Workflow.id_)]

    if not all_workflow_ids:
        msg = 'No workflows exist in directory: "{}"'
        raise ValueError(msg.format(project.dir_path))

    elif workflow_id:

        if workflow_id not in all_workflow_ids:
            msg = 'No workflow with ID "{}" was found in directory: "{}"'
            raise ValueError(msg.format(workflow_id, project.dir_path))

        workflow_ids = [workflow_id]

    else:
        workflow_ids = all_workflow_ids

    for i in workflow_ids:
        workflow = session.query(Workflow).get(i)
        workflow.kill_active()

    session.commit()
    session.close()


def update_config(name, value, config_dir=None):
    Config.update(name, value, config_dir=config_dir)


def cloud_connect(provider, config_dir=None):
    Config.set_config(config_dir)
    token_key = {'dropbox': 'dropbox_token'}[provider.lower()]
    provider = {'dropbox': CloudProvider.dropbox}[provider.lower()]
    token = Config.get(token_key)
    good = False
    if token:
        try:
            provider.check_access()
            print('Connected to cloud provider.')
            good = True
        except:
            print('Existing cloud provider key does not work.')
            pass
    if not good:
        print('Getting new cloud token.')
        token = provider.get_token()
        update_config(token_key, token, config_dir=config_dir)
