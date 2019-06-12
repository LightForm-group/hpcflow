"""`hpcflow.validation.py`

These validation functions are invoked during initialisation of the `Workflow`
and `CommandGroup` instances. They are extracted out as functions here to allow
re-use by the `visualise` module, whose code may be invoked without necessarily
constructing `Workflow` and `CommandGroup` instances.

"""

from hpcflow.nesting import NestingType

PROFILE_KEYS_REQ = [
    'command_groups',
    'profile_name',
    'profile_order',
]

PROFILE_KEYS_GOOD = PROFILE_KEYS_REQ + [
    'alternate_scratch',
    'archive',
    'directory',
    'inherits',
    'is_job_array',
    'modules',
    'nesting',
    'scheduler_options',
    'pre_commands',
    'variable_scope',
    'variables',
]

CMD_GROUP_KEYS_REQ = [
    'commands',
]

CMD_GROUP_KEYS_GOOD = CMD_GROUP_KEYS_REQ + [
    'directory',
    'is_job_array',
    'modules',
    'nesting',
    'scheduler_options',
    'profile_name',
    'profile_order',
    'exec_order',
]

SHARED_KEYS_GOOD = list(set(PROFILE_KEYS_GOOD) & set(CMD_GROUP_KEYS_GOOD))

CMD_GROUP_DEFAULTS = {
    'is_job_array': True,
    'nesting': None,
    'directory': '',
}


def validate_workflow(workflow_dict):
    """Validate the `nesting`, `order` and `num_tasks` for each in a list of
    workflow command groups."""

    return workflow_dict


def validate_job_profile(job_profile):
    """Validate the keys of a job profile.

    Parameters
    ----------
    job_profile : dict
        Dictionary representing a job profile.

    Returns
    -------
    job_profile : dict
        Dictionary representing a job profile.

    """

    # Validate profile keys:
    keys = job_profile.keys()
    missing_keys = list(set(PROFILE_KEYS_REQ) - set(keys))
    bad_keys = list(set(keys) - set(PROFILE_KEYS_GOOD))

    # Check required keys exist:
    if len(missing_keys) > 0:
        msg = ('Job profile is missing required key(s): {}')
        raise ValueError(msg.format(missing_keys))

    # Check for unknown keys:
    if len(bad_keys) > 0:
        msg = ('Job profile contains unknown key(s): {}')
        raise ValueError(msg.format(bad_keys))

    return job_profile


def validate_job_profile_list(job_profiles):
    """Validate a list of job profiles and sort by `profile_order`.

    Parameters
    ----------
    job_profiles : list of dict
        List of dictionaries representing job profiles.

    Returns
    -------
    job_profiles : list of dict
        List of dictionaries representing job profiles, sorted by
        `profile_order`.

    """

    # Some validation:
    profile_orders = []
    profile_names = []
    for i in job_profiles:

        # Check all profiles have a name:
        if i.get('profile_name') is None:
            msg = ('`profile_name` must be set for each job profile.')
            raise ValueError(msg)
        else:
            profile_names.append(i['profile_name'])

        if len(job_profiles) > 1:
            # Check all profiles have an order:
            if i.get('profile_order') is None:
                msg = ('`profile_order` must be set for each profile if there '
                       'are multiple job profiles, but was not specified for '
                       'the job profile "{}"')
                raise ValueError(msg.format(i['profile_name']))
            else:
                profile_orders.append(i['profile_order'])

    if len(job_profiles) > 1:
        # Check distinct profile names:
        if len(set(profile_names)) < len(job_profiles):
            msg = ('Each job profile must have a distinct `profile_name` if '
                   'there are multiple job profiles, but job profile names '
                   'are: {}')
            raise ValueError(msg.format(profile_names))

        # Check distinct profile orders:
        if len(set(profile_orders)) < len(job_profiles):
            msg = ('Each job profile must have a distinct `profile_order` if '
                   'there are multiple job profiles, but job profile orders '
                   'are: {}')
            raise ValueError(msg.format(profile_orders))

        # Sort by profile order:
        job_profiles = sorted(job_profiles, key=lambda x: x['profile_order'])

    return job_profiles


def validate_task_multiplicity(cmd_groups, common_err_msg='',
                               max_num_channels=1):
    """Validate an ordered list of command groups that represent (part of) a
    Workflow.

    Parameters
    ----------
    cmd_groups : list of dict
        List of dicts representing command groups.
    common_err_msg : str, optional
        Optional error message to prepend to any exception raised in here. By
        default, empty.
    max_num_channels : int, optional
        Restrict the allowed number of channels to a given number.

    Returns
    -------
    cmd_groups : list of dict
        List of dicts representing command groups.    

    """

    if not common_err_msg.endswith(' '):
        common_err_msg += ' '

    exec_orders_all = [i.get('exec_order') for i in cmd_groups]
    exec_orders = [i for i in exec_orders_all if i is not None]

    if exec_orders:
        # Check `exec_order` starts from zero and increases by 0 or 1:
        uniq_exec_orders = list(set(sorted(exec_orders)))
        if uniq_exec_orders != list(range(len(uniq_exec_orders))):
            msg = common_err_msg + ('If specified, `exec_order` must start at '
                                    'zero and increase by zero or one across '
                                    'command groups.')
            raise ValueError(msg)

        for i in uniq_exec_orders:
            # Get cmd_group indices that have this `exec_order`:
            exec_order_idx = [idx for idx, j in enumerate(exec_orders_all)
                              if j == i]

            if len(exec_order_idx) > 1:

                if len(exec_order_idx) > max_num_channels:
                    # Temporarily restrict number of channels to 1:
                    msg = ('Multiple channels are not yet implemented.')
                    raise NotImplementedError(msg)

    else:
        # Set execution orders for command groups:
        for i in range(len(cmd_groups)):
            cmd_groups[i]['exec_order'] = i

    return cmd_groups
