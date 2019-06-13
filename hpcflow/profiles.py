"""`hpcflow.profiles.py`

Module containing code associated with using YAML file job profiles to
construct a Workflow.

"""

from pathlib import Path
from pprint import pprint

import yaml

from hpcflow import CONFIG, PROFILES_DIR
from hpcflow.validation import (
    validate_task_multiplicity, validate_job_profile,
    validate_job_profile_list, CMD_GROUP_DEFAULTS, SHARED_KEYS_GOOD
)
from hpcflow.variables import (
    select_cmd_group_var_definitions, get_all_var_defns_from_lookup,
    get_variabled_filename_regex, find_variabled_filenames,
)


def parse_job_profiles(dir_path=None, profile_list=None):
    """Parse YAML file profiles into a form suitable for `models.Workflow`
    initialisation.

    Parameters
    ----------
    dir_path : Path
        The directory in which the Workflow will be generated. In addition to
        the profile file paths explicitly declared in the `profiles` parameter,
        this directory (`dir_path`) will be searched for profile files.
    profile_list : list of (str or Path), optional
        List of YAML profile file paths to be used to construct the Workflow.
        If `None` and if no profiles are found in `dir_path` according to the
        global configuration for the expected format of profile files, then a
        `ValueError` is raised.

    Returns
    -------
    workflow_dict : dict
        Dict representing the workflow, with two keys: `command_groups` and
        `variable_definitions`.

    """

    profile_matches = parse_profile_filenames(dir_path, profile_list)

    all_profiles = []
    for job_profile_path, var_values in profile_matches.items():
        all_profiles.append(
            resolve_job_profile(job_profile_path, var_values)
        )

    all_profiles = validate_job_profile_list(all_profiles)

    # Form command group list, where profile-level parameters are copied to the
    # child command groups, but equivalent command group parameters take
    # precedence. Also extract out variable definitions.
    var_definitions = {}
    command_groups = []
    pre_commands = []
    archives = []
    for i in all_profiles:

        pre_commands.extend(i.get('pre_commands') or [])

        profile_cmd_groups = []
        # Form command group list:
        exec_order_add = len(command_groups)
        for j in i['command_groups']:

            # Resolve archive locations from CONFIG file
            # Add archive_idx and archive_exclude; remove archive
            arch = j.pop('archive', None)
            if arch:
                arch_name = arch['name']
                arch_exc = arch.get('exclude')
                arch_loc = CONFIG['archive_locations'][arch_name]

                existing_idx = None
                for k_idx, k in enumerate(archives):
                    if k['name'] == arch_name:
                        existing_idx = k_idx
                        break

                if existing_idx is not None:
                    j['archive_idx'] = existing_idx
                else:
                    archives.append({
                        'name': arch_name,
                        **arch_loc,
                    })
                    j['archive_idx'] = len(archives) - 1

                if arch_exc:
                    j['archive_excludes'] = arch_exc

            # Populate with defaults first:
            cmd_group = {
                **CMD_GROUP_DEFAULTS,
            }
            # Overwrite with profile-level parameters:
            for k in SHARED_KEYS_GOOD:
                if i.get(k) is not None:
                    cmd_group.update({k: i[k]})
            # Overwrite with command-level parameters:
            cmd_group.update(**j)
            cmd_group['exec_order'] += exec_order_add
            profile_cmd_groups.append(cmd_group)

        command_groups.extend(profile_cmd_groups)

        # Extract var_defs from profile and from var lookup given var scope:
        var_scope = i.get('variable_scope')
        var_defns_all = i.get('variables', {})
        var_defns_all.update(get_all_var_defns_from_lookup(var_scope))

        for j in profile_cmd_groups:

            cmd_group_var_defns = select_cmd_group_var_definitions(
                var_defns_all,
                j['commands'],
                j['directory']
            )

            var_definitions.update(cmd_group_var_defns)

    workflow = {
        'command_groups': command_groups,
        'var_definitions': var_definitions,
        'pre_commands': pre_commands,
        'archives': archives,
    }

    return workflow


def parse_profile_filenames(dir_path, profile_list=None):
    """Resolve name and order variables embedded in profile filenames."""

    fn_fmt = CONFIG['profile_filename_fmt']
    var_delims = CONFIG['variable_delimiters']

    # There are only two allowed profile filename-encodable variables:
    var_value = {
        'profile_name': {
            'fmt': r'[A-Za-z0-9_\-]*',
            'type': str,
        },
        'profile_order': {
            'fmt': r'[0-9]+',
            'type': int,
        },
    }
    var_value_fmt = {k: v['fmt'] for k, v in var_value.items()}
    var_value_type = {k: v['type'] for k, v in var_value.items()}

    fn_regex, var_names = get_variabled_filename_regex(
        fn_fmt, var_delims, var_value_fmt)

    if profile_list:
        # Check given profile file names exist and match expected format:
        profile_matches = find_variabled_filenames(
            profile_list,
            fn_regex,
            var_names,
            var_value_type
        )

    else:
        # Search `dir_path` for matching file names:
        dir_files = [i for i in dir_path.iterdir() if i.is_file()]
        profile_matches = find_variabled_filenames(
            dir_files,
            fn_regex,
            var_names,
            var_value_type,
            all_must_match=False,
            check_exists=False
        )

        if not profile_matches:
            msg = ('No profiles found in directory: "{}"')
            raise ValueError(msg.format(dir_path))

    return profile_matches


def resolve_job_profile(job_profile_path, filename_var_values,
                        common_profiles_dir=None):
    """Resolve the inheritance tree of a job profile and do basic validation.

    Parameters
    ----------
    job_profile_path : str or Path
        Path to the job profile file, which may inherit from common profiles.
    filename_var_values : dict
        Dictionary that maps variable names to their values as embedded in the
        job profile file name.
    common_profiles_dir : str or Path
        Directory that contains common profiles. By default, set to `None`, in
        which case, the default profile directory is used (which is within the
        hpcflow data directory).

    Returns
    -------
    merged_profile : dict
        Dictionary of the resolved job profile.

    """

    job_profile_path = Path(job_profile_path)
    if common_profiles_dir:
        common_profiles_dir = Path(common_profiles_dir)
    else:
        common_profiles_dir = PROFILES_DIR

    # Get a list of all common profile file names (without any extension):
    prof_names = []
    for prof in common_profiles_dir.iterdir():
        prof_names.append(prof.stem)

    profile_inheritance = [job_profile_path.stem]

    with job_profile_path.open() as handle:
        job_profile = yaml.safe_load(handle)

    # Form a list of data from each profile in the inheritance tree:
    all_profiles = [job_profile]
    parent_profile = job_profile
    while 'inherits' in parent_profile:

        parent_profile_name = parent_profile['inherits']
        profile_inheritance.append(parent_profile_name)

        if parent_profile_name not in prof_names:
            msg = ('Cannot find profile "{}" in the profile directory "{}".')
            raise ValueError(msg.format(
                parent_profile_name, common_profiles_dir))

        parent_spec_path = common_profiles_dir.joinpath(
            parent_profile_name + CONFIG['profile_ext'])

        with parent_spec_path.open() as handle:
            parent_profile = yaml.safe_load(handle)
            all_profiles.append(parent_profile)

    # Merge inherited profile data:
    merged_profile = {}
    merged_vars = {}
    merged_opts = {}
    for i in all_profiles[::-1]:

        # Merge variables and opts independently to allow variables to inherit:
        if 'variables' in i:
            merged_vars.update(i.pop('variables'))

        if 'scheduler_options' in i:
            merged_opts.update(i.pop('scheduler_options'))

        merged_profile.update(**i)

    merged_profile.pop('inherits')
    merged_profile['variables'] = merged_vars
    merged_profile['scheduler_options'] = merged_opts

    # Add variables embedded in file name:
    allowed_filename_vars = ['profile_order', 'profile_name']
    for i in allowed_filename_vars:
        if i in filename_var_values:
            if i in merged_profile:
                msg = ('`{}` is set in the job profile file name and also in '
                       'the file YAML contents for job profile file "{}". It '
                       'must be specified only once.')
                raise ValueError(msg.format(i, job_profile_path.name))
            else:
                merged_profile.update({i: filename_var_values[i]})

    merged_profile = validate_job_profile(merged_profile)

    cmd_groups = merged_profile['command_groups']
    if not cmd_groups:
        msg = ('At least one command group must be specified in the job '
               'profile file named "{}".')
        raise ValueError(msg.format(job_profile_path.name))

    com_err_msg = '[Job profile name: "{}"]'.format(job_profile_path.name)
    cmd_groups = validate_task_multiplicity(cmd_groups, com_err_msg)
    merged_profile['command_groups'] = cmd_groups

    return merged_profile
