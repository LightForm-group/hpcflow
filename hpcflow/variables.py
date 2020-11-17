"""`hpcflow.variables.py`"""

import re
from copy import deepcopy
from pathlib import Path

from hpcflow.config import Config as CONFIG
from hpcflow.utils import coerce_same_length


class UnresolvedVariableError(Exception):
    pass


def get_var_defn_from_template(template, arg):
    """Construct a variable from a template and an argument.

    Parameters
    ----------
    template : dict
        A variable definition template from the variable definition lookup
        file.
    arg : str
        A parameter for the template. Where "<<arg>>" appears in the
        template `value` or `file_regex.pattern` keys, it is substituted by
        this parameter.

    Returns
    -------
    var_defn : dict
        Variable definition dictionary.    

    """

    # Replace any instance of `<<arg>>` with the passed argument.
    var_defn = deepcopy(template)
    regex_pat = var_defn['file_regex']['pattern']
    value = var_defn['value']
    arg = re.escape(arg)

    if '<<arg>>' in regex_pat:
        var_defn['file_regex']['pattern'] = regex_pat.replace('<<arg>>', arg)

    if '<<arg>>' in value:
        var_defn['value'] = value.replace('<<arg>>', arg)

    return var_defn


def get_all_var_defns_from_lookup(scope):
    """Get all in-scope variable definitions from the lookup file.

    Parameters
    ----------
    scope : str
        Scope within the variable lookup file in which the variable definition
        resides.

    Returns
    -------
    var_defns : dict of dict
        dict of variable definitions.

    """

    VARS_LOOKUP = CONFIG.get('variable_lookup')
    def_scope = VARS_LOOKUP['scopes'].get('_default', {})
    all_scoped_vars = VARS_LOOKUP['scopes'].get(scope, def_scope)
    scoped_vars = all_scoped_vars.get('variables', {})
    scoped_vars_template = all_scoped_vars.get('variables_from_template', {})

    var_defns = deepcopy(scoped_vars)

    for var_name, template_params in scoped_vars_template.items():
        # Construct the variable definition from a template
        template_name, template_arg = template_params
        template = VARS_LOOKUP['variable_templates'][template_name]
        var_defn = get_var_defn_from_template(template, template_arg)
        var_defns.update({
            var_name: var_defn
        })

    return var_defns


def resolve_sub_vars(var_defns, all_var_defns):
    """For all variable names embedded within variable definitions, resolve
    their own definitions.

    Variable definitions may include references to other variables. This
    function finds these references and returns a set of variable definitions
    that resolve these dependencies.

    Parameters
    ----------
    var_defns : dict
        The original set of variable definitions that may include references
        to variables within their values.
    all_var_defns : dict
        A large set of variable definitions from which new definitions are to
        be found.

    Returns
    -------
    new_var_defns : dict
        The set of variable definitions that resolve the embedded sub-variable
        names in the original set of variable definitions, `var_defns`.

    """

    new_var_defns = {}
    var_delims = CONFIG.get('variable_delimiters')

    for _, var_defn_i in var_defns.items():

        val_i = var_defn_i.get('value', '{}')
        sub_var_names = extract_variable_names(val_i, var_delims)

        if sub_var_names:
            sub_var_defns = {
                i: all_var_defns[i]
                for i in sub_var_names
            }
            new_var_defns.update(sub_var_defns)
            new_var_defns.update(
                resolve_sub_vars(sub_var_defns, all_var_defns)
            )

    return new_var_defns


def select_cmd_group_var_names(commands, directory):

    var_delims = CONFIG.get('variable_delimiters')

    # Retrieve the names of variables in `commands` and in `directory`:
    var_names = []
    for cmd in commands:
        vars_i = extract_variable_names(cmd, var_delims)
        var_names.extend(vars_i)

    if directory:
        var_names.extend(extract_variable_names(directory, var_delims))

    # Eliminate duplicates:
    var_names = list(set(var_names))

    return var_names


def select_cmd_group_var_definitions(var_defns_all, commands, directory):
    """For a given command group, select from a list of variable definitions
    only those definitions that are required.

    Parameters
    ----------
    var_defns_all : list of dict
    commands : list of str
    directory : str

    Returns
    -------
    var_defns : list of dict
        The subset of the input variable definitions that are required for
        the commands and directory associated with this command group.

    """

    var_defns = {}
    var_names = select_cmd_group_var_names(commands, directory)

    # Add the definitions of found variable names:
    for i in var_names:
        # TODO handle exception
        var_defns.update({i: var_defns_all[i]})

    # Recursively search for, and add definitions of, sub-variables:
    sub_var_defns = resolve_sub_vars(var_defns, var_defns_all)
    var_defns.update(sub_var_defns)

    return var_defns


def extract_variable_names(source_str, delimiters, characters=None):
    """Given a specified syntax for embedding variable names within a string,
    extract all variable names.

    Parameters
    ----------
    source_str : str
        The string within which to search for variable names.
    delimiters : two-tuple of str
        The left and right delimiters of a variable name.
    characters : str
        The regular expression that matches the allowed variable name. By
        default, this is set to a regular expression that matches at least one
        character.

    Returns
    -------
    var_names : list of str
        The variable names embedded in the original string.

    Examples
    --------
    Using single parentheses to delimit variable names, where variable names
    are formed of at least one Latin letter (including upper- and lower-case):

    >>> source_str = r'(foo).(bar).yml'
    >>> delimiters = ('(', ')')
    >>> characters = '[a-zA-Z]+?'
    >>> extract_variable_names(source_str, delimiters, characters)
    ['foo', 'bar']

    Using double angled brackets to delimit variable names, where variable
    names are formed of at least one character:

    >>> source_str = r'<<Foo79_8>>.<<baR-baR>>.yml'
    >>> delimiters = ('<<', '>>')
    >>> characters = '.+?' 
    >>> extract_variable_names(source_str, delimiters, characters)
    ['Foo79_8', 'baR-baR']        

    """

    if not characters:
        characters = r'.\S+?'

    if not characters.endswith('?'):
        # Always match as few characters as possible.
        characters += '?'

    delim_esc = [re.escape(i) for i in delimiters]

    # Form a capture group around the variable name:
    pattern = delim_esc[0] + '(' + characters + ')' + delim_esc[1]
    var_names = re.findall(pattern, source_str)

    return var_names


def get_variabled_filename_regex(filename_format, var_delims, var_values,
                                 var_chars=None):
    """Get the regular expression that matches a filename that may include
    embedded variable values according to a particular syntax.

    Parameters
    ----------
    filename_format : str
        The format of the filenames to match, which can include embedded
        variable names, whose syntax is determined by `var_delims`, `var_chars`
        and `var_values`.
    var_delims : two-tuple of str
        The left and right delimiters of variable names as embedded in
        `filename_format`.
    var_values : dict of (str: str)
        Dictionary whose keys are variable names that may appear in
        `filename_format`, and whose values are the regular expressions that
        matches the variable value.
    var_chars : str, optional
        The regular expression that matches the allowed variable name. By
        default, this is set to `None`, meaning no restriction is placed on the
        allowed variable names. If this is set to a string, the keys in
        `var_values` must match this regular expression.

    Returns
    -------
    tuple
        filename_regex : str
            The regular expression that will match filenames with embedded
            variables as determined by the input parameters.
        var_names : list
            The ordered list of variable names that were matched. The order
            matches the parenthesised match groups in `filename_regex`.

    Examples
    --------
    >>> filename_fmt = '<<order>>.<<foo>>.yml'
    >>> var_delims = ['<<', '>>']
    >>> var_values = {'foo': r'[a-z]*', 'order': 'r[0-9]+'}
    >>> get_variabled_filename_regex(filename_fmt, var_delims, var_values)
    ('(r[0-9]+)\\.([a-z]*)\\.yml', ['order', 'foo'])

    """

    if var_chars is not None:
        # Check keys in `var_values` match `var_chars` regex:

        var_chars_test = var_chars
        if var_chars.endswith('?'):
            # Remove "match as few as possible" symbol:
            var_chars_test = var_chars_test[:-1]

        for i in var_values:
            var_name_match = re.match(var_chars_test, i)
            if not var_name_match or var_name_match.group() != i:
                msg = ('`var_chars` regex given as "{}" does not match with '
                       'the variable name: "{}"')
                raise ValueError(msg.format(var_chars, i))

    # Extract the variable names from the profile filename format:
    var_names = extract_variable_names(filename_format, var_delims, var_chars)

    # If there are no variable names, just do regex escape on it, and return:
    if not var_names:
        filename_format_esc = re.escape(filename_format)
        var_names = []
        return (filename_format_esc, var_names)

    # Count how many times each variable appears in `pattern`:
    var_count = {}
    for i in var_names:
        if i in var_count:
            var_count[i] += 1
        else:
            var_count[i] = 1

    tot_var_count = sum(var_count.values())

    # Generate regex for matching the variable names in `pattern`:
    var_delims_esc = [re.escape(i) for i in var_delims]
    var_placeholder_esc = '{}{{}}{}'.format(*var_delims_esc)
    vars_fmt = '|'.join([var_placeholder_esc.format(i) for i in var_values])
    vars_fmt = '(' + vars_fmt + ')'

    # Need to regex-escape everything in `pattern` that is not a variable:
    spec_fmt_match = r'(.*)'.join(['{0:}' for _ in range(tot_var_count)])
    spec_fmt_match = r'(.*)' + spec_fmt_match.format(vars_fmt) + r'(.*)'

    match = list(re.match(spec_fmt_match, filename_format).groups())
    for i in range(0, len(match), 2):
        match[i] = re.escape(match[i])

    filename_regex = ''.join(match)

    # Replace variable placeholders with regex:
    var_placeholder = '{}{{}}{}'.format(*var_delims)
    for k, v in var_values.items():
        filename_regex = filename_regex.replace(
            var_placeholder.format(k), r'({})'.format(v))

    return filename_regex, var_names


def find_variabled_filenames(file_paths, filename_regex, var_names, var_types,
                             all_must_match=True, check_exists=True):
    """Find which in a list of file names match a regular expression, and
    capture parenthesised match groups for each filename match.

    Parameters
    ----------
    file_paths : list of (str or Path)
        Path of files whose names are to be matched against a given regular
        expression.
    filename_regex : str
        Regular expression to match filenames to.
    var_names : list
        List of variable names whose order matches the parenthesised match
        groups in `filename_regex`.
    var_types : dict of (str : type)
        Expected Python type of each variable value to which the regex matches
        will be cast.

    Returns
    -------
    file_matches : dict of (Path : dict)
        Dictionary whose keys are Path objects that point to matched files.
        Values are dictionaries that map the variable name to its value for
        that matched file.

    Examples
    --------

    TODO redo example.

    # If the current working directory (invoking directory) contains the files
    # "1.run.yml" and "2.process.yml", then the following can be evaluated:

    # >>> fn_regex = r'([0-9]+)\.([a-z]*)\.yml'
    # >>> var_names = ['order', 'foo']
    # >>> var_types = {'foo': str, 'order': int}
    # >>> find_variabled_filenames('', fn_regex, var_names, var_types)    
    # {WindowsPath('1.run.yml'): {'order': 1, 'foo': 'run'},
    #  WindowsPath('2.process.yml'): {'order': 2, 'foo': 'process'}}

    """

    file_matches = {}

    for i in file_paths:

        i = Path(i)
        if check_exists:
            if not i.exists():
                msg = ('File named "{}" does not exist, but parameter '
                       '`check_exists=True`.')
                raise FileNotFoundError(msg.format(i.name))

        i_match = re.fullmatch(filename_regex, i.name)

        if i_match:
            match_groups = i_match.groups()
            vars_i = {}
            for var_idx, var_name in enumerate(var_names):
                var_val = match_groups[var_idx]
                var_val_cast = var_types[var_name](var_val)

                if var_name in vars_i and var_val_cast != vars_i[var_name]:
                    msg = ('Inconsistent values for variable: "{}". Values are'
                           ' "{}" and "{}".')
                    raise ValueError(msg.format(
                        var_name, vars_i[var_name], var_val_cast))

                vars_i.update({var_name: var_val_cast})

            file_matches.update({i: vars_i})

        elif all_must_match:
            msg = ('File named "{}" does not match the regular expression "{}"'
                   ' but parameter `all_must_match=True`.')
            raise ValueError(msg.format(i.name, filename_regex))

    return file_matches


def resolve_variable_values(var_defns, directory):
    """Get the values of variables.

    Parameters
    ----------
    var_defns : list of VarDefinition
    directory : Path
        Directory within which to resolve the values.

    """

    unresolvable_var_names = []
    var_vals = {}
    dep_map = {}

    for i in var_defns:

        if i.is_base_variable():

            try:
                vals = i.get_values(directory)
            except UnresolvedVariableError:
                unresolvable_var_names.append(i.name)
                continue

            var_vals.update({
                i.name: {
                    'vals': vals,
                    'sub_var_vals': {},
                }
            })

        else:
            dep_map.update({
                i.name: i.get_dependent_variable_names(),
            })

    # Remove all vars from dep_map that depend on unresolvable vars:
    dep_map_new = {}
    for k, v in dep_map.items():

        keep_dep = True
        for i in v:
            if i in unresolvable_var_names:
                keep_dep = False
                break

        if keep_dep:
            dep_map_new.update({k: v})

    dep_map = dep_map_new

    count = 0
    while dep_map:

        if count > 5:
            msg = 'Could not resolve variables in 10 iterations.'
            raise ValueError(msg)

        count += 1
        dep_map_new = {}

        for k, v in dep_map.items():

            sub_var_vals = {}
            dep_resolved = True

            for i in v:

                if i in var_vals:
                    sub_var_vals.update({i: var_vals[i]['vals']})

                else:
                    dep_resolved = False
                    break

            if dep_resolved:

                var_defn_i = [i for i in var_defns if i.name == k][0]
                values = var_defn_i.get_values(directory)
                err_msg = 'Variable multiplicity mismatch!'

                sub_var_vals_keys = list(sub_var_vals.keys())
                sub_var_vals_vals = list(sub_var_vals.values())

                # Coerce all sub_var_vals and values to have the same length:
                try:
                    coerced_vals = coerce_same_length(
                        [values] + sub_var_vals_vals)
                except ValueError:
                    raise ValueError(err_msg)

                vals_new = coerced_vals[0]
                sub_var_vals_new = dict(zip(sub_var_vals_keys,
                                            coerced_vals[1:]))

                for k2, v2 in sub_var_vals_new.items():

                    for idx, rep in enumerate(v2):

                        vals_new[idx] = vals_new[idx].replace(
                            '<<{}>>'.format(k2), rep)

                var_vals.update({
                    k: {
                        'vals': vals_new,
                        'sub_var_vals': sub_var_vals,
                    }
                })

            else:
                dep_map_new.update({
                    k: v
                })

        dep_map = dep_map_new

    return var_vals
