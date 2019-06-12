"""`hpcflow.utils.py`

Utility functions that are not particularly specific to `hpcflow`.

"""


def coerce_same_length(all_lists):
    """
    TODO: add docstring and examples

    """

    all_len = [len(i) for i in all_lists]
    uniq_lens = set(all_len)
    num_uniq_lens = len(uniq_lens)

    if num_uniq_lens == 1:
        out = all_lists

    elif num_uniq_lens == 2:

        if min(uniq_lens) != 1:
            raise ValueError('bad!')

        max_len = max(uniq_lens)
        out = []
        for i in all_lists:
            if len(i) != max_len:
                i = i * max_len
            out.append(i)

    else:
        raise ValueError('bad!')

    return out


def zeropad(num, largest):
    """Return a zero-padded string of a number, given the largest number.

    TODO: want to support floating-point numbers as well? Or rename function
    accordingly.

    Parameters
    ----------
    num : int
        The number to be formatted with zeros padding on the left.
    largest : int
        The number that determines the number of zeros to pad with.

    Returns
    -------
    padded : str
        The original number, `num`, formatted as a string with zeros added
        on the left.

    """

    num_digits = len('{:.0f}'.format(largest))
    padded = '{0:0{width}}'.format(num, width=num_digits)

    return padded
