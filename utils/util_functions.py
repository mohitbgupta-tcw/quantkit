def divide_chunks(l: list, n: int):
    """
    Divide a list into chunks of size n

    Parameters
    ----------
    l: list
        list to be divided
    n: int
        chunk size
    """
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i : i + n]
