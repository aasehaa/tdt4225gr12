def convert_timestamp(excel_timestamp : float, to_datetime : bool = True) -> float:
    """Converts timestamps from Excel-style to the normal Unix/POSIX style
    We use the formula UnixTS = 86400(ExcelTS - 25569) - 7200, since there's 25569 between Dec 30, 1899
    and Jan 1, 1970, and there's 86400 seconds in a day. We also adjust time zone by adding 7200secs(2hr)

    :param excel_timestamp: Number of days passed since December 30th 1899
    :type excel_timestamp: float
    :param to_datetime: Determines wether or not the result should be float (False) or DateTime (True)
    :type to_datetime: bool
    :return: Datetime object, or time in seconds since January 1st 1970
    :rtype: float
    """
    return 86400 * (float(excel_timestamp) - 25569) + 7200
