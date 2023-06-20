class StreamingBase(object):
    """
    Class for saving values
    """

    def __init__(self):
        self.values = []

    def add_value(self, value: float):
        """
        add data point to values

        Parameters
        ----------
        value: float
            new data point
        """
        self.values.append(value)
        return
