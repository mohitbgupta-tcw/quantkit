import numpy as np
from collections import deque
from typing import Tuple


class StreamingBase(object):
    """
    Base Class for stracking matrices over time
    """

    def __init__(self):
        self._values = []
        self.total_iterations = 0
        self._results = dict()

    def add_value(self, value: float) -> None:
        """
        add data point to values

        Parameters
        ----------
        value: float
            new data point
        """
        self._values.append(value)

    @property
    def values(self) -> list:
        return self._values

    @property
    def results(self) -> dict:
        return self._results
