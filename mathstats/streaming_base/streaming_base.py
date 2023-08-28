import numpy as np
from collections import deque
from typing import Tuple


class StreamingBase(object):
    """
    Base Class for tracking matrices over time

    Parameters
    ----------
    num_ind_variables : int
        Number of independent variables
    """

    def __init__(self, num_ind_variables: int) -> None:
        self._values = []
        self.total_iterations = 0
        self._results = dict()
        self.number_of_ind_variables = num_ind_variables

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
