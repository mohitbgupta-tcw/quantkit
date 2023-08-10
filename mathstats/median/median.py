import quantkit.mathstats.streaming_base.streaming_base as sb
import numpy as np
from typing import Union


class Quantiles(object):
    """
    Class for calculating quantiles and median of datapoints
    """

    def __init__(self):
        self.streaming_base = sb.StreamingBase()

    def add_value(self, value: float) -> None:
        """
        add data point to saved values

        Parameters
        ----------
        value: float
            new data point
        """
        self.streaming_base.add_value(value)

    @property
    def median(self) -> float:
        """
        calculate median from saved values

        Returns
        -------
        float
            median
        """
        return np.median(np.array(self.streaming_base.values))

    def quantiles(self, q: Union[float, np.array]) -> Union[float, np.array]:
        """
        calculate q-th quantile from saved values

        Parameters
        ----------
        q: float | np.array
           Quantile or sequence of quantiles to compute, which must be between 0 and 1 inclusive

        Returns
        -------
        float | np.array
            If q is a single quantile, then the result is a scalar.
            If multiple quantiles are given, first axis of the result corresponds to the quantiles.
        """
        return np.quantile(self.streaming_base.values, q)
