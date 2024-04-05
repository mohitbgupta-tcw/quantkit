import pandas as pd
from typing import Union
import quantkit.bt.core_structure.algo as algo
import quantkit.bt.signals.simple_selectors as simple_selectors
import quantkit.bt.technical_analysis.returns as returns


class SelectMomentum(algo.AlgoStack):
    """
    Select securities based on momentum filter
    -> select top n securities based on total return over lookback period
    -> save them in temp["selected"]

    Parameters
    ----------
    n: int | float
        number or percentage of securities selected
    lookback: int, optional
        lookback period in month
    lag: int, optional
        lag interval for total return calculation in days
    sort_descending: bool, optional
        Sort descending (highest return is best)
    all_or_none: bool, optional
        populate result only if more than n securities are available
    """

    def __init__(
        self,
        n: Union[int, float],
        lookback: int = 3,
        lag=0,
        sort_descending: bool = True,
        all_or_none=False,
    ) -> None:
        super().__init__(
            returns.StatTotalReturn(lookback=lookback, lag=lag),
            simple_selectors.SelectN(
                n=n, sort_descending=sort_descending, all_or_none=all_or_none
            ),
        )
