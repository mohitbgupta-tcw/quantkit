import quantkit.backtester.strategies.strategy as strategy
import quantkit.utils.annualize_adjustments as annualize_adjustments
import numpy as np
import datetime


class MagicFormula(strategy.Strategy):
    """
    Magic Formula Strategy

    Idea: Rank ROIC and EV/EBIT, add scores and rank sort

    Parameters
    ----------
    params: dict
        strategy specific parameters which should include
            - type: "magic_formula", str
            - window_size: lookback period in trading days, int
            - return_engine: "cumprod", str
            - risk_engine: str
    """

    def __init__(self, params: dict) -> None:
        super().__init__(**params)
        self.top_n = params["top_n"]

    def assign(
        self,
        date: datetime.date,
        price_return: np.ndarray,
        index_comp: np.ndarray,
        roic: np.ndarray,
        ebit: np.ndarray,
        ev: np.ndarray,
        annualize_factor: int = 1.0,
        **kwargs,
    ) -> None:
        """
        Transform and assign returns to the actual calculator

        Parameters
        ----------
        date: datetime.date
            date of snapshot
        price_return: np.array
            zero base price return of universe
        index_comp: np.array
            index components for date
        roic: np.array
            roic of assets in universe
        ebit: np.array
            ebit of assets in universe
        ev: np.array
            ev of assets in universe
        annualize_factor: int, optional
            factor depending on data frequency
        """
        super().assign(date, price_return, index_comp, annualize_factor)
        self.roic = roic
        ev[ev < 0] = np.nan
        self.evebit = np.divide(ev, ebit.clip(min=0))

    @property
    def selected_securities(self) -> np.ndarray:
        """
        Index (position in universe_tickers as integer) of selected securities

        Returns
        -------
        np.array
            array of indexes
        """
        nan_sum = np.isnan(self.latest_return).sum()
        top_n = min(self.top_n, self.num_total_assets - nan_sum)

        roic_order = (-self.roic).argsort()
        roic_rank = roic_order.argsort()
        evebit_order = (self.evebit).argsort()
        evebit_rank = evebit_order.argsort()
        total_rank = evebit_rank + roic_rank
        sort = total_rank.argsort()

        selected_assets = 0
        i = 0
        a = list()

        while selected_assets < top_n and i < self.num_total_assets:
            if self.index_comp[sort[i]] > 0:
                a.append(sort[i])
                selected_assets += 1
            i += 1
        return np.array(a)

    @property
    def return_metrics_optimizer(self) -> np.ndarray:
        """
        Forecaseted DAILY returns from return engine of top n momentum securities
        in order of selected_securities

        Returns
        -------
        np.array
            returns
        """
        return self.return_metrics_intuitive[self.selected_securities]
