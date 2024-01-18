import quantkit.asset_allocation.strategies.strategy as strategy
import quantkit.utils.annualize_adjustments as annualize_adjustments
import numpy as np
import datetime


class RelativeValue(strategy.Strategy):
    """
    Roboter Version of D's value trading strategy

    Idea: Filter down universe based on specified kpi's

    Parameters
    ----------
    params: dict
        strategy specific parameters which should include
            - type: "relative_value", str
            - window_size: lookback period in trading days, int
            - return_engine: "cumprod", str
            - risk_engine: str
            - allocation_models: weighting strategies, list
            - market_cap_threshold: int
            - div_yield_threshold: float
            - roe_threshold: float
            - freecashflow_threshold: float

    """

    def __init__(self, params: dict) -> None:
        super().__init__(**params)
        self.market_cap_threshold = params["market_cap_threshold"]
        self.div_yield_threshold = params["div_yield_threshold"]
        self.roe_threshold = params["roe_threshold"]
        self.freecashflow_threshold = params["freecashflow_threshold"]

    def assign(
        self,
        date: datetime.date,
        price_return: np.ndarray,
        index_comp: np.ndarray,
        market_caps: np.ndarray,
        divyield: np.ndarray,
        roe: np.ndarray,
        fcfps: np.ndarray,
        pe: np.ndarray,
        pb: np.ndarray,
        ps: np.ndarray,
        spx_pe: float,
        spx_pb: float,
        spx_ps: float,
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
        market_caps: np.array
            market caps of assets in universe
        divyield: np.array
            dividend yields of assets in universe
        roe: np.array
            roe of assets in universe
        fcfps: np.array
            fcfps of assets in universe
        pe: np.array
            pe of assets in universe
        pb: np.array
            pb of assets in universe
        ps: np.array
            ps of assets in universe
        spx_pe: float
            SPY PE ratio
        spx_pb: float
            SPY PB ratio
        spx_ps: float
            SPY PS ratio
        annualize_factor: int, optional
            factor depending on data frequency
        """
        super().assign(date, price_return, index_comp, annualize_factor)
        self.market_caps = market_caps
        self.divyield = divyield
        self.roe = roe
        self.fcfps = fcfps
        self.pe = pe
        self.pb = pb
        self.ps = ps
        self.spx_pe = spx_pe
        self.spx_pb = spx_pb
        self.spx_ps = spx_ps

    @property
    def selected_securities(self) -> np.ndarray:
        """
        Index (position in universe_tickers as integer) of selected securities

        Returns
        -------
        np.array
            array of indexes
        """
        ss = np.arange(self.num_total_assets)
        return ss[
            ~np.isnan(self.latest_return)
            & (self.index_comp > 0)
            & (self.market_caps > self.market_cap_threshold)
            & (self.divyield > self.div_yield_threshold)
            & (self.roe > self.roe_threshold)
            & (self.fcfps > self.freecashflow_threshold)
            & (
                (self.pe < self.spx_pe)
                | (self.pb < self.spx_pb)
                | (self.ps < self.spx_ps)
            )
        ]

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
