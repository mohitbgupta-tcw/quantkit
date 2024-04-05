import pandas as pd
import numpy as np
from quantkit.bt.performance_statistics.stats import PerformanceStats


class Backtest(object):
    """
    Run Backtest
    -> combine Strategy with data to produce Result
    -> test strategy over a dataset

    Parameters
    ----------
    strategy: Strategy
        strategy to be tested
    data: pd.DataFrame
        DataFrame containing price data of underlying universe
    name: str, optional
        backtest name - defaults to strategy name
    initial_capital: float , optional
        initial amount of capital passed to strategy
    commissions: fn(quantity, price), optional
        commission function to be usedm p.e. commissions=lambda q, p: max(1, abs(q) * 0.01)
    integer_positions: bool, optional
        use integer positions for securities
    additional_data: dict, optional
        additional kwargs passed to StrategyBase.setup
        -> data can be retrieved by Algos using StrategyBase.get_data()
        Examples:
            - "bidoffer":
                DataFrame with same format as "data", will be used by strategy for transaction cost modeling
            - "coupons":
                DataFrame with same format as "data", will be used by class CouponPayingSecurity to determine cashflows
            - "cost_long"/"cost_short":
                DataFrame with the same format as "data", will be used by class CouponPayingSecurity
                to calculate asymmetric holding cost of long (or short) positions
    """

    def __init__(
        self,
        strategy,
        data: pd.DataFrame,
        name: str = None,
        initial_capital: float = 1000000.0,
        commissions=None,
        integer_positions: bool = True,
        additional_data: dict = None,
    ) -> None:
        self.strategy = strategy
        self.strategy.use_integer_positions(integer_positions)

        self._process_data(data, additional_data)

        self.initial_capital = initial_capital
        self.name = name if name is not None else strategy.name

        if commissions is not None:
            self.strategy.set_commissions(commissions)

        self.stats = {}
        self._original_prices = None
        self._weights = None
        self._sweights = None
        self.has_run = False

    def _process_data(self, data: pd.DataFrame, additional_data: dict) -> None:
        """
        Preprocess data and additional data

        Parameters
        ----------
        data: pd.DataFrame
            DataFrame containing price data of underlying universe
        additional_data: dict
            additional kwargs passed to StrategyBase.setup
        """
        # add virtual row at t0-1day with NaNs
        data_new = pd.concat(
            [
                pd.DataFrame(
                    np.nan,
                    columns=data.columns,
                    index=[data.index[0] - pd.DateOffset(days=1)],
                ),
                data,
            ]
        )

        self.prices = data_new
        self.dates = data_new.index

        self.additional_data = (additional_data or {}).copy()

        for k in self.additional_data:
            old = self.additional_data[k]
            if isinstance(old, pd.DataFrame) and old.index.equals(data.index):
                empty_row = pd.DataFrame(
                    np.nan,
                    columns=old.columns,
                    index=[old.index[0] - pd.DateOffset(days=1)],
                )
                new = pd.concat([empty_row, old])
                self.additional_data[k] = new
            elif isinstance(old, pd.Series) and old.index.equals(data.index):
                empty_row = pd.Series(
                    np.nan, index=[old.index[0] - pd.DateOffset(days=1)]
                )
                new = pd.concat([empty_row, old])
                self.additional_data[k] = new

    def run(self):
        """
        Run Backtest
        """
        if self.has_run:
            return

        self.has_run = True

        # setup strategy
        self.strategy.setup(self.prices, **self.additional_data)

        # adjust strategy with initial capital
        self.strategy.adjust(self.initial_capital)

        # backtest loop
        for inow, dt in enumerate(self.dates):
            self.strategy.pre_settlement_update(dt, inow)

            if not self.strategy.bankrupt:
                self.strategy.run()
                self.strategy.post_settlement_update()

        self.stats = PerformanceStats(self.strategy.prices)
        self._original_prices = self.strategy.prices

    @property
    def weights(self) -> pd.DataFrame:
        """
        Each component's weight over time

        Returns
        -------
        pd.DataFrame
            weights of components
        """
        if self._weights is not None:
            return self._weights
        else:
            vals = pd.DataFrame({x.full_name: x.values for x in self.strategy.members})
            vals = vals.div(self.strategy.values, axis=0)
            self._weights = vals
            return vals

    @property
    def positions(self) -> pd.DataFrame:
        """
        Each security's position over time

        Returns
        -------
        pd.DataFrame
            positions of securities
        """
        return self.strategy.positions

    @property
    def security_weights(self) -> pd.DataFrame:
        """
        Each security's weight over time

        Returns
        -------
        pd.DataFrame
            weight of securities
        """
        if self._sweights is not None:
            return self._sweights
        else:
            vals = {}
            for m in self.strategy.members:
                if m._issec:
                    m_values = m.values.copy()
                    if m.name in vals:
                        vals[m.name] += m_values
                    else:
                        vals[m.name] = m_values
            vals = pd.DataFrame(vals)
            vals = vals.div(self.strategy.values, axis=0)

            self._sweights = vals
            return vals

    @property
    def turnover(self) -> pd.DataFrame:
        """
        Turnover of backtest
        -> defined as the lesser of positive or negative outlays divided by NAV

        Returns
        -------
        pd.DataFrame
            turnovers
        """
        s = self.strategy
        outlays = s.outlays

        # seperate positive and negative outlays, sum them up, and keep min
        outlaysp = outlays[outlays >= 0].fillna(value=0).sum(axis=1)
        outlaysn = np.abs(outlays[outlays < 0].fillna(value=0).sum(axis=1))

        # merge and keep minimum
        min_outlay = pd.DataFrame({"pos": outlaysp, "neg": outlaysn}).min(axis=1)

        # turnover is defined as min outlay / nav
        mrg = pd.DataFrame({"outlay": min_outlay, "nav": s.values})

        return mrg["outlay"] / mrg["nav"]
