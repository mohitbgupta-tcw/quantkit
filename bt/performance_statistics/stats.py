import pandas as pd
import numpy as np
import datetime
from tabulate import tabulate
from typing import Union
from quantkit.bt.util.formatting_functions import fmtn, fmtp, fmtpn


class PerformanceStats(object):
    """
    Calculates performance statistics given Series of prices.

    Parameters
    ----------
    prices: pd.Series
        Series of prices
    risk_free_rate: float | Series, optional
        annual risk-free rate or risk-free rate price series
    annualization_factor: int, optional
        annualizing factor
    """

    def __init__(
        self,
        prices: pd.Series,
        rf: Union[float, pd.Series] = 0.0,
        annualization_factor: int = 252,
    ) -> None:
        self.prices = prices
        self.name = self.prices.name
        self._start = self.prices.index[0]
        self._end = self.prices.index[-1]

        self.rf = rf
        self.annualization_factor = annualization_factor

        self._update(self.prices)

    def set_riskfree_rate(self, rf: Union[float, pd.Series]) -> None:
        """
        - set annual risk-free rate property
        - calculate annualized monthly and daily rates
        - recalculate performance stats

        risk_free_rate: float | Series
            Annual risk-free rate or risk-free rate price series
        """
        self.rf = rf
        self._update(self.prices)

    def _update(self, obj: pd.Series) -> None:
        """
        Update and set Performance statistics

        Parameters
        ----------
        obj: pd.Series
            price Series
        """
        self._calculate(obj)
        self.return_table = pd.DataFrame(self.return_table).T
        # name columns
        if len(self.return_table.columns) == 13:
            self.return_table.columns = [
                "Jan",
                "Feb",
                "Mar",
                "Apr",
                "May",
                "Jun",
                "Jul",
                "Aug",
                "Sep",
                "Oct",
                "Nov",
                "Dec",
                "YTD",
            ]

        self.lookback_returns = pd.Series(
            [
                self.mtd,
                self.three_month,
                self.six_month,
                self.ytd,
                self.one_year,
                self.three_year,
                self.five_year,
                self.ten_year,
                self.cagr,
            ],
            ["mtd", "3m", "6m", "ytd", "1y", "3y", "5y", "10y", "incep"],
        )
        self.lookback_returns.name = self.name
        self.stats = self._create_stats_series()

    def _calculate(self, obj: pd.Series):
        """
        Calculate Performance statistics

        Parameters
        ----------
        obj: pd.Series
            price Series
        """
        self.daily_mean = np.nan
        self.daily_vol = np.nan
        self.daily_sharpe = np.nan
        self.daily_sortino = np.nan
        self.best_day = np.nan
        self.worst_day = np.nan
        self.total_return = np.nan
        self.cagr = np.nan
        self.incep = np.nan
        self.drawdown = np.nan
        self.max_drawdown = np.nan
        self.daily_skew = np.nan
        self.daily_kurt = np.nan
        self.monthly_returns = np.nan
        self.monthly_mean = np.nan
        self.monthly_vol = np.nan
        self.monthly_sharpe = np.nan
        self.monthly_sortino = np.nan
        self.best_month = np.nan
        self.worst_month = np.nan
        self.mtd = np.nan
        self.three_month = np.nan
        self.pos_month_perc = np.nan
        self.avg_up_month = np.nan
        self.avg_down_month = np.nan
        self.monthly_skew = np.nan
        self.monthly_kurt = np.nan
        self.six_month = np.nan
        self.yearly_returns = np.nan
        self.ytd = np.nan
        self.one_year = np.nan
        self.yearly_mean = np.nan
        self.yearly_vol = np.nan
        self.yearly_sharpe = np.nan
        self.yearly_sortino = np.nan
        self.best_year = np.nan
        self.worst_year = np.nan
        self.three_year = np.nan
        self.win_year_perc = np.nan
        self.twelve_month_win_perc = np.nan
        self.yearly_skew = np.nan
        self.yearly_kurt = np.nan
        self.five_year = np.nan
        self.ten_year = np.nan
        self.calmar = np.nan

        self.return_table = {}

        if len(obj) == 0:
            return

        self.daily_prices = obj.resample("D").last().dropna()
        self.monthly_prices = obj.resample("M").last().dropna()
        self.yearly_prices = obj.resample("Y").last().dropna()

        if len(self.daily_prices) == 1:
            return

        self.mtd = self._calc_mtd()
        self.ytd = self._calc_ytd()

        # stats using daily data
        self.returns = self._to_returns(self.prices)
        self.log_returns = self._to_log_returns()

        if self.returns.index.to_series().diff().min() < pd.Timedelta("2 days"):
            self.daily_mean = self.returns.mean() * self.annualization_factor
            self.daily_vol = np.std(self.returns, ddof=1) * np.sqrt(
                self.annualization_factor
            )

            self.daily_sharpe = self._calc_sharpe(returns=self.returns)
            self.daily_sortino = self._calc_sortino_ratio(returns=self.returns)

            self.best_day = self.returns.max()
            self.worst_day = self.returns.min()

        self.total_return = obj.iloc[-1] / obj.iloc[0] - 1

        self.cagr = self._calc_cagr(prices=self.daily_prices)
        self.incep = self.cagr

        self.drawdown = self._to_drawdown_series()
        self.max_drawdown = self.drawdown.min()
        self.calmar = np.divide(self.cagr, np.abs(self.max_drawdown))

        if len(self.returns) < 4:
            return

        if self.returns.index.to_series().diff().min() <= pd.Timedelta("2 days"):
            self.daily_skew = self.returns.skew()

            if len(self.returns[(~np.isnan(self.returns)) & (self.returns != 0)]) > 0:
                self.daily_kurt = self.returns.kurt()

        # stats using monthly data
        self.monthly_returns = self._to_returns(self.monthly_prices)

        if len(self.monthly_returns) < 2:
            return

        if self.returns.index.to_series().diff().min() < pd.Timedelta("32 days"):
            self.monthly_mean = self.monthly_returns.mean() * 12
            self.monthly_vol = np.std(self.monthly_returns, ddof=1) * np.sqrt(12)

            self.monthly_sharpe = self._calc_sharpe(
                returns=self.monthly_returns, nperiods=12
            )
            self.monthly_sortino = self._calc_sortino_ratio(
                returns=self.monthly_returns, nperiods=12
            )

            self.best_month = self.monthly_returns.max()
            self.worst_month = self.monthly_returns.min()

            # -1 here to account for first return that will be nan
            self.pos_month_perc = len(
                self.monthly_returns[self.monthly_returns > 0]
            ) / float(len(self.monthly_returns) - 1)
            self.avg_up_month = self.monthly_returns[self.monthly_returns > 0].mean()
            self.avg_down_month = self.monthly_returns[self.monthly_returns <= 0].mean()

            # return_table
            for idx in self.monthly_returns.index:
                if idx.year not in self.return_table:
                    self.return_table[idx.year] = {
                        1: 0,
                        2: 0,
                        3: 0,
                        4: 0,
                        5: 0,
                        6: 0,
                        7: 0,
                        8: 0,
                        9: 0,
                        10: 0,
                        11: 0,
                        12: 0,
                    }
                if not np.isnan(self.monthly_returns[idx]):
                    self.return_table[idx.year][idx.month] = self.monthly_returns[idx]
            # add first month
            fidx = self.monthly_returns.index[0]
            try:
                self.return_table[fidx.year][fidx.month] = (
                    float(self.monthly_prices.iloc[0]) / self.daily_prices.iloc[0] - 1
                )
            except ZeroDivisionError:
                self.return_table[fidx.year][fidx.month] = 0
            # calculate the YTD values
            for idx in self.return_table:
                arr = np.array(list(self.return_table[idx].values()))
                self.return_table[idx][13] = np.prod(arr + 1) - 1

        if self.returns.index.to_series().diff().min() < pd.Timedelta("93 days"):
            if len(self.monthly_returns) < 3:
                return

            denom = self.daily_prices[
                : self.daily_prices.index[-1] - pd.DateOffset(months=3)
            ]
            if len(denom) > 0:
                self.three_month = self.daily_prices.iloc[-1] / denom.iloc[-1] - 1

        if self.returns.index.to_series().diff().min() < pd.Timedelta("32 days"):
            if len(self.monthly_returns) < 4:
                return

            self.monthly_skew = self.monthly_returns.skew()

            # if all zero/nan kurt fails division by zero
            if (
                len(
                    self.monthly_returns[
                        (~np.isnan(self.monthly_returns)) & (self.monthly_returns != 0)
                    ]
                )
                > 0
            ):
                self.monthly_kurt = self.monthly_returns.kurt()

        if self.returns.index.to_series().diff().min() < pd.Timedelta("185 days"):
            if len(self.monthly_returns) < 6:
                return

            denom = self.daily_prices[
                : self.daily_prices.index[-1] - pd.DateOffset(months=6)
            ]

            if len(denom) > 0:
                self.six_month = self.daily_prices.iloc[-1] / denom.iloc[-1] - 1

        # stats using yearly data
        if self.returns.index.to_series().diff().min() < pd.Timedelta("367 days"):
            self.yearly_returns = self._to_returns(self.yearly_prices)

            if len(self.yearly_returns) < 2:
                return

            denom = self.daily_prices[
                : self.daily_prices.index[-1] - pd.DateOffset(years=1)
            ]

            if len(denom) > 0:
                self.one_year = self.daily_prices.iloc[-1] / denom.iloc[-1] - 1

            self.yearly_mean = self.yearly_returns.mean()
            self.yearly_vol = np.std(self.yearly_returns, ddof=1)

            self.yearly_sharpe = self._calc_sharpe(
                returns=self.yearly_returns, nperiods=1
            )
            self.yearly_sortino = self._calc_sortino_ratio(
                returns=self.yearly_returns, nperiods=1
            )

            self.best_year = self.yearly_returns.max()
            self.worst_year = self.yearly_returns.min()

            # -1 here to account for first return that will be nan
            self.win_year_perc = len(
                self.yearly_returns[self.yearly_returns > 0]
            ) / float(len(self.yearly_returns) - 1)

            if self.monthly_returns.size > 11:
                tot = 0
                win = 0
                for i in range(11, len(self.monthly_returns)):
                    tot += 1
                    if (
                        self.monthly_prices.iloc[i] / self.monthly_prices.iloc[i - 11]
                        > 1
                    ):
                        win += 1
                self.twelve_month_win_perc = float(win) / tot

        if self.returns.index.to_series().diff().min() < pd.Timedelta("1097 days"):
            if len(self.yearly_returns) < 3:
                return

            # annualize stat for over 1 year
            self.three_year = self._calc_cagr(
                self.daily_prices[
                    self.daily_prices.index[-1] - pd.DateOffset(years=3) :
                ]
            )

        if self.returns.index.to_series().diff().min() < pd.Timedelta("367 days"):
            if len(self.yearly_returns) < 4:
                return

            self.yearly_skew = self.yearly_returns.skew()

            # if all zero/nan kurt fails division by zero
            if (
                len(
                    self.yearly_returns[
                        (~np.isnan(self.yearly_returns)) & (self.yearly_returns != 0)
                    ]
                )
                > 0
            ):
                self.yearly_kurt = self.yearly_returns.kurt()

        if self.returns.index.to_series().diff().min() < pd.Timedelta("1828 days"):
            if len(self.yearly_returns) < 5:
                return
            self.five_year = self._calc_cagr(
                self.daily_prices[
                    self.daily_prices.index[-1] - pd.DateOffset(years=5) :
                ]
            )

        if self.returns.index.to_series().diff().min() < pd.Timedelta("3654 days"):
            if len(self.yearly_returns) < 10:
                return
            self.ten_year = self._calc_cagr(
                self.daily_prices[
                    self.daily_prices.index[-1] - pd.DateOffset(years=10) :
                ]
            )
        return

    def _stats(self) -> list:
        """
        Returns
        -------
        list:
            list of labels for stats
        """
        stats = [
            ("_start", "Start", "dt"),
            ("_end", "End", "dt"),
            ("rf", "Risk-free rate", "p"),
            (None, None, None),
            ("total_return", "Total Return", "p"),
            ("cagr", "CAGR", "p"),
            ("max_drawdown", "Max Drawdown", "p"),
            ("calmar", "Calmar Ratio", "n"),
            (None, None, None),
            ("mtd", "MTD", "p"),
            ("three_month", "3m", "p"),
            ("six_month", "6m", "p"),
            ("ytd", "YTD", "p"),
            ("one_year", "1Y", "p"),
            ("three_year", "3Y (ann.)", "p"),
            ("five_year", "5Y (ann.)", "p"),
            ("ten_year", "10Y (ann.)", "p"),
            ("incep", "Since Incep. (ann.)", "p"),
            (None, None, None),
            ("daily_sharpe", "Daily Sharpe", "n"),
            ("daily_sortino", "Daily Sortino", "n"),
            ("daily_mean", "Daily Mean (ann.)", "p"),
            ("daily_vol", "Daily Vol (ann.)", "p"),
            ("daily_skew", "Daily Skew", "n"),
            ("daily_kurt", "Daily Kurt", "n"),
            ("best_day", "Best Day", "p"),
            ("worst_day", "Worst Day", "p"),
            (None, None, None),
            ("monthly_sharpe", "Monthly Sharpe", "n"),
            ("monthly_sortino", "Monthly Sortino", "n"),
            ("monthly_mean", "Monthly Mean (ann.)", "p"),
            ("monthly_vol", "Monthly Vol (ann.)", "p"),
            ("monthly_skew", "Monthly Skew", "n"),
            ("monthly_kurt", "Monthly Kurt", "n"),
            ("best_month", "Best Month", "p"),
            ("worst_month", "Worst Month", "p"),
            (None, None, None),
            ("yearly_sharpe", "Yearly Sharpe", "n"),
            ("yearly_sortino", "Yearly Sortino", "n"),
            ("yearly_mean", "Yearly Mean", "p"),
            ("yearly_vol", "Yearly Vol", "p"),
            ("yearly_skew", "Yearly Skew", "n"),
            ("yearly_kurt", "Yearly Kurt", "n"),
            ("best_year", "Best Year", "p"),
            ("worst_year", "Worst Year", "p"),
            (None, None, None),
            ("avg_up_month", "Avg. Up Month", "p"),
            ("avg_down_month", "Avg. Down Month", "p"),
            ("win_year_perc", "Win Year %", "p"),
            ("twelve_month_win_perc", "Win 12m %", "p"),
        ]

        return stats

    def set_date_range(
        self, start: datetime.date = None, end: datetime.date = None
    ) -> None:
        """
        Update date range of stats
        -> calculate stats on subset of range

        Parameters
        ----------
        start datetime.date, optional
            start date
        end: datetime.date, optional
            end date
        """
        self._start = self._start if start is None else pd.to_datetime(start)
        self._end = self._end if end is None else pd.to_datetime(end)
        self._update(self.prices.loc[self._start : self._end])

    def display(self) -> str:
        """
        Display overview containing descriptive stats for Series provided

        Returns
        -------
        str:
            formatted statistics
        """
        print("Stats for %s from %s - %s" % (self.name, self._start, self._end))
        if isinstance(self.rf, float):
            print("Annual risk-free rate considered: %s" % (fmtp(self.rf)))
        print("Summary:")
        data = [
            [
                fmtp(self.total_return),
                fmtn(self.daily_sharpe),
                fmtp(self.cagr),
                fmtp(self.max_drawdown),
            ]
        ]
        print(
            tabulate(data, headers=["Total Return", "Sharpe", "CAGR", "Max Drawdown"])
        )

        print("\nAnnualized Returns:")
        data = [
            [
                fmtp(self.mtd),
                fmtp(self.three_month),
                fmtp(self.six_month),
                fmtp(self.ytd),
                fmtp(self.one_year),
                fmtp(self.three_year),
                fmtp(self.five_year),
                fmtp(self.ten_year),
                fmtp(self.incep),
            ]
        ]
        print(
            tabulate(
                data,
                headers=["mtd", "3m", "6m", "ytd", "1y", "3y", "5y", "10y", "incep."],
            )
        )

        print("\nPeriodic:")
        data = [
            [
                "sharpe",
                fmtn(self.daily_sharpe),
                fmtn(self.monthly_sharpe),
                fmtn(self.yearly_sharpe),
            ],
            [
                "mean",
                fmtp(self.daily_mean),
                fmtp(self.monthly_mean),
                fmtp(self.yearly_mean),
            ],
            [
                "vol",
                fmtp(self.daily_vol),
                fmtp(self.monthly_vol),
                fmtp(self.yearly_vol),
            ],
            [
                "skew",
                fmtn(self.daily_skew),
                fmtn(self.monthly_skew),
                fmtn(self.yearly_skew),
            ],
            [
                "kurt",
                fmtn(self.daily_kurt),
                fmtn(self.monthly_kurt),
                fmtn(self.yearly_kurt),
            ],
            [
                "best",
                fmtp(self.best_day),
                fmtp(self.best_month),
                fmtp(self.best_year),
            ],
            [
                "worst",
                fmtp(self.worst_day),
                fmtp(self.worst_month),
                fmtp(self.worst_year),
            ],
        ]
        print(tabulate(data, headers=["daily", "monthly", "yearly"]))

        print("\nDrawdowns:")
        data = [
            [
                fmtp(self.max_drawdown),
            ]
        ]
        print(tabulate(data, headers=["max"]))

        print("\nMisc:")
        data = [
            ["avg. up month", fmtp(self.avg_up_month)],
            ["avg. down month", fmtp(self.avg_down_month)],
            ["up year %", fmtp(self.win_year_perc)],
            ["12m up %", fmtp(self.twelve_month_win_perc)],
        ]
        print(tabulate(data))

    def _create_stats_series(self) -> pd.Series:
        """
        Returns
        -------
        pd.Series:
            Series of statistics
        """
        stats = self._stats()

        short_names = []
        values = []

        for stat in stats:
            k, n, f = stat

            # blank row
            if k is None:
                continue
            elif k == "rf" and not isinstance(self.rf, float):
                continue

            if n in short_names:
                continue

            short_names.append(k)
            raw = getattr(self, k)
            values.append(raw)
        return pd.Series(values, short_names)

    def _deannualize(self, returns: pd.Series, nperiods: int = None) -> pd.Series:
        """
        Convert return expressed in annual terms on a different basis

        Parameters
        ----------
        nperiods: int, optional
            annualization factor
        returns: pd.Series
            annualized returns

        Returns
        -------
        pd.Series:
            deannualized returns
        """
        if nperiods is None:
            nperiods = self.annualization_factor
        return np.power(1 + returns, 1.0 / nperiods) - 1.0

    def _year_frac(self, start: datetime.date, end: datetime.date) -> float:
        """
        Similar to Excel's YEARFRAC function
        Calculate year fraction between two dates

        Parameters
        ----------
        start: datetime.date
            start date
        end: datetime.date
            end date

        Returns
        -------
        float:
            years between dates
        """
        if start > end:
            raise ValueError("start cannot be larger than end")

        return (end - start).total_seconds() / (31557600)

    def _calc_mtd(self) -> float:
        """
        Calculate MTD return of a price series
        -> use daily_prices if prices are only available for one month

        Returns
        -------
        float:
            MTD return
        """
        if len(self.monthly_prices) == 1:
            return self.daily_prices.iloc[-1] / self.daily_prices.iloc[0] - 1
        else:
            return self.daily_prices.iloc[-1] / self.monthly_prices.iloc[-2] - 1

    def _calc_ytd(self) -> float:
        """
        Calculate YTD return of a price series
        -> use daily_prices if prices are only available for one year

        Returns
        -------
        float:
            YTD return
        """
        if len(self.yearly_prices) == 1:
            return self.daily_prices.iloc[-1] / self.daily_prices.iloc[0] - 1
        else:
            return self.daily_prices.iloc[-1] / self.yearly_prices.iloc[-2] - 1

    def _to_returns(self, prices: pd.Series) -> pd.Series:
        """
        Simple Returns of price Series

        Parameters
        ----------
        prices: pd.Series
            price Series

        Returns
        -------
        pd.Series:
            day over day returns
        """
        return prices.pct_change().dropna()

    def _to_log_returns(self) -> pd.Series:
        """
        Simple Log Returns of price Series

        Returns
        -------
        pd.Series:
            day over day log returns
        """
        return np.log(self._to_returns(self.prices))

    def _to_excess_returns(self, returns: pd.Series, nperiods: int = None) -> pd.Series:
        """
        Given a Series of returns, calculate excess returns over rf.

        Returns
        -------
        returns: pd.Series
            return Series
        nperiods: int, optional
            annualization factor
        pd.Series:
            day over day excess returns
        """
        if nperiods is None:
            nperiods = self.annualization_factor

        _rf = self._deannualize(self.rf)
        return returns - _rf

    def _calc_sharpe(
        self, returns: pd.Series, nperiods: int = None, annualize: bool = True
    ) -> float:
        """
        Calculate Sharpe Ratio

        Parameters
        ----------
        returns: pd.Series
            return Series
        nperiods: int, optional
            annualization factor
        annualize: bool, optional
            annualize sharpe ratio

        Returns
        -------
        float:
            sharpe ratio
        """
        if nperiods is None:
            nperiods = self.annualization_factor

        er = self._to_excess_returns(returns=returns, nperiods=nperiods)
        std = np.std(er, ddof=1)
        res = np.divide(er.mean(), std)

        if annualize:
            return res * np.sqrt(nperiods)
        return res

    def _calc_sortino_ratio(
        self, returns: pd.Series, nperiods: int = None, annualize: bool = True
    ) -> float:
        """
        Calculate Sortino Ratio

        Parameters
        ----------
        returns: pd.Series
            return Series
        nperiods: int, optional
            annualization factor
        annualize: bool, optional
            annualize sharpe ratio

        Returns
        -------
        float:
            sortino ratio
        """
        if nperiods is None:
            nperiods = self.annualization_factor

        er = self._to_excess_returns(returns=returns, nperiods=nperiods)
        negative_returns = np.minimum(er, 0.0)
        std = np.std(negative_returns, ddof=1)
        res = np.divide(er.mean(), std)

        if annualize:
            return res * np.sqrt(nperiods)
        return res

    def _calc_cagr(self, prices: pd.Series) -> float:
        """
        Calculate CAGR (compound annual growth rate)

        Parameters
        ----------
        prices: pd.Series
            price Series

        Returns:
        --------
        float:
            CAGR
        """
        start = prices.index[0]
        end = prices.index[-1]
        return (prices.iloc[-1] / prices.iloc[0]) ** (
            1 / self._year_frac(start, end)
        ) - 1

    def _to_drawdown_series(self) -> pd.Series:
        """
        Calculates the  drawdown series

        Calculation
        -----------
        drawdown series = current / hwm - 1

        Returns
        -------
        pd.Series
            Drawdown Series
        """
        drawdown = self.prices.copy()
        drawdown[np.isnan(drawdown)] = -np.Inf

        if isinstance(drawdown, pd.DataFrame):
            roll_max = pd.DataFrame()
            for col in drawdown:
                roll_max[col] = np.maximum.accumulate(drawdown[col])
        else:
            roll_max = np.maximum.accumulate(drawdown)

        drawdown = drawdown / roll_max - 1.0
        return drawdown


class GroupStats(dict):
    """
    Statistics for multiple price Series in form of a dictionary

    Parameters
    ----------
    prices: pd.Series
        Multiple price series to be compared.
    """

    def __init__(self, *prices) -> None:
        names = []
        dfs = []
        for p in prices:
            if isinstance(p, pd.DataFrame):
                names.extend(p.columns)
                dfs.append(p)
            elif isinstance(p, pd.Series):
                names.append(p.name)
                tmpdf = pd.DataFrame({p.name: p})
                dfs.append(tmpdf)
        self._names = names

        # store original prices
        self._prices = pd.concat(dfs, axis=1).dropna()
        self._prices = self._prices[self._names]

        self._start = self._prices.index[0]
        self._end = self._prices.index[-1]
        self._update(self._prices)

    def __getitem__(self, key) -> PerformanceStats:
        """
        Defines behavior for when an item is accessed using the notation self[key]
        Returns Node object of key

        Parameters
        ----------
        key: str
            key of backtest

        Returns
        -------
        PerformanceStats:
            PerformanceStats of key
        """
        if isinstance(key, int):
            return self[self._names[key]]
        else:
            return self.get(key)

    def _update(self, data: pd.DataFrame) -> None:
        """
        Update Performance statistics

        Parameters
        ----------
        data: pd.DataFrame
            price DataFrame
        """
        self._calculate(data)
        self._update_stats()

    def _calculate(self, data: pd.DataFrame) -> None:
        """
        Calculate Performance statistics

        Parameters
        ----------
        data: pd.DataFrame
            price DataFrame
        """
        self.prices = data
        for c in data.columns:
            prc = data[c]
            self[c] = PerformanceStats(prc)

    def _stats(self) -> list:
        """
        Returns
        -------
        list:
            list of labels for stats
        """
        stats = [
            ("_start", "Start", "dt"),
            ("_end", "End", "dt"),
            ("rf", "Risk-free rate", "p"),
            (None, None, None),
            ("total_return", "Total Return", "p"),
            ("daily_sharpe", "Daily Sharpe", "n"),
            ("daily_sortino", "Daily Sortino", "n"),
            ("cagr", "CAGR", "p"),
            ("max_drawdown", "Max Drawdown", "p"),
            ("calmar", "Calmar Ratio", "n"),
            (None, None, None),
            ("mtd", "MTD", "p"),
            ("three_month", "3m", "p"),
            ("six_month", "6m", "p"),
            ("ytd", "YTD", "p"),
            ("one_year", "1Y", "p"),
            ("three_year", "3Y (ann.)", "p"),
            ("five_year", "5Y (ann.)", "p"),
            ("ten_year", "10Y (ann.)", "p"),
            ("incep", "Since Incep. (ann.)", "p"),
            (None, None, None),
            ("daily_sharpe", "Daily Sharpe", "n"),
            ("daily_sortino", "Daily Sortino", "n"),
            ("daily_mean", "Daily Mean (ann.)", "p"),
            ("daily_vol", "Daily Vol (ann.)", "p"),
            ("daily_skew", "Daily Skew", "n"),
            ("daily_kurt", "Daily Kurt", "n"),
            ("best_day", "Best Day", "p"),
            ("worst_day", "Worst Day", "p"),
            (None, None, None),
            ("monthly_sharpe", "Monthly Sharpe", "n"),
            ("monthly_sortino", "Monthly Sortino", "n"),
            ("monthly_mean", "Monthly Mean (ann.)", "p"),
            ("monthly_vol", "Monthly Vol (ann.)", "p"),
            ("monthly_skew", "Monthly Skew", "n"),
            ("monthly_kurt", "Monthly Kurt", "n"),
            ("best_month", "Best Month", "p"),
            ("worst_month", "Worst Month", "p"),
            (None, None, None),
            ("yearly_sharpe", "Yearly Sharpe", "n"),
            ("yearly_sortino", "Yearly Sortino", "n"),
            ("yearly_mean", "Yearly Mean", "p"),
            ("yearly_vol", "Yearly Vol", "p"),
            ("yearly_skew", "Yearly Skew", "n"),
            ("yearly_kurt", "Yearly Kurt", "n"),
            ("best_year", "Best Year", "p"),
            ("worst_year", "Worst Year", "p"),
            (None, None, None),
            ("avg_up_month", "Avg. Up Month", "p"),
            ("avg_down_month", "Avg. Down Month", "p"),
            ("win_year_perc", "Win Year %", "p"),
            ("twelve_month_win_perc", "Win 12m %", "p"),
        ]

        return stats

    def _update_stats(self) -> None:
        """
        Set Performance statistics
        """
        self.lookback_returns = pd.DataFrame(
            {x.lookback_returns.name: x.lookback_returns for x in self.values()}
        )

        self.stats = pd.DataFrame({x.name: x.stats for x in self.values()})

    def set_riskfree_rate(self, rf: Union[float, pd.Series]) -> None:
        """
        - set annual risk-free rate property
        - recalculate performance stats

        risk_free_rate: float | Series
            annual risk-free rate or risk-free rate price series
        """
        for key in self._names:
            self[key].set_riskfree_rate(rf)

        self._update_stats()

    def set_date_range(
        self, start: datetime.date = None, end: datetime.date = None
    ) -> None:
        """
        Update date range of stats
        -> calculate stats on subset of range

        Parameters
        ----------
        start datetime.date, optional
            start date
        end: datetime.date, optional
            end date
        """
        start = self._start if start is None else pd.to_datetime(start)
        end = self._end if end is None else pd.to_datetime(end)
        self._update(self._prices.loc[start:end])

    def display(self) -> str:
        """
        Display overview containing descriptive stats for Series provided

        Returns
        -------
        str:
            formatted statistics
        """
        data = []
        first_row = ["Stat"]
        first_row.extend(self._names)
        data.append(first_row)

        stats = self._stats()

        for stat in stats:
            k, n, f = stat
            # blank row
            if k is None:
                row = [""] * len(data[0])
                data.append(row)
                continue

            row = [n]
            for key in self._names:
                raw = getattr(self[key], k)

                # if rf is a series print nan
                if k == "rf" and not isinstance(raw, float):
                    row.append(np.nan)
                elif f is None:
                    row.append(raw)
                elif f == "p":
                    row.append(fmtp(raw))
                elif f == "n":
                    row.append(fmtn(raw))
                elif f == "dt":
                    row.append(raw.strftime("%Y-%m-%d"))
                else:
                    raise NotImplementedError("unsupported format %s" % f)
            data.append(row)

        print(tabulate(data, headers="firstrow"))
