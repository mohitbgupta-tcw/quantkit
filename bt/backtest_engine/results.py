import pandas as pd
from matplotlib import pyplot as plt
from typing import Union
from quantkit.bt.performance_statistics.stats import GroupStats


class Result(GroupStats):
    """
    Calculate Stats and Results for backtests

    Parameters
    ----------
    backtests: list
        list of backtests
    """

    def __init__(self, *backtests) -> None:
        tmp = [pd.DataFrame({x.name: x.strategy.prices}) for x in backtests]
        super().__init__(*tmp)
        self.backtest_list = backtests
        self.backtests = {x.name: x for x in backtests}

    def _get_backtest(self, backtest: Union[str, int]) -> str:
        """
        Return name of backtest

        Parameters
        ----------
        backtest: str | int
            identifier of backtest, can be either index (int) or name (str)

        Returns
        -------
        str:
            name of backtest
        """
        if isinstance(backtest, int):
            return self.backtest_list[backtest].name

        return backtest

    def get_data(self, backtest: Union[str, int] = 0) -> pd.DataFrame:
        """
        Get Data for specified backtest

        Parameters
        ----------
        backtest: str | int, optional
            identifier of backtest, can be either index (int) or name (str)

        Returns
        -------
        pd.DataFrame
            DataFrame of price, value, notional_value, cash, fees, flows of strategy
        """
        key = self._get_backtest(backtest)
        data = self.backtests[key].strategy.data
        return data

    def get_universe(self, backtest: Union[str, int] = 0) -> pd.DataFrame:
        """
        Get Universe (underlying prices of securities) for specified backtest

        Parameters
        ----------
        backtest: str | int, optional
            identifier of backtest, can be either index (int) or name (str)

        Returns
        -------
        pd.DataFrame
            DataFrame of universe prices
        """
        key = self._get_backtest(backtest)
        data = self.backtests[key].strategy.universe
        return data

    def get_weights(self, backtest: Union[str, int] = 0) -> pd.DataFrame:
        """
        Get Weights for specified backtest

        Parameters
        ----------
        backtest: str | int, optional
            identifier of backtest, can be either index (int) or name (str)

        Returns
        -------
        pd.DataFrame
            DataFrame of weights
        """
        key = self._get_backtest(backtest)
        data = self.backtests[key].weights
        return data

    def get_security_weights(self, backtest: Union[str, int] = 0) -> pd.DataFrame:
        """
        Get security Weights for specified backtest

        Parameters
        ----------
        backtest: str | int, optional
            identifier of backtest, can be either index (int) or name (str)

        Returns
        -------
        pd.DataFrame
            DataFrame of security weights
        """
        key = self._get_backtest(backtest)
        data = self.backtests[key].security_weights
        return data

    def get_positions(self, backtest: Union[str, int] = 0) -> pd.DataFrame:
        """
        Get security positions for specified backtest

        Parameters
        ----------
        backtest: str | int, optional
            identifier of backtest, can be either index (int) or name (str)

        Returns
        -------
        pd.DataFrame
            DataFrame of positions
        """
        key = self._get_backtest(backtest)
        data = self.backtests[key].positions
        return data

    def get_transactions(self, backtest: Union[str, int] = 0) -> pd.DataFrame:
        """
        Get strategy transactions for specified backtest

        Parameters
        ----------
        backtest: str | int, optional
            identifier of backtest, can be either index (int) or name (str)

        Returns
        -------
        pd.DataFrame
            DataFrame of transactions in format Date, Security | quantity, price
        """
        key = self._get_backtest(backtest)
        data = self.backtests[key].strategy.get_transactions()
        return data

    def get_outlays(self, backtest: Union[str, int] = 0) -> pd.DataFrame:
        """
        Get strategy outlays for specified backtest
        outlays are total dollar amount spent (gained) by purchase (sale) of securities

        Parameters
        ----------
        backtest: str | int, optional
            identifier of backtest, can be either index (int) or name (str)

        Returns
        -------
        pd.DataFrame
            DataFrame of outlays
        """
        key = self._get_backtest(backtest)
        data = self.backtests[key].strategy.outlays
        mask = (data != 0).any(axis=1)
        return data[mask]


class RandomBenchmarkResult(Result):
    """
    Calculate Stats and Results for backtests

    Parameters
    ----------
    backtests: list
        list of backtests
    """

    def __init__(self, *backtests) -> None:
        super().__init__(*backtests)
        self.base_name = backtests[0].name
        self.r_stats = self.stats.drop(self.base_name, axis=1)
        self.b_stats = self.stats[self.base_name]
