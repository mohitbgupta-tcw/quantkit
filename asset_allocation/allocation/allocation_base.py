import numpy as np
import pandas as pd
from typing import Union
import datetime


class Allocation(object):
    """
    Base module for running allocation workflow

    Parameters
    ----------
    asset_list: list
        all assets to run optimization on
    risk_engine: mstar_asset_allocation.risk_calc.risk_metrics, optional
        risk engine used to forecast cov matrix
    return_engine: mstar_asset_allocation.return_calc.return_metrics, optional
        return engine used to forecast returns
    """

    def __init__(self, asset_list: list, risk_engine=None, return_engine=None) -> None:
        self.asset_list = asset_list
        self.num_total_assets = len(asset_list)
        self.risk_engine = risk_engine
        self.return_engine = return_engine
        self.allocations = None
        self.allocations_history = dict()

    def update(self, selected_assets: Union[list, np.array]) -> None:
        """
        - initialize optimizer
        - assign new forecasted cov matrix from risk engine to optimizer
        - assign forecasted returns from return engine to optimizer

        Parameters
        ----------
        selected_assets: list | np.array
            list of selected assets (their location in universe as integer)
        """
        raise NotImplementedError

    def allocate(
        self, date: datetime.date, selected_assets: Union[list, np.array]
    ) -> None:
        """
        Solve for optimal portfolio and save allocation

        Parameters
        ----------
        date : datetime.date
            date of snapshot
        selected_assets: list | np.array
            list of selected assets (their location in universe as integer)
        """
        raise NotImplementedError

    def get_weights_constraints(self, w_consts_d: dict):
        """
        Create weight constraints:
        - if no weight constraints is passed -> min 0%, max 100%
        - if weight constraint dict is passed -> add constraint for each asset in dict

        Parameter
        ---------
        w_consts_d: dict
            dictionary of weight_constraints

        Return
        ------
        np.array
            list of minimum weights
        np.array
            list of maximum weights
        """

        default_min_weights = np.zeros(self.num_total_assets)
        default_max_weights = np.ones(self.num_total_assets)
        if w_consts_d is None or not len(w_consts_d):
            return default_min_weights, default_max_weights

        for ix, this_asset in enumerate(self.asset_list):
            this_constraint = w_consts_d.get(this_asset)
            if this_constraint is not None:
                this_min_weight, this_max_weight = this_constraint
                default_min_weights[ix] = this_min_weight
                default_max_weights[ix] = this_max_weight

        return default_min_weights, default_max_weights
