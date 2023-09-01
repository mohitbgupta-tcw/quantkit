import numpy as np


class Allocation(object):
    """
    Base module for running allocation workflow
    """

    def __init__(self, asset_list, risk_engine=None, return_engine=None):
        """
        Parameters
        ----------
        asset_list: list
            all assets to run optimization on
        risk_engine: mstar_asset_allocation.risk_calc.risk_metrics, optional
            risk engine used to forecast cov matrix
        return_engine: mstar_asset_allocation.return_calc.return_metrics, optional
            return engine used to forecast returns
        """
        self.asset_list = asset_list
        self.num_total_assets = len(asset_list)
        self.risk_engine = risk_engine
        self.return_engine = return_engine
        self.allocations = None
        self.allocations_history = []

    def update(self):
        """
        Assign new forecasted cov matrix from risk engine
        and returns from return engine to optimizer
        """
        raise NotImplementedError

    def allocate(self, date):
        """
        Solve for optimal portfolio and save allocation

        Parameters
        ----------
        date : datetime.date
            date of snapshot
        """
        raise NotImplementedError

    def get_weights_constraints(self, w_consts_d):
        """
        convert to list of weight constraints

        Parameter
        ---------
        w_consts_d: dict
            dictionary of weight_constraints

        Return
        ------
        min_weights: np.array
            list of minimum weights
        max_weights: np.array
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
