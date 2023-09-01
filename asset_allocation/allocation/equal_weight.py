import quantkit.asset_allocation.allocation.allocation_base as allocation_base
import numpy as np


class EqualWeight(allocation_base.Allocation):
    def __init__(self, asset_list, risk_engine, return_engine, **kwargs):
        """
        Parameters
        ----------
        asset_list: list
            assets to run optimization on
        risk_engine: mstar_asset_allocation.risk_calc.risk_metrics
            risk engine used to forecast cov matrix
        return_engine: mstar_asset_allocation.return_calc.return_metrics
            return engine used to forecast returns
        """
        super().__init__(asset_list, risk_engine, return_engine)

    def update(self, **kwargs):
        """
        Assign new forecasted cov matrix from risk engine
        and return from return engine to optimizer
        """
        return

    def allocate(self, date, selected_assets):
        """
        Solve for optimal portfolio and save allocation

        Parameters
        ----------
        date : datetime.date
            date of snapshot
        """
        asset_count = len(selected_assets)
        allocation = np.zeros(shape=self.num_total_assets)
        opt_allocation = tuple(np.array([1 / asset_count] * asset_count))
        allocation[selected_assets] = opt_allocation

        self.allocations = ((date,), allocation)
        self.allocations_history.append(self.allocations)
        return
