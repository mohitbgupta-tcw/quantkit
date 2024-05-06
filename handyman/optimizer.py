import pandas as pd
import numpy as np
from quantkit.backtester.allocation.min_variance import MinimumVariance
from quantkit.backtester.allocation.mean_variance import MeanVariance, VolTarget
import quantkit.backtester.return_calc.log_return as log_return
import quantkit.backtester.risk_calc.log_vol as log_vol


def minimum_variance(
    returns_df: pd.DataFrame,
    window_size: int,
    weights_constraint: dict = None,
    max_weight_turnover: float = None,
) -> pd.DataFrame:
    """
    Run Minimum Variance Optimization over time

    Parameters
    ----------
    returns_df: pd.DataFrame
        DataFrame of asset returns
    window_size: int
        lookback window for covariance calculation
    weight_constraint: dict, optional
        dictionary of weight_constraints for assets
    max_weight_turnover: float, optional
        maximum threshold for weight change per asset

    Returns
    -------
    pd.DataFrame:
        DataFrame of weights for assets over time
    """
    universe = list(returns_df.columns)
    selected_assets = [i for i in range(len(universe))]
    risk_engine = log_vol.WindowLogNormalVol(universe=universe, window_size=window_size)

    minvar_optimizer = MinimumVariance(
        asset_list=universe,
        risk_engine=risk_engine,
        return_engine=None,
        weights_constraint=weights_constraint,
    )
    for date, row in returns_df.iterrows():
        return_array = row.to_numpy()
        risk_engine.assign(date, return_array, annualize_factor=1)
        if risk_engine.is_valid():
            if max_weight_turnover:
                curr_weights = minvar_optimizer.allocations[1]
                if not np.all(np.isnan(curr_weights)):
                    current_weights_constraint = {
                        fund: [
                            np.max(
                                [
                                    curr_weights[i] - max_weight_turnover,
                                    weights_constraint[fund][0],
                                ]
                            ),
                            np.min(
                                [
                                    curr_weights[i] + max_weight_turnover,
                                    weights_constraint[fund][1],
                                ]
                            ),
                        ]
                        for i, fund in enumerate(universe)
                    }
                    (
                        minvar_optimizer.min_weights,
                        minvar_optimizer.max_weights,
                    ) = minvar_optimizer.get_weights_constraints(
                        current_weights_constraint
                    )
            minvar_optimizer.update(selected_assets=selected_assets)
            minvar_optimizer.allocate(date=date, selected_assets=selected_assets)

    weights = pd.DataFrame.from_dict(
        minvar_optimizer.allocations_history,
        orient="index",
        columns=universe,
    )
    return weights


def mean_variance(
    returns_df: pd.DataFrame,
    window_size_risk: int,
    window_size_return: int,
    weights_constraint: dict = None,
    max_weight_turnover: float = None,
    duration_target: float = None,
    duration_array: np.ndarray = None,
) -> pd.DataFrame:
    """
    Run Mean Variance Optimization over time

    Parameters
    ----------
    returns_df: pd.DataFrame
        DataFrame of asset returns
    window_size: int
        lookback window for covariance calculation
    weight_constraint: dict, optional
        dictionary of weight_constraints for assets
    max_weight_turnover: float, optional
        maximum threshold for weight change per asset
    duration_target: float, optional
        duration target of underlying funds
        must specified if duration_array also
    duration_array: np.array
        array of durations of underlying funds
        must be specified if duration_target is specified

    Returns
    -------
    pd.DataFrame:
        DataFrame of weights for assets over time
    """
    universe = list(returns_df.columns)
    selected_assets = [i for i in range(len(universe))]

    risk_engine = log_vol.WindowLogNormalVol(
        universe=universe, window_size=window_size_risk
    )
    return_engine = log_return.LogReturn(
        universe=universe, window_size=window_size_return
    )

    mvo_optimizer = MeanVariance(
        asset_list=universe,
        risk_engine=risk_engine,
        return_engine=return_engine,
        weights_constraint=weights_constraint,
    )
    for date, row in returns_df.iterrows():
        return_array = row.to_numpy()
        risk_engine.assign(date, return_array, annualize_factor=1)
        return_engine.assign(date, return_array, annualize_factor=1)
        if risk_engine.is_valid() and return_engine.is_valid():
            if max_weight_turnover:
                if not mvo_optimizer.allocations is None and not np.all(
                    np.isnan(mvo_optimizer.allocations[1])
                ):
                    curr_weights = mvo_optimizer.allocations[1]
                    current_weights_constraint = {
                        fund: [
                            np.max(
                                [
                                    curr_weights[i] - max_weight_turnover,
                                    weights_constraint[fund][0],
                                ]
                            ),
                            np.min(
                                [
                                    curr_weights[i] + max_weight_turnover,
                                    weights_constraint[fund][1],
                                ]
                            ),
                        ]
                        for i, fund in enumerate(universe)
                    }
                    (
                        mvo_optimizer.min_weights,
                        mvo_optimizer.max_weights,
                    ) = mvo_optimizer.get_weights_constraints(
                        current_weights_constraint
                    )
            mvo_optimizer.update(selected_assets=selected_assets)

            if duration_target:
                mvo_optimizer.optimizer._add_constraint(
                    mvo_optimizer.optimizer._sum(
                        mvo_optimizer.optimizer._multiply(
                            mvo_optimizer.optimizer.weights, duration_array
                        )
                    )
                    >= duration_target
                )

            mvo_optimizer.allocate(date=date, selected_assets=selected_assets)
    weights = pd.DataFrame.from_dict(
        mvo_optimizer.allocations_history,
        orient="index",
        columns=universe,
    )
    return weights


def volatility_target(
    returns_df: pd.DataFrame,
    vol_target: float,
    window_size_risk: int,
    window_size_return: int,
    weights_constraint: dict = None,
    max_weight_turnover: float = None,
) -> pd.DataFrame:
    """
    Run MVO with volatility target over time

    Parameters
    ----------
    returns_df: pd.DataFrame
        DataFrame of asset returns
    vol_target: float
        volatility target
    window_size: int
        lookback window for covariance calculation
    weight_constraint: dict, optional
        dictionary of weight_constraints for assets
    max_weight_turnover: float, optional
        maximum threshold for weight change per asset

    Returns
    -------
    pd.DataFrame:
        DataFrame of weights for assets over time
    """
    universe = list(returns_df.columns)
    selected_assets = [i for i in range(len(universe))]

    risk_engine = log_vol.WindowLogNormalVol(
        universe=universe, window_size=window_size_risk
    )
    return_engine = log_return.LogReturn(
        universe=universe, window_size=window_size_return
    )

    vol_optimizer = VolTarget(
        asset_list=universe,
        vol_target=vol_target,
        risk_engine=risk_engine,
        return_engine=return_engine,
        weights_constraint=weights_constraint,
    )
    for date, row in returns_df.iterrows():
        return_array = row.to_numpy()
        risk_engine.assign(date, return_array, annualize_factor=1)
        return_engine.assign(date, return_array, annualize_factor=1)
        if risk_engine.is_valid() and return_engine.is_valid():
            if max_weight_turnover:
                if not vol_optimizer.allocations is None and not np.all(
                    np.isnan(vol_optimizer.allocations[1])
                ):
                    curr_weights = vol_optimizer.allocations[1]
                    current_weights_constraint = {
                        fund: [
                            np.max(
                                [
                                    curr_weights[i] - max_weight_turnover,
                                    weights_constraint[fund][0],
                                ]
                            ),
                            np.min(
                                [
                                    curr_weights[i] + max_weight_turnover,
                                    weights_constraint[fund][1],
                                ]
                            ),
                        ]
                        for i, fund in enumerate(universe)
                    }
                    (
                        vol_optimizer.min_weights,
                        vol_optimizer.max_weights,
                    ) = vol_optimizer.get_weights_constraints(
                        current_weights_constraint
                    )
            vol_optimizer.update(selected_assets=selected_assets)
            vol_optimizer.allocate(date=date, selected_assets=selected_assets)
    weights = pd.DataFrame.from_dict(
        vol_optimizer.allocations_history,
        orient="index",
        columns=universe,
    )
    return weights
