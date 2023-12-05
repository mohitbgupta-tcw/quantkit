import sys, os

sys.path.append(os.getcwd())

import numpy as np
import pandas as pd
from scipy.stats import gmean
import quantkit.mathstats.covariance.simple_covariance as simple_covariance
import quantkit.mathstats.covariance.window_covariance as window_covariance
import quantkit.mathstats.covariance.expo_covariance as expo_covariance
import quantkit.mathstats.product.simple_cumprod as simple_cumprod
import quantkit.mathstats.product.rolling_cumprod as rolling_cumprod
import quantkit.mathstats.matrix.correlation as correlation


def test_integer_dataset():
    """
    Use intergers and:
    - Test quantkit simple covariance calculation - compare to np.cov()
    - Test quantkit simple mean calculation - compare to np.mean()
    - Test quantkit geometric mean calculation - compare to scipy.stats.gmean()
    - Test quantkit exponential weighted mean calculation - compare to pd.ewm()
    - Test quantkit simple cumprod calculation - compare to np.cumprod()
    - Test quantkit correlation - compare to np.corrcoef
    """
    data = np.random.randint(low=1, high=10, size=(4, 4))
    cov_model = simple_covariance.Covariance(min_observations=2, num_ind_variables=4)
    expo_model_adjusted = expo_covariance.ExponentialWeightedCovariance(
        num_ind_variables=4, min_observations=2, adjust=True
    )
    expo_model_unadjusted = expo_covariance.ExponentialWeightedCovariance(
        num_ind_variables=4, min_observations=2, adjust=False
    )
    prod_model = simple_cumprod.SimpleCumProd(num_ind_variables=4)

    for i in range(4):
        batch_ind = np.array(data[i])
        cov_model.update(batch_ind)
        expo_model_adjusted.update(batch_ind, batch_weight=0.9)
        expo_model_unadjusted.update(batch_ind, batch_weight=0.9)
        prod_model.update(batch_ind)

    df = pd.DataFrame(data)
    expected_cov = np.around(np.cov(data, rowvar=0), 6)
    expected_corr = np.around(np.corrcoef(data, rowvar=0), 6)
    expected_mean = np.around(np.mean(data, axis=0), 6)
    expected_gmean = np.around(gmean(data), 6)
    expected_cumprod = np.around(np.cumprod(data, axis=0)[-1, :], 6)
    expected_ewa_adjusted = np.around(
        np.array(df.ewm(adjust=True, alpha=0.1).mean().tail(1)).squeeze(), 6
    )
    expected_ewa_unadjusted = np.around(
        np.array(df.ewm(adjust=False, alpha=0.1).mean().tail(1)).squeeze(), 6
    )

    assert np.array_equal(np.around(cov_model.results["cov"], 6), expected_cov)
    assert np.array_equal(np.around(cov_model.results["mean"], 6), expected_mean)
    assert np.array_equal(np.around(cov_model.results["gmean"], 6), expected_gmean)
    assert np.array_equal(
        np.around(expo_model_adjusted.results["mean"], 6), expected_ewa_adjusted
    )
    assert np.array_equal(
        np.around(expo_model_unadjusted.results["mean"], 6), expected_ewa_unadjusted
    )
    assert np.array_equal(np.around(prod_model.cumprod, 6), expected_cumprod)
    assert np.array_equal(
        np.around(correlation.cov_to_corr(np.cov(data, rowvar=0)), 6), expected_corr
    )


def test_float_dataset():
    """
    Use floats in range -1 to 1 and:
    - Test quantkit simple covariance calculation - compare to np.cov()
    - Test quantkit simple mean calculation - compare to np.mean()
    - Test quantkit geometric mean with geobase calculation - compare to scipy.stats.gmean()
    - Test quantkit simple cumprod calculation - compare to np.cumprod()
    - Test quantkit correlation - compare to np.corrcoef
    """
    data = np.random.uniform(-1, 1, [7, 5])

    cov_model = simple_covariance.Covariance(
        min_observations=3, num_ind_variables=5, geo_base=1
    )
    prod_model = simple_cumprod.SimpleCumProd(num_ind_variables=5)
    expo_model_adjusted = expo_covariance.ExponentialWeightedCovariance(
        num_ind_variables=5, min_observations=3, adjust=True
    )
    expo_model_unadjusted = expo_covariance.ExponentialWeightedCovariance(
        num_ind_variables=5, min_observations=2, adjust=False
    )

    for i in range(7):
        batch_ind = np.array(data[i])
        cov_model.update(batch_ind)
        expo_model_adjusted.update(batch_ind, batch_weight=0.9)
        expo_model_unadjusted.update(batch_ind, batch_weight=0.9)
        prod_model.update(batch_ind)

    df = pd.DataFrame(data)
    expected_cov = np.around(np.cov(data, rowvar=0), 6)
    expected_corr = np.around(np.corrcoef(data, rowvar=0), 6)
    expected_mean = np.around(np.mean(data, axis=0), 6)
    expected_gmean = np.around(gmean(data + 1) - 1, 6)
    expected_cumprod = np.around(np.cumprod(data, axis=0)[-1, :], 6)
    expected_ewa_adjusted = np.around(
        np.array(df.ewm(adjust=True, alpha=0.1).mean().tail(1)).squeeze(), 6
    )
    expected_ewa_unadjusted = np.around(
        np.array(df.ewm(adjust=False, alpha=0.1).mean().tail(1)).squeeze(), 6
    )

    assert np.array_equal(np.around(cov_model.results["cov"], 6), expected_cov)
    assert np.array_equal(np.around(cov_model.results["mean"], 6), expected_mean)
    assert np.array_equal(np.around(cov_model.results["gmean"], 6), expected_gmean)
    assert np.array_equal(
        np.around(expo_model_adjusted.results["mean"], 6), expected_ewa_adjusted
    )
    assert np.array_equal(
        np.around(expo_model_unadjusted.results["mean"], 6), expected_ewa_unadjusted
    )
    assert np.array_equal(np.around(prod_model.cumprod, 6), expected_cumprod)
    assert np.array_equal(
        np.around(correlation.cov_to_corr(np.cov(data, rowvar=0)), 6), expected_corr
    )


def test_rolling_integer_dataset():
    """
    Use intergers and:
    - Test quantkit rolling covariance calculation - compare to pd.rolling(2).cov()
    - Test quantkit rolling mean calculation - compare to pd.rolling(2).mean()
    - Test quantkit rolling geometric mean calculation - compare to scipy.stats.gmean()
    - Test quantkit rolling cumprod calculation - compare to pd.rolling(2).cumprod()
    """
    data = np.random.randint(low=1, high=10, size=(5, 5))

    cov_model = window_covariance.WindowCovariance(
        num_ind_variables=5, window_size=2, ddof=1
    )
    prod_model = rolling_cumprod.RollingCumProd(num_ind_variables=5, window_size=2)

    quantkit_rolling_mean = []
    quantkit_geometric_mean = []
    quantkit_cumprod = []
    for i in range(len(data)):
        batch_ind = np.array(data[i])
        cov_model.update(batch_ind)

        outgoing_variable = prod_model.windowed_outgoing_row.squeeze()
        prod_model.update(batch_ind, outgoing_variable)

        if i > 0:
            quantkit_rolling_mean.append(np.around(cov_model.results["mean"], 6))
            quantkit_geometric_mean.append(np.around(cov_model.results["gmean"], 6))
            quantkit_cumprod.append(np.around(prod_model.cumprod, 6))

    df = pd.DataFrame(data)
    expected_cov = np.around(np.array(df.rolling(2).cov().tail(5)), 6)
    expected_mean = np.around(np.array(df.rolling(2).mean().dropna()), 6)
    expected_gmean = np.around(np.array(df.rolling(2).apply(gmean).dropna()), 6)
    expected_cumprod = np.around(
        np.array(df.rolling(window=2).agg(lambda x: x.prod()).dropna()), 6
    )

    assert np.array_equal(np.around(cov_model.results["cov"], 6), expected_cov)
    assert np.array_equal(quantkit_rolling_mean, expected_mean)
    assert np.array_equal(quantkit_geometric_mean, expected_gmean)
    assert np.array_equal(quantkit_cumprod, expected_cumprod)


def test_rolling_float_dataset():
    """
    Use floats in range -1 to 1 and:
    - Test quantkit rolling covariance calculation - compare to pd.rolling(4).cov()
    - Test quantkit rolling mean calculation - compare to pd.rolling(4).mean()
    - Test quantkit rolling geometric mean with geobase calculation - compare to scipy.stats.gmean()
    """
    data = np.random.uniform(-1, 1, [7, 5])

    cov_model = window_covariance.WindowCovariance(
        num_ind_variables=5, window_size=4, ddof=1, geo_base=1
    )
    prod_model = rolling_cumprod.RollingCumProd(num_ind_variables=5, window_size=4)

    quantkit_rolling_mean = []
    quantkit_geometric_mean = []
    quantkit_cumprod = []
    for i in range(len(data)):
        batch_ind = np.array(data[i])
        cov_model.update(batch_ind)

        outgoing_variable = prod_model.windowed_outgoing_row.squeeze()
        prod_model.update(batch_ind, outgoing_variable)

        if i > 2:
            quantkit_rolling_mean.append(np.around(cov_model.results["mean"], 6))
            quantkit_geometric_mean.append(np.around(cov_model.results["gmean"], 6))
            quantkit_cumprod.append(np.around(prod_model.cumprod, 6))

    df = pd.DataFrame(data)
    expected_cov = np.around(np.array(df.rolling(4).cov().tail(5)), 6)
    expected_mean = np.around(np.array(df.rolling(4).mean().dropna()), 6)
    expected_gmean = np.around(
        np.array((df + 1).rolling(4).apply(gmean).dropna()) - 1, 6
    )
    expected_cumprod = np.around(
        np.array(df.rolling(window=4).agg(lambda x: x.prod()).dropna()), 6
    )

    assert np.array_equal(np.around(cov_model.results["cov"], 6), expected_cov)
    assert np.array_equal(quantkit_rolling_mean, expected_mean)
    assert np.array_equal(quantkit_geometric_mean, expected_gmean)
    assert np.array_equal(quantkit_cumprod, expected_cumprod)


def test_emwa_cov():
    np.random.seed(0)
    data = np.random.rand(100, 2)

    model = expo_covariance.ExponentialWeightedCovariance(
        num_ind_variables=2, adjust=True, min_observations=2
    )

    for i in range(len(data)):
        # print(f"--------Iteration {model.total_iterations}--------")

        batch_ind = np.array(data[i])
        model.update(batch_ind, batch_weight=0.9)

    expected_cov_matrix = np.array(
        [
            [0.0812067095966652, -0.0063932574897836],
            [-0.0063932574897836, 0.0737351978481982],
        ]
    )

    assert np.array_equal(
        np.around(np.array(model.results["cov"]), 6),
        np.around(np.array(expected_cov_matrix), 6),
    )


if __name__ == "__main__":
    test_integer_dataset()
    test_float_dataset()
    test_rolling_integer_dataset()
    test_rolling_float_dataset()
    test_emwa_cov()
