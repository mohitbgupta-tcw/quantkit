import sys, os

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression, Ridge
from sklearn.metrics import r2_score
import quantkit.mathstats.regression.ols_regression as lr
import quantkit.mathstats.regression.ridge_regression as rr

from quantkit.tests.shared_test_utils import *


def test_window_ols_dataset():
    """
    For a dataset, test quantkits linear regression against scikit-learn, especially
    - beta = coef_
    - sigma = intercept_
    - r_squared
    """

    window_size = 6

    dep_variables = np.random.uniform(-10, 10, [7, 5])
    ind_variables = np.random.uniform(-4, 4, [7, 2])

    # quantkit
    regression = lr.OrdinaryLR(window_size=6, num_dep_variables=5, num_ind_variables=2)

    for dep, ind in zip(dep_variables, ind_variables):
        ind = np.array(ind)
        dep = np.array(dep)

        regression.update(ind, dep)

    # scikit learn
    dep_variables2 = dep_variables[-window_size:, :]
    ind_variables2 = ind_variables[-window_size:, :]

    reg = LinearRegression()
    reg = reg.fit(ind_variables2, dep_variables2)

    r_list = []
    pred = reg.predict(ind_variables2)
    for i in range(5):
        r_list.append(
            np.around(r2_score(np.array(dep_variables2)[:, i], pred[:, i]), 6)
        )

    assert np.array_equal(
        np.around(regression.results["beta"].squeeze(), 6),
        np.around(reg.coef_.T.squeeze(), 6),
    )
    assert np.array_equal(
        np.around(regression.results["sigma"].squeeze(), 6),
        np.around(reg.intercept_.squeeze(), 6),
    )
    assert np.array_equal(
        np.around(regression.results["r_squared"].squeeze(), 6), r_list
    )


def test_window_ridge_dataset():
    """
    For a dataset, test quantkits ridge regression against scikit-learn, especially
    - beta = coef_
    - sigma = intercept_
    - r_squared
    """
    window_size = 6

    dep_variables = np.random.uniform(-10, 10, [7, 5])
    ind_variables = np.random.uniform(-4, 4, [7, 2])

    # quantkit
    regression = rr.RidgeLR(
        window_size=6, num_dep_variables=5, num_ind_variables=2, alpha=1
    )

    for dep, ind in zip(dep_variables, ind_variables):
        ind = np.array(ind)
        dep = np.array(dep)

        regression.update(ind, dep)

    # scikit learn
    dep_variables2 = dep_variables[-window_size:, :]
    ind_variables2 = ind_variables[-window_size:, :]

    reg = Ridge(alpha=1)
    reg = reg.fit(ind_variables2, dep_variables2)

    assert np.array_equal(
        np.around(regression.results["beta"].squeeze(), 6),
        np.around(reg.coef_.T.squeeze(), 6),
    )
    assert np.array_equal(
        np.around(regression.results["sigma"].squeeze(), 6),
        np.around(reg.intercept_.squeeze(), 6),
    )


if __name__ == "__main__":
    test_window_ols_dataset()
    test_window_ridge_dataset()
