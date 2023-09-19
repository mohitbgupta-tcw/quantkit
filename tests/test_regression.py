import sys, os

sys.path.append(os.getcwd())

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
import quantkit.mathstats.regression.ols_regression as lr


def window_regression_dataset():
    """
    For a dataset, test quantkits linear regression against scikit-learn, espicially
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


if __name__ == "__main__":
    window_regression_dataset()
