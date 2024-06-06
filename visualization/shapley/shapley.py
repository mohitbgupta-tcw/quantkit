from lightgbm import LGBMClassifier
import shap
import numpy as np


def plot_shapley(X: np.ndarray, y: np.ndarray, max_depth: int = 3) -> None:
    """
    Return Shapley plot based on underlying LGBM tree model

    Parameters
    ----------
    X: np.array
        independent variables
    y: np.array
        dependent variables
    max_depth: int, optional
        depth of tree
    """

    cat_features = []
    for cat in X.select_dtypes(exclude="number"):
        cat_features.append(cat)
        X[cat] = X[cat].astype("category").cat.codes.astype("category")

    clf = LGBMClassifier(max_depth=max_depth, n_estimators=1000, objective="binary")
    clf.fit(X, y, eval_set=(X, y))

    explainer = shap.TreeExplainer(clf)
    shap_values = explainer.shap_values(X)
    y = clf.predict(X).astype("bool")
    shap.summary_plot(shap_values[1], X.astype("float"))
