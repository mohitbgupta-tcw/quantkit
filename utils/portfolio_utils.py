import pandas as pd

pd.options.mode.chained_assignment = None


def calculate_portfolio_waci(df: pd.DataFrame) -> float:
    """
    For a given portfolio, calculate the WACI

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame with data for one portfolio

    Returns
    -------
    float
        portfolio's WACI
    """
    df["market_weight_carbon_intensity"] = (
        df["Portfolio Weight"] / 100 * df["CARBON_EMISSIONS_SCOPE_12_INTEN"]
    )

    df_filtered = df[df["CARBON_EMISSIONS_SCOPE_12_INTEN"] > 0]

    df_filtered = df_filtered[
        df_filtered["Sector Level 2"].isin(
            ["Industrial", "Utility", "Financial Institution", "Quasi Sovereign"]
        )
    ]

    waci = (
        sum(df_filtered["market_weight_carbon_intensity"])
        / sum(df_filtered["Portfolio Weight"])
        * 100
    )
    return waci


def calculate_portfolio_esrm(df: pd.DataFrame) -> float:
    """
    For a given portfolio, calculate the market weighted ESRM score

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame with data for one portfolio

    Returns
    -------
    float
        portfolio's ESRM score
    """
    df["market_weight_esrm"] = df["Portfolio Weight"] / 100 * df["ESRM Score"]
    esrm = (
        sum(df[df["market_weight_esrm"] > 0]["market_weight_esrm"])
        / sum(df[df["market_weight_esrm"] > 0]["Portfolio Weight"])
        * 100
    )
    return esrm


def calculate_portfolio_governance(df: pd.DataFrame) -> float:
    """
    For a given portfolio, calculate the market weighted Governance score

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame with data for one portfolio

    Returns
    -------
    float
        portfolio's Governance score
    """
    df["market_weight_governance"] = (
        df["Portfolio Weight"] / 100 * df["Governance Score"]
    )
    gov = (
        sum(df[df["market_weight_governance"] > 0]["market_weight_governance"])
        / sum(df[df["market_weight_governance"] > 0]["Portfolio Weight"])
        * 100
    )
    return gov


def calculate_portfolio_transition(df: pd.DataFrame) -> float:
    """
    For a given portfolio, calculate the market weighted Transition score

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame with data for one portfolio

    Returns
    -------
    float
        portfolio's Transition score
    """
    df["market_weight_transition"] = (
        df["Portfolio Weight"] / 100 * df["Transition Score"]
    )
    trans = (
        sum(df[df["market_weight_transition"] > 0]["market_weight_transition"])
        / sum(df[df["market_weight_transition"] > 0]["Portfolio Weight"])
        * 100
    )
    return trans


def calculate_risk_distribution(df: pd.DataFrame) -> dict:
    """
    For a given portfolio, calculate score distribution,
    in particular the weight of the portfolio which has leading, average and poor score.

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame with data for one portfolio

    Returns
    -------
    dict
        dictionary with weight per score
    """
    scores = ["Leading ESG Score", "Average ESG Score", "Poor Risk Score", "Not Scored"]
    data = dict()
    for score in scores:
        weight = sum(df[df["Risk_Score_Overall"] == score]["Portfolio Weight"])
        data[score] = weight
    return data


def calculate_carbon_intensity(df: pd.DataFrame, portfolio_type: str) -> pd.DataFrame:
    """
    For a given portfolio, calculate carbon intensity per sector and return top 10
    highest contributers

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame with data for one portfolio
    portfolio_type: str
        portfolio type, either "equity" or "fixed_income"

    Returns
    -------
    pd.DataFrame
        DataFrame with sectors with hightest carbon intensity
    """
    df["market_weight_carbon_intensity"] = (
        df["Portfolio Weight"] / 100 * df["CARBON_EMISSIONS_SCOPE_12_INTEN"]
    )
    df_filtered = df[df["CARBON_EMISSIONS_SCOPE_12_INTEN"] > 0]

    df_filtered = df_filtered[
        df_filtered["Sector Level 2"].isin(
            ["Industrial", "Utility", "Financial Institution", "Quasi Sovereign"]
        )
    ]

    if portfolio_type == "equity":
        df_grouped = df_filtered.groupby("GICS_SECTOR", as_index=False).apply(
            lambda x: x["market_weight_carbon_intensity"].sum()
            / x["Portfolio Weight"].sum()
            * 100
        )
    elif portfolio_type == "fixed_income":
        df_grouped = df_filtered.groupby("BCLASS_SECTOR", as_index=False).apply(
            lambda x: x["market_weight_carbon_intensity"].sum()
            / x["Portfolio Weight"].sum()
            * 100
        )
    else:
        raise ValueError("""portfolio_type should be in ['fixed_income', 'equity']""")
    df_grouped.columns = ["Sector", "Carbon_Intensity"]
    df_grouped = df_grouped.sort_values(["Carbon_Intensity"], ascending=True)
    df_grouped = df_grouped[-10:]
    return df_grouped


def calculate_planet_distribution(df: pd.DataFrame) -> dict:
    """
    For a given portfolio, calculate score distribution of sustainable planet themes.

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame with data for one portfolio

    Returns
    -------
    dict
        dictionary with portfolio weight per theme
    """
    themes = [
        "RENEWENERGY",
        "MOBILITY",
        "CIRCULARITY",
        "CCADAPT",
        "BIODIVERSITY",
        "SMARTCITIES",
    ]
    data = dict()
    for theme in themes:
        weight = df[df["SCLASS_Level4-P"] == theme]["Portfolio Weight"].sum()
        data[theme] = weight
    return data


def calculate_people_distribution(df: pd.DataFrame) -> dict:
    """
    For a given portfolio, calculate score distribution of sustainable people themes.

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame with data for one portfolio

    Returns
    -------
    dict
        dictionary with portfolio weight per theme
    """
    themes = ["HEALTH", "SANITATION", "EDU", "INCLUSION", "NUTRITION", "AFFORDABLE"]
    data = dict()
    for theme in themes:
        weight = df[df["SCLASS_Level4-P"] == theme]["Portfolio Weight"].sum()
        data[theme] = weight
    return data


def calculate_bond_distribution(df: pd.DataFrame) -> dict:
    """
    For a given portfolio, calculate score distribution of labeled Bonds.

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame with data for one portfolio

    Returns
    -------
    dict
        dictionary with portfolio weight per bond type
    """
    bonds = [
        "Labeled Green",
        "Labeled Social",
        "Labeled Sustainable",
        "Labeled Sustainable Linked",
    ]
    data = dict()
    for bond in bonds:
        weight = df[df["Labeled ESG Type"] == bond]["Portfolio Weight"].sum()
        data[bond] = weight
    return data


def calculate_transition_distribution(df: pd.DataFrame) -> dict:
    """
    For a given portfolio, calculate score distribution of transition themes.

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame with data for one portfolio

    Returns
    -------
    dict
        dictionary with portfolio weight per theme
    """
    themes = [
        "LOWCARBON",
        "PIVOTTRANSPORT",
        "MATERIALS",
        "CARBONACCOUNT",
        "AGRIFORESTRY",
        "REALASSETS",
    ]
    data = dict()
    for theme in themes:
        weight = df[df["SCLASS_Level4-P"] == theme]["Portfolio Weight"].sum()
        data[theme] = weight
    return data
