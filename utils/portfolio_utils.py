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

    if not df_filtered.empty:
        waci = (
            sum(df_filtered["market_weight_carbon_intensity"])
            / sum(df_filtered["Portfolio Weight"])
            * 100
        )
    else:
        waci = 0
    return waci


def calculate_portfolio_coverage(df: pd.DataFrame) -> float:
    """
    For a given portfolio, calculate the coverage for risk scores

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame with data for one portfolio

    Returns
    -------
    float
        portfolio's coverage
    """
    df_filtered = df[
        ((df["Governance Score"] == 5) & (df["NA_Flags_Governance"] >= 7))
        | ((df["ESRM Score"] == 5) & (df["NA_Flags_ESRM"] >= 7))
    ]
    if not df_filtered.empty:
        cov = sum(df_filtered["Portfolio Weight"])
    else:
        cov = 0
    return 100 - cov


def calculate_portfolio_msci_coverage(df: pd.DataFrame) -> float:
    """
    For a given portfolio, calculate the msci coverage for risk scores

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame with data for one portfolio

    Returns
    -------
    float
        portfolio's coverage
    """
    df_filtered = df[
        (
            (df["INDUSTRY_ADJUSTED_SCORE"].isna())
            & (
                df["Sector Level 2"].isin(
                    [
                        "Industrial",
                        "Utility",
                        "Financial Institution",
                        "Quasi Sovereign",
                    ]
                )
            )
        )
    ]
    if not df_filtered.empty:
        cov = sum(df_filtered["Portfolio Weight"])
    else:
        cov = 0
    return 100 - cov


def calculate_portfolio_msci_score(df: pd.DataFrame) -> float:
    """
    For a given portfolio, calculate the market weighted MSCI score

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame with data for one portfolio

    Returns
    -------
    float
        portfolio's ESRM score
    """
    df["market_weight_msci"] = (
        df["Portfolio Weight"] / 100 * df["INDUSTRY_ADJUSTED_SCORE"]
    )
    df_filtered = df[df["INDUSTRY_ADJUSTED_SCORE"] > 0]
    if not df_filtered.empty:
        msci = (
            sum(
                df_filtered[df_filtered["market_weight_msci"] > 0]["market_weight_msci"]
            )
            / sum(
                df_filtered[df_filtered["market_weight_msci"] > 0]["Portfolio Weight"]
            )
            * 100
        )
    else:
        msci = 0
    return msci


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
    df_filtered = df[~((df["ESRM Score"] == 5) & (df["NA_Flags_ESRM"] >= 7))]
    if not df_filtered.empty:
        esrm = (
            sum(
                df_filtered[df_filtered["market_weight_esrm"] > 0]["market_weight_esrm"]
            )
            / sum(
                df_filtered[df_filtered["market_weight_esrm"] > 0]["Portfolio Weight"]
            )
            * 100
        )
    else:
        esrm = 0
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
    df_filtered = df[
        ~((df["Governance Score"] == 5) & (df["NA_Flags_Governance"] >= 7))
    ]
    if not df_filtered.empty:
        gov = (
            sum(
                df_filtered[df_filtered["market_weight_governance"] > 0][
                    "market_weight_governance"
                ]
            )
            / sum(
                df_filtered[df_filtered["market_weight_governance"] > 0][
                    "Portfolio Weight"
                ]
            )
            * 100
        )
    else:
        gov = 0
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
    df_filtered = df[~((df["ESRM Score"] == 5) & (df["NA_Flags_ESRM"] >= 7))]
    df_filtered = df_filtered[
        ~(
            (df_filtered["Governance Score"] == 5)
            & (df_filtered["NA_Flags_Governance"] >= 7)
        )
    ]
    if not df_filtered.empty:
        trans = (
            sum(
                df_filtered[df_filtered["market_weight_transition"] > 0][
                    "market_weight_transition"
                ]
            )
            / sum(
                df_filtered[df_filtered["market_weight_transition"] > 0][
                    "Portfolio Weight"
                ]
            )
            * 100
        )
    else:
        trans = 0
    return trans


def calculate_portfolio_sovereign(df: pd.DataFrame) -> float:
    """
    For a given portfolio, calculate the market weighted Sovereign score

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame with data for one portfolio

    Returns
    -------
    float
        portfolio's Sovereign score
    """
    df["market_weight_sovereign"] = df["Portfolio Weight"] / 100 * df["Sovereign Score"]
    df_filtered = df[df["market_weight_sovereign"] > 0]
    if not df_filtered.empty:
        sov = (
            sum(df_filtered["market_weight_sovereign"])
            / sum(df_filtered["Portfolio Weight"])
            * 100
        )
    else:
        sov = 0
    return sov


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

    if portfolio_type in ["equity", "equity_a9", "equity_msci"]:
        df_grouped = df_filtered.groupby("GICS_SECTOR", as_index=False).apply(
            lambda x: x["market_weight_carbon_intensity"].sum()
            / x["Portfolio Weight"].sum()
            * 100
        )
    elif portfolio_type in ["fixed_income", "fixed_income_a9", "fixed_income_a8"]:
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
        "Labeled Green/Sustainable Linked",
        "Labeled Sustainable/Sustainable Linked",
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


def calculate_country_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """
    For a given portfolio, calculate country distribution of labeled bonds.

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame with data for one portfolio

    Returns
    -------
    pd.DataFrame
        DataFrame with % of total of labeled Bonds per Country
    """
    bonds = [
        "Labeled Green",
        "Labeled Social",
        "Labeled Sustainable",
        "Labeled Sustainable Linked",
        "Labeled Green/Sustainable Linked",
        "Labeled Sustainable/Sustainable Linked",
    ]
    df_filtered = df[df["Labeled ESG Type"].isin(bonds)]
    df_filtered = (
        df_filtered.groupby("Country of Risk")["Portfolio Weight"].sum().reset_index()
    )
    df_filtered["Contribution"] = (
        df_filtered["Portfolio Weight"] / df_filtered["Portfolio Weight"].sum()
    )
    df_filtered = df_filtered.sort_values("Contribution", ascending=False)
    df_filtered = df_filtered[["Country of Risk", "Contribution"]]
    df_filtered["Country of Risk"] = df_filtered["Country of Risk"].str.title()
    return df_filtered


def calculate_sector_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """
    For a given portfolio, calculate sector distribution of labeled bonds.

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame with data for one portfolio

    Returns
    -------
    pd.DataFrame
        DataFrame with % of total of labeled Bonds per Sector
    """
    bonds = [
        "Labeled Green",
        "Labeled Social",
        "Labeled Sustainable",
        "Labeled Sustainable Linked",
        "Labeled Green/Sustainable Linked",
        "Labeled Sustainable/Sustainable Linked",
    ]
    df_filtered = df[df["Labeled ESG Type"].isin(bonds)]
    df_filtered = (
        df_filtered.groupby("JPM Sector")["Portfolio Weight"].sum().reset_index()
    )
    df_filtered["Contribution"] = (
        df_filtered["Portfolio Weight"] / df_filtered["Portfolio Weight"].sum()
    )
    df_filtered = df_filtered.sort_values("Contribution", ascending=False)
    df_filtered = df_filtered[["JPM Sector", "Contribution"]]
    df_filtered["JPM Sector"] = df_filtered["JPM Sector"].str.title()
    return df_filtered


def calculate_sustainable_classification(df: pd.DataFrame) -> pd.DataFrame:
    """
    For a given portfolio, calculate sustainable classification (SCLASS Level 2) distribution

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame with data for one portfolio

    Returns
    -------
    pd.DataFrame
        DataFrame with SCLASS Level 2 weights
    """
    color = {
        "ESG-Labeled Bonds": "#82b460",
        "Transition": "#0072a0",
        "ESG Scores": "#91bcd1",
        "Excluded Sector": "#cd523a",
        "Poor Data": "#f0a787",
        "Poor Risk Score": "#cd523a",
        "Not Scored": "#bebcbb",
        "Sustainable Theme": "#bde6b8",
    }
    sort = {
        "ESG-Labeled Bonds": 1,
        "Sustainable Theme": 2,
        "Transition": 3,
        "ESG Scores": 4,
        "Poor Risk Score": 5,
        "Excluded Sector": 6,
        "Poor Data": 7,
        "Not Scored": 8,
    }
    df_grouped = df.groupby("SCLASS_Level2")["Portfolio Weight"].sum().reset_index()
    df_grouped["Sort"] = df_grouped["SCLASS_Level2"].map(sort)
    df_grouped = df_grouped.sort_values("Sort", ignore_index=True)
    df_grouped = df_grouped.drop("Sort", axis=1)
    df_grouped["Color"] = df_grouped["SCLASS_Level2"].map(color)
    return df_grouped


def calculate_portfolio_summary(df: pd.DataFrame, portfolio_type: str) -> dict:
    """
    For a given portfolio, calculate the portfolio summary

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame with data for one portfolio
    portfolio_type: str
        portfolio type, either "equity" or "fixed_income"

    Returns
    -------
    dict
        portfolio's summary
    """
    summary = dict()
    scores_people = calculate_people_distribution(df)
    scores_planet = calculate_planet_distribution(df)
    total = sum(scores_people.values()) + sum(scores_planet.values())

    if portfolio_type in ["fixed_income", "fixed_income_a9", "fixed_income_a8", "em"]:
        bonds = calculate_bond_distribution(df)
        total += sum(bonds.values())

    excluded = df[df["SCLASS_Level1"] == "Excluded"]["Portfolio Weight"].sum()

    scores = calculate_risk_distribution(df)
    other = scores["Not Scored"]

    summary["sustainable_theme"] = total
    summary["exclusion"] = excluded
    summary["other"] = other
    summary["sustainable_managed"] = 100 - total - other - excluded
    return summary


def calculate_bclass2_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """
    For a given portfolio, calculate BCLASS Level 2 distribution

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame with data for one portfolio

    Returns
    -------
    pd.DataFrame
        DataFrame with distribution
    """

    bonds = [
        "Labeled Green",
        "Labeled Social",
        "Labeled Sustainable",
        "Labeled Sustainable Linked",
        "Labeled Green/Sustainable Linked",
        "Labeled Sustainable/Sustainable Linked",
    ]
    df_filtered = df[df["Labeled ESG Type"].isin(bonds)]

    df_filtered = (
        df_filtered.groupby("BCLASS_Level2")["Portfolio Weight"].sum().reset_index()
    )
    df_filtered["Contribution"] = (
        df_filtered["Portfolio Weight"] / df_filtered["Portfolio Weight"].sum()
    )
    df_filtered = df_filtered.sort_values("Contribution", ascending=False)
    df_filtered = df_filtered[["BCLASS_Level2", "Contribution"]]
    return df_filtered


def calculate_bclass3_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """
    For a given portfolio, calculate BCLASS Level 3 distribution

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame with data for one portfolio

    Returns
    -------
    pd.DataFrame
        DataFrame with distribution
    """

    bonds = [
        "Labeled Green",
        "Labeled Social",
        "Labeled Sustainable",
        "Labeled Sustainable Linked",
        "Labeled Green/Sustainable Linked",
        "Labeled Sustainable/Sustainable Linked",
    ]
    df_filtered = df[df["Labeled ESG Type"].isin(bonds)]

    df_filtered = (
        df_filtered.groupby("BCLASS_Level3")["Portfolio Weight"].sum().reset_index()
    )
    df_filtered["Contribution"] = (
        df_filtered["Portfolio Weight"] / df_filtered["Portfolio Weight"].sum()
    )
    df_filtered = df_filtered.sort_values("Contribution", ascending=False)
    df_filtered = df_filtered[["BCLASS_Level3", "Contribution"]]
    return df_filtered
