TargetA = ["Approved SBT"]
TargetAA = ["Approved SBT", "Ambitious Target"]
TargetAAC = ["Approved SBT", "Committed SBT", "Ambitious Target"]
TargetAACN = [
    "Approved SBT",
    "Committed SBT",
    "Ambitious Target",
    "Non-Ambitious Target",
]
TargetAC = ["Approved SBT", "Committed SBT"]
TargetCA = ["Committed SBT", "Ambitious Target"]
TargetCN = ["Committed SBT", "Non-Ambitious Target"]
TargetN = ["Non-Ambitious Target"]


def include_rule(industry, inclusions, **kwargs):
    """
    Checks if a company's industry is in inclusion list
    (list of industries company's industry should be in to be included in theme)

    Parameters
    ----------
    industry: str
        name of industry company belongs to
    exclusions: list
        list of industries for inclusion

    Returns
    -------
    bool
        industry included
    """
    if industry in inclusions:
        return True
    return False


def bigger_eq_rule(val: float, threshold: float, **kwargs):
    """
    Check if value inputted is bigger or equal than specified threshold

    Parameters
    ----------
    val: float
        value
    threshold: float
        threshold value

    Returns
    -------
    bool
        input is bigger than threshold
    """
    if val >= threshold:
        return True
    return False


def eq_rule(val: float, threshold: float, **kwargs):
    """
    Check if value inputted is equal to specified threshold

    Parameters
    ----------
    val: float
        value
    threshold: float
        threshold value

    Returns
    -------
    bool
        input is bigger than threshold
    """
    if val == threshold:
        return True
    return False


def bigger_rule(val: float, threshold: float, **kwargs):
    """
    Check if value inputted is bigger than specified threshold

    Parameters
    ----------
    val: float
        value
    threshold: float
        threshold value

    Returns
    -------
    bool
        input is bigger than threshold
    """
    if val > threshold:
        return True
    return False


def OilandGas(
    reduction_target: str,
    sbti_commited_target: int,
    sbti_approved_target: int,
    capex: float,
    climate_rev: float,
    biofuel_rev: float,
    **kwargs
):
    """
    Check Transition target for oil and gas companies

    Parameters
    ----------
    reduction_target: str
        ClimateGHGReductionTargets
    sbti_commited_target: int
        company has committed to work on a science-based emission reduction target
    sbti_approved_target: int
        company has one or more active carbon emissions reduction target/s
        approved by the Science Based Targets initiative
    capex: float
        capex
    climate_rev: float
        all revenues derived from any of the climate change environment impact themes
    biofuel_rev: float
        recent-year percentage of revenue a company has derived from
        non-virgin source biofuels

    Returns
    -------
    bool
        rule is fulfilled
    """
    if (
        include_rule(reduction_target, TargetAAC)
        or eq_rule(sbti_commited_target, 1)
        or eq_rule(sbti_approved_target, 1)
    ) and (bigger_rule(capex, 0) or bigger_rule(climate_rev, 0)):
        return True
    elif (bigger_eq_rule(capex, 15) and bigger_rule(climate_rev, 0)) or (
        bigger_rule(capex, 0) and bigger_eq_rule(climate_rev, 15)
    ):
        return True
    elif bigger_eq_rule(climate_rev, 25) or bigger_rule(biofuel_rev, 25):
        return True
    return False


def CoalFuels(
    alt_energy_rev: float, climate_rev: float, thermal_coal_rev: float, **kwargs
):
    """
    Check Transition target for coal companies

    Parameters
    ----------
    alt_energy_rev: float
        recent-year percentage of revenue a company has derived from products,
        services, or infrastructure projects supporting the development or
        delivery of renewable energy and alternative fuels
    climate_rev: float
        all revenues derived from any of the climate change environment impact themes
    thermal_coal_rev: float
        maximum percentage of revenue greater than 0% that a company derives
        from the mining of thermal coal

    Returns
    -------
    bool
        rule is fulfilled
    """
    if (
        bigger_eq_rule(alt_energy_rev, 95)
        or bigger_eq_rule(climate_rev, 95)
        or eq_rule(thermal_coal_rev, 0)
    ):
        return True
    return False


def IndGases(
    reduction_target: str,
    sbti_commited_target: int,
    sbti_approved_target: int,
    climate_rev: float,
    company_name: str,
    **kwargs
):
    """
    Check Transition target for industrial gases companies

    Parameters
    ----------
    reduction_target: str
        ClimateGHGReductionTargets
    sbti_commited_target: int
        company has committed to work on a science-based emission reduction target
    sbti_approved_target: int
        company has one or more active carbon emissions reduction target/s
        approved by the Science Based Targets initiative
    climate_rev: float
        all revenues derived from any of the climate change environment impact themes
    company_name: str
        name of company

    Returns
    -------
    bool
        rule is fulfilled
    """
    if (
        include_rule(reduction_target, TargetAAC)
        or eq_rule(sbti_commited_target, 1)
        or eq_rule(sbti_approved_target, 1)
        or include_rule("Lithium", company_name)
    ):
        return True
    elif (
        include_rule(reduction_target, TargetAACN)
        or eq_rule(sbti_commited_target, 1)
        or eq_rule(sbti_approved_target, 1)
    ) and bigger_eq_rule(climate_rev, 5):
        return True
    return False


def Utilities(
    reduction_target: str,
    sbti_commited_target: int,
    sbti_approved_target: int,
    capex: float,
    climate_rev: float,
    **kwargs
):
    """
    Check Transition target for utilities companies

    Parameters
    ----------
    reduction_target: str
        ClimateGHGReductionTargets
    sbti_commited_target: int
        company has committed to work on a science-based emission reduction target
    sbti_approved_target: int
        company has one or more active carbon emissions reduction target/s
        approved by the Science Based Targets initiative
    capex: float
        capex
    climate_rev: float
        all revenues derived from any of the climate change environment impact themes

    Returns
    -------
    bool
        rule is fulfilled
    """
    if reduction_target == "Approved SBT" or eq_rule(sbti_approved_target, 1):
        return True
    elif reduction_target == "Ambitious Target" and (
        bigger_eq_rule(capex, 30) or bigger_eq_rule(climate_rev, 20)
    ):
        return True
    elif (
        reduction_target == "Committed SBT" or eq_rule(sbti_commited_target, 1)
    ) and bigger_eq_rule(climate_rev, 35):
        return True
    elif bigger_eq_rule(climate_rev, 40):
        return True
    return False


def Target_A(reduction_target: str, sbti_approved_target: int, **kwargs):
    """
    Check Transition target for Target A companies

    Parameters
    ----------
    reduction_target: str
        ClimateGHGReductionTargets
    sbti_approved_target: int
        company has one or more active carbon emissions reduction target/s
        approved by the Science Based Targets initiative

    Returns
    -------
    bool
        rule is fulfilled
    """
    if include_rule(reduction_target, TargetA) or eq_rule(sbti_approved_target, 1):
        return True
    return False


def Target_AA(reduction_target: str, sbti_approved_target: int, **kwargs):
    """
    Check Transition target for Target AA companies

    Parameters
    ----------
    reduction_target: str
        ClimateGHGReductionTargets
    sbti_approved_target: int
        company has one or more active carbon emissions reduction target/s
        approved by the Science Based Targets initiative

    Returns
    -------
    bool
        rule is fulfilled
    """
    if include_rule(reduction_target, TargetAA) or eq_rule(sbti_approved_target, 1):
        return True
    return False


def Target_AAC(
    reduction_target: str,
    sbti_approved_target: int,
    sbti_commited_target: int,
    **kwargs
):
    """
    Check Transition target for Target AAC companies

    Parameters
    ----------
    reduction_target: str
        ClimateGHGReductionTargets
    sbti_approved_target: int
        company has one or more active carbon emissions reduction target/s
        approved by the Science Based Targets initiative
    sbti_commited_target: int
        company has committed to work on a science-based emission reduction target

    Returns
    -------
    bool
        rule is fulfilled
    """
    if (
        include_rule(reduction_target, TargetAAC)
        or eq_rule(sbti_approved_target, 1)
        or eq_rule(sbti_commited_target, 1)
    ):
        return True
    return False


def Target_AACN(
    reduction_target: str,
    sbti_approved_target: int,
    sbti_commited_target: int,
    **kwargs
):
    """
    Check Transition target for Target AACN companies

    Parameters
    ----------
    reduction_target: str
        ClimateGHGReductionTargets
    sbti_approved_target: int
        company has one or more active carbon emissions reduction target/s
        approved by the Science Based Targets initiative
    sbti_commited_target: int
        company has committed to work on a science-based emission reduction target

    Returns
    -------
    bool
        rule is fulfilled
    """
    if (
        include_rule(reduction_target, TargetAACN)
        or eq_rule(sbti_approved_target, 1)
        or eq_rule(sbti_commited_target, 1)
    ):
        return True
    return False


def Target_AC(
    reduction_target: str,
    sbti_approved_target: int,
    sbti_commited_target: int,
    **kwargs
):
    """
    Check Transition target for Target AC companies

    Parameters
    ----------
    reduction_target: str
        ClimateGHGReductionTargets
    sbti_approved_target: int
        company has one or more active carbon emissions reduction target/s
        approved by the Science Based Targets initiative
    sbti_commited_target: int
        company has committed to work on a science-based emission reduction target

    Returns
    -------
    bool
        rule is fulfilled
    """
    if (
        include_rule(reduction_target, TargetAC)
        or eq_rule(sbti_approved_target, 1)
        or eq_rule(sbti_commited_target, 1)
    ):
        return True
    return False


def Target_CA(reduction_target: str, sbti_commited_target: int, **kwargs):
    """
    Check Transition target for Target CA companies

    Parameters
    ----------
    reduction_target: str
        ClimateGHGReductionTargets
    sbti_commited_target: int
        company has committed to work on a science-based emission reduction target

    Returns
    -------
    bool
        rule is fulfilled
    """
    if include_rule(reduction_target, TargetCA) or eq_rule(sbti_commited_target, 1):
        return True
    return False


def Target_CN(reduction_target: str, sbti_commited_target: int, **kwargs):
    """
    Check Transition target for Target CN companies

    Parameters
    ----------
    reduction_target: str
        ClimateGHGReductionTargets
    sbti_commited_target: int
        company has committed to work on a science-based emission reduction target

    Returns
    -------
    bool
        rule is fulfilled
    """
    if include_rule(reduction_target, TargetCN) or eq_rule(sbti_commited_target, 1):
        return True
    return False


def Target_N(reduction_target: str, **kwargs):
    """
    Check Transition target for Target N companies

    Parameters
    ----------
    reduction_target: str
        ClimateGHGReductionTargets

    Returns
    -------
    bool
        rule is fulfilled
    """
    if include_rule(reduction_target, TargetN):
        return True
    return False


def Target_NRev(reduction_target: str, climate_rev: float, capex: float, **kwargs):
    """
    Check Transition target for Target NRev companies

    Parameters
    ----------
    reduction_target: str
        ClimateGHGReductionTargets
    climate_rev: float
        all revenues derived from any of the climate change environment impact themes
    capex: float
        capex

    Returns
    -------
    bool
        rule is fulfilled
    """
    if (
        include_rule(reduction_target, TargetN) and bigger_rule(climate_rev, 0)
    ) or bigger_rule(capex, 10):
        return True
    return False


def Revenue(climate_rev: float, revenue_threshold: int, capex: float):
    """
    Check Transition Revenue Target

    Parameters
    ----------
    climate_rev: float
        all revenues derived from any of the climate change environment impact themes
    revenue_threshold: float
        minimum revenue threshold
    capex: float
        capex

    Returns
    -------
    bool
        rule is fulfilled
    """
    if bigger_eq_rule(climate_rev, revenue_threshold) or bigger_eq_rule(capex, 10):
        return True
    return False
