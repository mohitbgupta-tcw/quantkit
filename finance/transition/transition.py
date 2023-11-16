import quantkit.utils.util_functions as util_functions
import quantkit.utils.mapping_configs as mapping_configs


def OilandGas(
    reduction_target: str,
    sbti_commited_target: int,
    sbti_approved_target: int,
    capex: float,
    climate_rev: float,
    biofuel_rev: float,
    **kwargs,
) -> bool:
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
        util_functions.include_rule(reduction_target, mapping_configs.TargetAAC)
        or util_functions.eq_rule(sbti_commited_target, 1)
        or util_functions.eq_rule(sbti_approved_target, 1)
    ) and (
        util_functions.bigger_rule(capex, 0)
        or util_functions.bigger_rule(climate_rev, 0)
    ):
        return True
    elif (
        util_functions.bigger_eq_rule(capex, 15)
        and util_functions.bigger_rule(climate_rev, 0)
    ) or (
        util_functions.bigger_rule(capex, 0)
        and util_functions.bigger_eq_rule(climate_rev, 15)
    ):
        return True
    elif util_functions.bigger_eq_rule(climate_rev, 25) or util_functions.bigger_rule(
        biofuel_rev, 25
    ):
        return True
    return False


def CoalFuels(
    alt_energy_rev: float, climate_rev: float, thermal_coal_rev: float, **kwargs
) -> bool:
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
        util_functions.bigger_eq_rule(alt_energy_rev, 95)
        or util_functions.bigger_eq_rule(climate_rev, 95)
        or util_functions.eq_rule(thermal_coal_rev, 0)
    ):
        return True
    return False


def IndGases(
    reduction_target: str,
    sbti_commited_target: int,
    sbti_approved_target: int,
    climate_rev: float,
    company_name: str,
    **kwargs,
) -> bool:
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
        util_functions.include_rule(reduction_target, mapping_configs.TargetAAC)
        or util_functions.eq_rule(sbti_commited_target, 1)
        or util_functions.eq_rule(sbti_approved_target, 1)
        or util_functions.include_rule("Lithium", company_name)
    ):
        return True
    elif (
        util_functions.include_rule(reduction_target, mapping_configs.TargetAACN)
        or util_functions.eq_rule(sbti_commited_target, 1)
        or util_functions.eq_rule(sbti_approved_target, 1)
    ) and util_functions.bigger_eq_rule(climate_rev, 5):
        return True
    return False


def Utilities(
    reduction_target: str,
    sbti_commited_target: int,
    sbti_approved_target: int,
    capex: float,
    climate_rev: float,
    **kwargs,
) -> bool:
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
    if reduction_target == "Approved SBT" or util_functions.eq_rule(
        sbti_approved_target, 1
    ):
        return True
    elif reduction_target == "Ambitious Target" and (
        util_functions.bigger_eq_rule(capex, 30)
        or util_functions.bigger_eq_rule(climate_rev, 20)
    ):
        return True
    elif (
        reduction_target == "Committed SBT"
        or util_functions.eq_rule(sbti_commited_target, 1)
    ) and util_functions.bigger_eq_rule(climate_rev, 35):
        return True
    elif util_functions.bigger_eq_rule(climate_rev, 40):
        return True
    return False


def Target_A(reduction_target: str, sbti_approved_target: int, **kwargs) -> bool:
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
    if util_functions.include_rule(
        reduction_target, mapping_configs.TargetA
    ) or util_functions.eq_rule(sbti_approved_target, 1):
        return True
    return False


def Target_AA(reduction_target: str, sbti_approved_target: int, **kwargs) -> bool:
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
    if util_functions.include_rule(
        reduction_target, mapping_configs.TargetAA
    ) or util_functions.eq_rule(sbti_approved_target, 1):
        return True
    return False


def Target_AAC(
    reduction_target: str,
    sbti_approved_target: int,
    sbti_commited_target: int,
    **kwargs,
) -> bool:
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
        util_functions.include_rule(reduction_target, mapping_configs.TargetAAC)
        or util_functions.eq_rule(sbti_approved_target, 1)
        or util_functions.eq_rule(sbti_commited_target, 1)
    ):
        return True
    return False


def Target_AACN(
    reduction_target: str,
    sbti_approved_target: int,
    sbti_commited_target: int,
    **kwargs,
) -> bool:
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
        util_functions.include_rule(reduction_target, mapping_configs.TargetAACN)
        or util_functions.eq_rule(sbti_approved_target, 1)
        or util_functions.eq_rule(sbti_commited_target, 1)
    ):
        return True
    return False


def Target_AC(
    reduction_target: str,
    sbti_approved_target: int,
    sbti_commited_target: int,
    **kwargs,
) -> bool:
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
        util_functions.include_rule(reduction_target, mapping_configs.TargetAC)
        or util_functions.eq_rule(sbti_approved_target, 1)
        or util_functions.eq_rule(sbti_commited_target, 1)
    ):
        return True
    return False


def Target_CA(reduction_target: str, sbti_commited_target: int, **kwargs) -> bool:
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
    if util_functions.include_rule(
        reduction_target, mapping_configs.TargetCA
    ) or util_functions.eq_rule(sbti_commited_target, 1):
        return True
    return False


def Target_CN(reduction_target: str, sbti_commited_target: int, **kwargs) -> bool:
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
    if util_functions.include_rule(
        reduction_target, mapping_configs.TargetCN
    ) or util_functions.eq_rule(sbti_commited_target, 1):
        return True
    return False


def Target_N(reduction_target: str, **kwargs) -> bool:
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
    if util_functions.include_rule(reduction_target, mapping_configs.TargetN):
        return True
    return False


def Target_NRev(
    reduction_target: str, climate_rev: float, capex: float, **kwargs
) -> bool:
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
        util_functions.include_rule(reduction_target, mapping_configs.TargetN)
        and util_functions.bigger_rule(climate_rev, 0)
    ) or util_functions.bigger_rule(capex, 10):
        return True
    return False


def Revenue(climate_rev: float, revenue_threshold: int, capex: float) -> bool:
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
    if util_functions.bigger_eq_rule(
        climate_rev, revenue_threshold
    ) or util_functions.bigger_eq_rule(capex, 10):
        return True
    return False
