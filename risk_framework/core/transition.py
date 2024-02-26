import quantkit.utils.util_functions as util_functions
import quantkit.utils.mapping_configs as mapping_configs


def calculate_transition_tag(
    ENABLER_IMPROVER: str,
    climate_rev: float,
    TRANSITION_REVENUE_ENABLER: str,
    TRANSITION_REVENUE_IMPROVER: str,
    company_capex: float,
    CAPEX: str,
    company_decarb: float,
    DECARB: str,
    reduction_target: str,
    sbti_approved_target: int,
    sbti_commited_target: int,
    **kwargs
) -> bool:
    """
    Check if company fulfills transition tag logic

    Parameters
    ----------
    ENABLER_IMPROVER: str
        company's sub industry has enabler or improver tag
    climate_rev: float
        all revenues derived from any of the climate change environment impact themes
    TRANSITION_REVENUE_ENABLER: str
        minimum revenue threshold in form "REVENUE_level" for enabler
    TRANSITION_REVENUE_IMPROVER: str
        minimum revenue threshold in form "REVENUE_level" for improver
    company_capex: float
        capex
    CAPEX: str
        minimum capex threshold in form "CAPEX_level"
    company_decarb: float
        capex
    DECARB: str
        minimum capex threshold in form "REDUCTION_level"
    reduction_target: str
        ClimateGHGReductionTargets
    sbti_approved_target: int
        company has one or more active carbon emissions reduction target/s
        approved by the Science Based Targets initiative
    sbti_commited_target: int
        company has committed to work on a science-based emission reduction target

    Returns
    -------
    bool:
        company fulfills transition logic
    """
    if ENABLER_IMPROVER == "ENABLER":
        return calculate_enabler_tag(
            climate_rev=climate_rev, TRANSITION_REVENUE=TRANSITION_REVENUE_ENABLER
        )
    elif ENABLER_IMPROVER == "IMPROVER":
        return calculate_improver_tag(
            climate_rev=climate_rev,
            TRANSITION_REVENUE=TRANSITION_REVENUE_IMPROVER,
            company_capex=company_capex,
            CAPEX=CAPEX,
            company_decarb=company_decarb,
            DECARB=DECARB,
            reduction_target=reduction_target,
            sbti_approved_target=sbti_approved_target,
            sbti_commited_target=sbti_commited_target,
        )


def calculate_enabler_tag(climate_rev: float, TRANSITION_REVENUE: str) -> bool:
    """
    Check if company fulfills enabler transition logic

    Logic
    -----
        climate_revenue > revenue_threshold

    Parameters
    ----------
    climate_rev: float
        all revenues derived from any of the climate change environment impact themes
    TRANSITION_REVENUE: str
        minimum revenue threshold in form "REVENUE_level"

    Returns
    -------
    bool:
        enabler fulfills enabler transition logic
    """
    return check_revenue(climate_rev=climate_rev, TRANSITION_REVENUE=TRANSITION_REVENUE)


def calculate_improver_tag(
    climate_rev: float,
    TRANSITION_REVENUE: str,
    company_capex: float,
    CAPEX: str,
    company_decarb: float,
    DECARB: str,
    reduction_target: str,
    sbti_approved_target: int,
    sbti_commited_target: int,
) -> bool:
    """
    Check if company fulfills improver transition logic

    Logic
    -----
        has AAC tag
        AND
            climate_revenue > revenue_threshold OR
            capex_revenue > capex_threshold

    Parameters
    ----------
    climate_rev: float
        all revenues derived from any of the climate change environment impact themes
    TRANSITION_REVENUE: str
        minimum revenue threshold in form "REVENUE_level"
    company_capex: float
        capex
    CAPEX: str
        minimum capex threshold in form "CAPEX_level"
    company_decarb: float
        capex
    DECARB: str
        minimum capex threshold in form "REDUCTION_level"
    reduction_target: str
        ClimateGHGReductionTargets
    sbti_approved_target: int
        company has one or more active carbon emissions reduction target/s
        approved by the Science Based Targets initiative
    sbti_commited_target: int
        company has committed to work on a science-based emission reduction target

    Returns
    -------
    bool:
        enabler fulfills improver transition logic
    """
    target = check_target(
        reduction_target=reduction_target,
        sbti_approved_target=sbti_approved_target,
        sbti_commited_target=sbti_commited_target,
    )
    revenue = check_revenue(
        climate_rev=climate_rev, TRANSITION_REVENUE=TRANSITION_REVENUE
    )
    capex = check_capex(company_capex=company_capex, CAPEX=CAPEX)

    decarb = check_decarb(company_decarb=company_decarb, DECARB=DECARB)
    return target and (revenue or capex or decarb)


def check_target(
    reduction_target: str,
    sbti_approved_target: int,
    sbti_commited_target: int,
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


def check_revenue(climate_rev: float, TRANSITION_REVENUE: str) -> bool:
    """
    Check Transition Revenue Target

    Parameters
    ----------
    climate_rev: float
        all revenues derived from any of the climate change environment impact themes
    TRANSITION_REVENUE: str
        minimum revenue threshold in form "REVENUE_level"

    Returns
    -------
    bool
        rule is fulfilled
    """
    level = int(TRANSITION_REVENUE.split("_")[1])
    if util_functions.bigger_eq_rule(climate_rev, level):
        return True
    return False


def check_capex(company_capex: float, CAPEX: str) -> bool:
    """
    Check Transition CapEx Target

    Parameters
    ----------
    company_capex: float
        capex
    CAPEX: str
        minimum capex threshold in form "CAPEX_level"

    Returns
    -------
    bool
        rule is fulfilled
    """
    level = int(CAPEX.split("_")[1])
    if util_functions.bigger_eq_rule(company_capex, level):
        return True
    return False


def check_decarb(company_decarb: float, DECARB: str) -> bool:
    """
    Check Transition CapEx Target

    Parameters
    ----------
    company_decarb: float
        capex
    DECARB: str
        minimum capex threshold in form "REDUCTION_level"

    Returns
    -------
    bool
        rule is fulfilled
    """
    level = int(DECARB.split("_")[1]) / 100
    if company_decarb <= -level:
        return True
    return False
