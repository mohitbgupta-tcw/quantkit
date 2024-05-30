import pandas as pd
import quantkit.utils.snowflake_utils as snowflake_utils


def company_information(isins: list, local_configs: str = "") -> pd.DataFrame:
    """
    For a given list of security ISIN's, load company information such as
        - Ticker
        - Name
        - Region of Origin
        - Industry
        - Sector

    Parameters
    ----------
    isins: list
        list of company isins
    local_configs: str, optional
        path to a local configarations file

    Returns
    -------
    pd.DataFrame
        DataFrame with columns
            - ISIN
            - TICKERSYMBOL
            - COMPANYNAME
            - COUNTRY
            - INDUSTRY
            - INDUSTRY_GROUP
            - SECTOR
    """
    sec_string = ", ".join(f"'{sec}'" for sec in isins)

    query = f"""
    SELECT 
        co.companyId,
        si.identifierValue as ISIN,
        si.activeFlag,
        ti.tickerSymbol,
        co.companyName,
        geo.country,
        st.subtypevalue,
        st.childlevel
    FROM XF_TCW.XPRESSFEED.ciqSecurityIdentifier si
    JOIN XF_TCW.XPRESSFEED.ciqTradingItem ti 
        ON si.securityId = ti.securityId
        AND ti.primaryflag = 1
    JOIN XF_TCW.XPRESSFEED.ciqSecurity s 
        ON si.securityId = s.securityId
    JOIN XF_TCW.XPRESSFEED.ciqCompany co 
        ON s.companyId = co.companyId
    JOIN XF_TCW.XPRESSFEED.ciqCountryGeo geo 
        ON co.incorporationCountryId = geo.countryId
    JOIN XF_TCW.XPRESSFEED.ciqcompanyindustrytree it 
        ON co.companyid = it.companyid
        AND it.primaryflag = 1
    JOIN XF_TCW.XPRESSFEED.ciqsubtype st 
        ON st.subtypeid = it.subtypeid
    WHERE 1=1
    AND si.identifierTypeId = 8334
    AND si.identifierValue IN ({sec_string})
    AND st.childlevel IN (1, 2, 3)
    ORDER BY 
        ti.tickerSymbol ASC, 
        st.childlevel ASC
    """

    childlevel_dict = {1: "SECTOR", 2: "INDUSTRY_GROUP", 3: "INDUSTRY"}

    df = snowflake_utils.load_from_snowflake(query=query, local_configs=local_configs)
    df["CHILDLEVEL"] = df["CHILDLEVEL"].map(childlevel_dict)
    df = (
        df.pivot(
            index=[
                "COMPANYID",
                "ISIN",
                "TICKERSYMBOL",
                "COMPANYNAME",
                "COUNTRY",
                "ACTIVEFLAG",
            ],
            columns="CHILDLEVEL",
            values="SUBTYPEVALUE",
        )
        .reset_index()
        .set_index("COMPANYID")
    )
    df = df.sort_values("TICKERSYMBOL")
    df.columns.name = None
    df = df.sort_values(["ISIN", "ACTIVEFLAG"])
    df = df.drop_duplicates(["ISIN"], keep="last")
    return df


def get_price_data(
    isins: list, start_date: str = None, end_date: str = None, local_configs: str = ""
) -> pd.DataFrame:
    """
    For a given list of security ISIN's, load pricing data

    Parameters
    ----------
    isins: list
        list of company isins
    start_date: str, optional
        start date in format "yyyy-mm-dd"
    end_date: str, optional
        end date in format "yyyy-mm-dd"
    local_configs: str, optional
        path to a local configarations file

    Returns
    -------
    pd.DataFrame
        DataFrame with columns
            - PRICECLOSE
            - PRICECLOSE_UNADJ
            - VOLUME
    """
    sec_string = ", ".join(f"'{sec}'" for sec in isins)

    start_date_filter = f"AND pe.pricingdate >= '{start_date}'" if start_date else ""
    end_date_filter = f"AND pe.pricingdate <= '{end_date}'" if end_date else ""

    query = f"""
    SELECT 
    pe.pricingdate,
    si.identifierValue as ISIN,
    pe.priceclose,
    pe.adjustmentfactor,
    pe.volume,
    si.activeFlag 
    FROM XF_TCW.XPRESSFEED.ciqSecurityIdentifier si
    JOIN XF_TCW.XPRESSFEED.ciqTradingItem ti on si.securityId = ti.securityId
        AND ti.primaryflag = 1
    JOIN XF_TCW.XPRESSFEED.ciqpriceequity pe on ti.tradingitemid = pe.tradingitemid
    WHERE 1=1
    AND si.identifierTypeId = 8334 -- ISIN identifier type
    AND si.identifierValue IN ({sec_string})
    {start_date_filter}
    {end_date_filter}
    ORDER BY pe.pricingdate DESC
    """

    df = snowflake_utils.load_from_snowflake(query=query, local_configs=local_configs)
    df["PRICINGDATE"] = pd.to_datetime(df["PRICINGDATE"])
    df["PRICECLOSE"] = df["PRICECLOSE"].astype(float)
    df["ADJUSTMENTFACTOR"] = df["ADJUSTMENTFACTOR"].astype(float)
    df["VOLUME"] = df["VOLUME"].astype(float)
    df = df.sort_values(["PRICINGDATE", "ISIN", "ACTIVEFLAG"])
    df = df.drop_duplicates(["PRICINGDATE", "ISIN"], keep="last")
    df["PRICECLOSE_UNADJ"] = df["PRICECLOSE"] * df["ADJUSTMENTFACTOR"]
    df = df.set_index(["PRICINGDATE", "ISIN"])
    df = df.sort_index()
    df = df[["PRICECLOSE", "PRICECLOSE_UNADJ", "VOLUME"]]
    return df
