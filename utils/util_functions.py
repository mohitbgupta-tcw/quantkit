import quantkit.data_sources.msci as msci


def divide_chunks(l: list, n: int):
    """
    Divide a list into chunks of size n

    Parameters
    ----------
    l: list
        list to be divided
    n: int
        chunk size
    """
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i : i + n]


def create_msci_mapping(isin_list: list, params: dict):
    """
    Create MSCI mapping DataFrame

    Parameters
    ----------
    isin_list: list
        list of isins
    params: dict
        parameters with msci information

    Returns
    -------
    pd.DataFrame
    """
    msci_params = params["msci_parameters"]
    filters = {
        "issuer_identifier_type": "ISIN",
        "issuer_identifier_list": isin_list,
        "parent_child": "inherit_missing_values",
        "factor_name_list": [
            "ISSUER_NAME",
            "ISSUER_TICKER",
            "ISSUER_CUSIP",
            "ISSUER_SEDOL",
            "ISSUER_ISIN",
            "ISSUER_CNTRY_DOMICILE",
            "ISSUERID",
        ],
    }
    url = "https://api.msci.com/esg/data/v1.0/issuers?category_path_list=ESG+Ratings:Company+Summary&coverage=esg_ratings&format=json"
    msci_object = msci.MSCI(url=url, filters=filters, **msci_params)
    msci_object.load()
    msci_df = msci_object.df
    msci_df = msci_df.rename(columns={"CLIENT_IDENTIFIER": "Client_ID"})
    return msci_df
