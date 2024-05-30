import pandas as pd
import quantkit.core.data_sources.pcaf as pcaf


def get_available_versions() -> list:
    """
    Get all available version from PCAF API

    Returns
    -------
    list:
        list of all available version
    """
    api_key = "0dd6a9bda4dbda99988cbe91d233dade"
    url = "https://db.carbonaccountingfinancials.com/PCAF_emission_factor_database.php"

    pcaf_object = pcaf.PCAF(key=api_key, url=url)
    return pcaf_object.metadata["version"]


def run_pcaf_api(asset_class: str, version: str = "latest") -> pd.DataFrame:
    """
    For a specified asset class, load data through PCAF API

    Parameters
    ----------
    asset_class: str
        string of asset class to pull data for, should be in
        ['BL_LE_PF', 'M', 'CRE', 'MV', 'SD', 'CL', 'PMVL']
    version: str, optional
        version to pull

    Returns
    -------
    pd.DataFrame:
        PCAF DataFrame for asset class
    """
    api_key = "0dd6a9bda4dbda99988cbe91d233dade"
    url = "https://db.carbonaccountingfinancials.com/PCAF_emission_factor_database.php"

    asset_class_map = {
        "BL_LE_PF": "1",
        "M": "2",
        "CRE": "3",
        "MV": "4",
        "SD": "5",
        "CL": "9",
        "PMVL": "10",
    }

    asset_class_nr = asset_class_map[asset_class]

    pcaf_object = pcaf.PCAF(key=api_key, url=url, version=version)
    pcaf_object.load(asset_class_nr)
    return pcaf_object.df
