import quantkit.finance.data_sources.data_sources as ds
import quantkit.utils.logging as logging
import pandas as pd


class ExclusionsDataSource(object):
    """
    Provide exclusions based on Article 8 and Article 9

    Parameters
    ----------
    params: dict
        datasource specific parameters including source

    Returns
    -------
    DataFrame
        As Of Date: pd.datetime
            date of exclusion
        Data Source: str
            data source
        List_Cd: str
            indicatpr showing if exclusion is because of Article 8 or 9
        List Purpose: str
            list purpose
        ID_BB_COMPANY: str
            Bloomberg ID of company
        MSCI Issuer ID: str
            MSCI issuer id
        MSCI Issuer Name: str
            MSCI issuer name
        Field Name: str
            reason for exclusion
        Field Value
            value over threshold
    """

    def __init__(self, params: dict):
        logging.log("Loading Exclusions Data")
        self.params = params
        self.article8 = ExclusionData(params["Article8"])
        self.article9 = ExclusionData(params["Article9"])

        self.df_ = self.transform_df()

    def transform_df(self):
        """
        - Add column to indicate if exclusion is based on Article 8 or 9
        - Concat Article 8 and 9 df's
        """
        self.article8.datasource.df["Article"] = "Article 8"
        self.article9.datasource.df["Article"] = "Article 9"
        df_ = pd.concat(
            [self.article8.datasource.df, self.article9.datasource.df],
            ignore_index=True,
        )
        return df_

    @property
    def df(self):
        """
        Returns
        -------
        DataFrame
            df
        """
        return self.df_


class ExclusionData(ds.DataSources):
    """
    Wrapper to load Article 8 and Article 9 data

    Parameters
    ----------
    params: dict
        datasource specific parameters including datasource
    """

    def __init__(self, params: dict):
        super().__init__(params)

    def transform_df(self):
        """
        None
        """
        return
