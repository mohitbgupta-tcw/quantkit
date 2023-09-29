import quantkit.data_sources.data_sources as ds
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

    def __init__(self, params: dict, **kwargs) -> None:
        self.params = params
        self.article8 = ExclusionData(params["Article8"], **kwargs)
        self.article9 = ExclusionData(params["Article9"], **kwargs)
        self.exclusions = dict()

    def load(self) -> None:
        """
        load data and transform dataframe
        """
        logging.log("Loading Exclusions Data")
        self.article8.load()
        self.article9.load()
        self.transform_df()

    def transform_df(self) -> None:
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
        self.df_ = df_

    def iter(self) -> None:
        """
        Attach exclusion information to dict
        """
        for index, row in self.df.iterrows():
            isin = row["MSCI Issuer ID"]

            exclusion_information = row.to_dict()
            if isin in self.exclusions:
                self.exclusions[isin].append(exclusion_information)
            else:
                self.exclusions[isin] = [exclusion_information]

    @property
    def df(self) -> pd.DataFrame:
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

    def __init__(self, params: dict, **kwargs) -> None:
        super().__init__(params, **kwargs)
        self.table_name = params["table_name"] if "table_name" in params else ""

    def load(self) -> None:
        """
        load data and transform dataframe
        """
        from_table = f"""{self.database}.{self.schema}."{self.table_name}" """
        query = f"""
        SELECT * 
        FROM {from_table}
        """
        self.datasource.load(query=query)
        self.transform_df()

    def transform_df(self) -> None:
        """
        None
        """
        pass
