import quantkit.core.data_loader.portfolio_datasource as portfolio_datasource
import quantkit.utils.snowflake_utils as snowflake_utils
import pandas as pd


class PortfolioDataSource(portfolio_datasource.PortfolioDataSource):
    """
    Provide portfolio data

    Parameters
    ----------
    params: dict
        datasource specific parameters including source

    Returns
    -------
    DataFrame
        DATE: datetime.date
            date of portfolio
        PORTFOLIO: str
            portfolio id
        PORTFOLIO_NAME: str
            portfolio name
        SECURITY_KEY: str
            key of security
        PORTFOLIO_WEIGHT: float
            weight of security in portfolio
        BASE_MKT_VAL: float
            market value of position in portfolio
        OAS: float
            OAS
    """

    def transform_df(self) -> None:
        """
        - Transformations for custom universe
        - Transformation for sustainable universe
        """
        super().transform_df()

        if self.params["custom_universe"]:
            for sec in self.params["custom_universe"]:
                sec_dict = {
                    "DATE": pd.bdate_range(
                        start=self.params["start_date"], end=self.params["end_date"]
                    ),
                    "PORTFOLIO": "Custom_Portfolio",
                    "PORTFOLIO_NAME": "Custom_Portfolio",
                    "SECURITY_KEY": sec,
                    "PORTFOLIO_WEIGHT": 1 / len(self.params["custom_universe"]),
                }
                sec_df = pd.DataFrame(data=sec_dict)
                self.datasource.df = pd.concat([self.datasource.df, sec_df])

        if self.params["sustainable"]:
            indices = (
                self.params["equity_universe"]
                + self.params["fixed_income_universe"]
                + self.params["tcw_universe"]
            )
            sust_df = snowflake_utils.load_from_snowflake(
                database="SANDBOX_ESG",
                schema="ESG_SCORES_THEMES",
                table_name="Sustainability_Framework_Detailed",
                local_configs={"API_settings": self.api_settings},
            )
            sust_df = sust_df[sust_df["Portfolio ISIN"].isin(indices)][
                ["Security ISIN", "SCLASS_Level2"]
            ]
            sust_df = sust_df[
                sust_df["SCLASS_Level2"].isin(["Transition", "Sustainable Theme"])
            ]
            sust_universe = list(sust_df["Security ISIN"].unique())
            self.datasource.df = self.datasource.df[
                self.datasource.df["ISIN"].isin(sust_universe)
            ]
