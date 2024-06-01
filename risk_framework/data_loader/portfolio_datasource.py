import quantkit.core.data_loader.portfolio_datasource as portfolio_datasource
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
        - Add custom EM benchmark
        """
        super().transform_df()
        df_em = self.datasource.df[
            self.datasource.df["PORTFOLIO"].isin(
                ["JPM CEMBI BROAD DIVERSE", "JPM EMBI GLOBAL DIVERSIFI"]
            )
        ]
        df_em["PORTFOLIO"] = df_em["PORTFOLIO_NAME"] = "JPM EM Custom Index (50/50)"
        df_em["PORTFOLIO_WEIGHT"] = df_em["PORTFOLIO_WEIGHT"] * 0.5
        self.datasource.df = pd.concat([self.datasource.df, df_em])
