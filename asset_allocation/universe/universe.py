import quantkit.finance.data_sources.portfolio_datasource.portfolio_datasource as portfolio_datasource
import quantkit.utils.snowflake_utils as snowflake_utils


class Universe(portfolio_datasource.PortfolioDataSource):
    def __init__(self, params: dict, **kwargs) -> None:
        super().__init__(params, **kwargs)

    def load(
        self,
    ) -> None:
        super().load(
            start_date=self.params["start_date"],
            end_date=self.params["end_date"],
            equity_benchmark=self.params["equity_universe"],
            fixed_income_benchmark=self.params["fixed_income_universe"],
            pfs=self.params["tcw_universe"],
        )

    def transform_df(self) -> None:
        super().transform_df()

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
