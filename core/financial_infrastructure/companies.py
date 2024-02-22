import pandas as pd
import quantkit.core.financial_infrastructure.asset_base as asset_base


class CompanyStore(asset_base.AssetBase):
    """
    Company object. Stores information such as:
        - isin
        - attached securities (Equity and Bonds)
        - company derived from data providers

    Parameters
    ----------
    isin: str
        company's isin
    row_data: pd.Series
        company information derived from MSCI
    """

    def __init__(self, isin: str, row_data: pd.Series, **kwargs) -> None:
        super().__init__(isin, row_data, **kwargs)
        self.type = "company"

    def get_parent_issuer_data(self, companies: dict) -> None:
        """
        Assign data from parent to sub-company if data is missing (nan)
        Data includes:
            - MSCI
            - SDG
            - Bloomberg

        Parameters
        ----------
        companies: dict
            dictionary of all company objects
        """
        # get parent id from msci
        parent_id = self.msci_information["PARENT_ULTIMATE_ISSUERID"]

        # find parent store
        for c, comp_store in companies.items():
            if comp_store.msci_information["ISSUERID"] == parent_id:
                parent = c

                # assign sdg data for missing values
                if hasattr(self, "sdg_information"):
                    for val in self.sdg_information:
                        if pd.isna(self.sdg_information[val]):
                            new_val = companies[parent].sdg_information[val]
                            self.sdg_information[val] = new_val

                # assign msci data for missing values
                if hasattr(self, "msci_information"):
                    for val in self.msci_information:
                        if pd.isna(self.msci_information[val]):
                            new_val = companies[parent].msci_information[val]
                            self.msci_information[val] = new_val

                # assign bloomberg data for missing values
                if hasattr(self, "bloomberg_information"):
                    for val in self.bloomberg_information:
                        if pd.isna(self.bloomberg_information[val]):
                            new_val = companies[parent].bloomberg_information[val]
                            self.bloomberg_information[val] = new_val

                break

    def iter(
        self,
        companies: dict,
        **kwargs,
    ) -> None:
        """
        - attach data from parent

        Parameters
        ----------
        companies: dict
            dictionary of all company objects
        """
        # attach data from parent if missing
        if not pd.isna(self.msci_information["PARENT_ULTIMATE_ISSUERID"]):
            self.get_parent_issuer_data(companies)
