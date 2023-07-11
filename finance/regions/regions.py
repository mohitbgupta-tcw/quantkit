class Region(object):
    """
    Region object. Regions are stored by their ISO2 code, p.e. US for United States.
    Stores information such as:
        - ISO2
        - ISO3
        - Region
        - Region_Theme
        - Sovereign_Score
        - attached companies (as store object) which are based in region

    Parameters
    ----------
    name: str
        ISO2 code of country/ region
    row_data: pd.Series
        Series of information for the region. Safed in self.information
    """

    def __init__(self, name: str, row_data):
        self.region = name
        self.information = row_data.to_dict()
        self.companies = dict()

    def add_company(self, isin: str, company):
        """
        add company object to region

        Parameters
        ----------
        isin: str
            company isin
        company: CompanyStore
            company store
        """
        self.companies[isin] = company
        return
