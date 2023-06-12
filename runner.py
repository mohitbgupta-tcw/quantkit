import pandas as pd
import numpy as np
import quantkit.utils.mapping_configs as mapping_configs
import quantkit.utils.configs as configs
import quantkit.finance.data_sources.regions_datasource.regions_datasource as rd
import quantkit.finance.data_sources.category_datasource.category_database as cd
import quantkit.finance.data_sources.security_datasource.security_datasource as sd
import quantkit.finance.data_sources.portfolio_datasource.portfolio_datasource as pod
import quantkit.finance.data_sources.sdg_datasource.sdg_datasource as sdgp
import quantkit.finance.data_sources.sector_datasource.sector_database as secdb
import quantkit.finance.data_sources.exclusions_datasource.exclusions_database as exd
import quantkit.finance.data_sources.themes_datasource.themes_datasource as thd
import quantkit.finance.data_sources.transition_datasource.transition_datasource as trd
import quantkit.finance.data_sources.adjustment_datasource.adjustment_database as ads
import quantkit.finance.data_sources.securitized_datasource.securitized_datasource as securidb
import quantkit.finance.portfolios.portfolios as portfolios
import quantkit.finance.companies.companies as comp
import quantkit.finance.sectors.sectors as sectors
import quantkit.finance.securities.securities as secs
import quantkit.finance.regions.regions as regions
import quantkit.finance.themes.themes as themes
import quantkit.utils.logging as logging
from typing import Union
import operator


class Runner(object):
    def init(self):
        """
        - initialize datsources and load data
        - create reusable attributes
        - itereate over DataFrames and create connected objects
        """

        # read params file
        self.params = configs.read_configs()

        # load regions data
        self.region_datasource = rd.RegionsDataSource(self.params["regions_datasource"])
        self.regions = dict()

        # load portfolio data
        self.portfolio_datasource = pod.PortfolioDataSource(
            self.params["portfolio_datasource"]
        )
        self.portfolios = dict()

        # load category data
        self.category_datasource = cd.CategoryDataSource(
            self.params["category_datasource"]
        )
        self.categories = dict()

        # load security data
        self.security_datasource = sd.SecurityDataSource(
            self.params["security_datasource"]
        )
        self.securities = dict()
        self.equities = dict()
        self.fixed_income = dict()
        self.other_securities = dict()
        self.companies = dict()
        self.munis = dict()
        self.sovereigns = dict()
        self.securitized = dict()
        self.reiter = list()

        # load SDG data
        self.sdg_datasource = sdgp.SDGDataSource(self.params["sdg_datasource"])

        # load sector data
        self.sector_datasource = secdb.SectorDataSource(
            self.params["sector_datasource"]
        )
        self.sectors = dict()

        # load BCLASS data
        self.bclass_datasource = secdb.BClassDataSource(
            self.params["bclass_datasource"]
        )
        self.bclass = dict()

        # load GICS data
        self.gics_datasource = secdb.GICSDataSource(self.params["gics_datasource"])
        self.gics = dict()

        self.industries = dict()

        # load securitized mapping
        self.securitized_datasource = securidb.SecuritizedDataSource(
            self.params["securitized_datasource"]
        )
        self.securitized_mapping = dict()

        # load transition data
        self.transition_datasource = trd.TransitionDataSource(
            self.params["transition_datasource"]
        )

        # load exclusion data
        self.exclusion_datasource = exd.ExclusionsDataSource(
            self.params["exclusion_datasource"]
        )

        # load analyst adjustment data
        self.adjustment_datasource = ads.AdjustmentDataSource(
            self.params["adjustment_datasource"]
        )

        # load themes data
        self.theme_datasource = thd.ThemeDataSource(self.params["theme_datasource"])
        self.themes = dict()

        # iterate over dataframes and create objects
        logging.log("Start Iterating")
        self.iter()

    def iter(self):
        """
        iterate over DataFrames and create connected objects
        """
        self.iter_themes()
        self.iter_regions()
        self.iter_sectors()
        self.iter_securitized_mapping()
        self.iter_portfolios()
        self.iter_securities()
        self.iter_holdings()
        self.iter_companies()
        self.iter_sovereigns()
        self.iter_securitized()
        self.iter_muni()
        return

    def iter_themes(self):
        """
        - create Theme objects for each theme
        - save object for each theme in self.themes
        - key is Acronym
        """
        df_ = self.theme_datasource.datasource.df
        df_unique = df_.drop_duplicates(subset=["Acronym"])
        for index, row in df_unique.iterrows():
            theme = row["Acronym"]
            df_information = df_[df_["Acronym"] == theme]
            self.themes[theme] = themes.Theme(
                acronym=theme,
                name=row["Theme"],
                pillar=row["Pillar"],
                information_df=df_information,
                params=self.params["theme_calculation"],
            )
        return

    def iter_regions(self):
        """
        - create Region objects for each region
        - save object for each region in self.regions
        - key is ISO2
        """
        for index, row in self.region_datasource.df.iterrows():
            r = row["ISO2"]
            self.regions[r] = regions.Region(r, row)
        return

    def iter_sectors(self):
        """
        - create Sector objects
        - create BClass4 objects and attached Industry object
        - map transition targets to sub sectors
        """
        # create Sector objects for GICS and BCLASS
        df_ = self.sector_datasource.df
        for s in list(df_["Sector_Code"].unique()):
            self.sectors[s] = sectors.Sector(s)

        # create Sub-Sector objects for GICS and BCLASS
        self.iter_subsector(
            datasource=self.bclass_datasource,
            col="BCLASS_Level4",
            dic=self.bclass,
            class_=sectors.BClass,
            sector="BCLASS",
        )

        self.iter_subsector(
            datasource=self.gics_datasource,
            col="GICS_SUB_IND",
            dic=self.gics,
            class_=sectors.GICS,
            sector="GICS",
        )

        # map transition target and transition revenue to each sub-sector
        self.iter_transitions()
        return

    def iter_subsector(
        self,
        datasource: Union[secdb.BClassDataSource, secdb.GICSDataSource],
        col: str,
        dic: dict,
        class_: Union[sectors.BClass, sectors.GICS],
        sector: str,
    ):
        """
        Create Sub Sector and Industry objects for specific sector
        Save objects in sub-sector and industry attributes.

        Parameters
        ----------
        datasource: secdb.BClassDataSource | secdb.GICSDataSource
            sector specific datasource with DataFrame
        col: str
            name of column of Sub-Sector in Sub-Sector database
        dic: dict
            Sub-Sector specific dictionary to save object in
        class_: sectors.BClass | sectors.GICS
            Sub-Sector class name
        sector: str
            Sector name
        """
        # create Sub-Sector and Industry objects
        df_ = datasource.df
        for index, row in df_.iterrows():
            # create Sub Sector object
            sub_sector = row[col]
            dic[sub_sector] = class_(class_name=sub_sector, row_information=row)
            ss_object = dic[sub_sector]

            # assign Sub Sector object to Sector and vice verse
            self.sectors[sector].sub_sectors[sub_sector] = ss_object
            ss_object.sector = self.sectors[sector]

            # create Industry object if not already available
            industry = row["Industry"]
            self.industries[industry] = self.industries.get(
                industry,
                sectors.Industry(
                    industry,
                    transition_risk=row["Transition Risk Module"],
                    Q_Low=self.params["transition_parameters"]["Q_Low"],
                    Q_High=self.params["transition_parameters"]["Q_High"],
                ),
            )

            # assign Sub Sector object to Industry and vice verse
            self.industries[industry].sub_sectors[sub_sector] = ss_object
            ss_object.industry = self.industries[industry]

        return

    def iter_transitions(self):
        """
        For each Sub-Sector, assign transition targets and transition revenue

        Revenue_10	>10% Climate Revenue
        Revenue_20	>20% Climate Revenue
        Revenue_30	>30% Climate Revenue
        Revenue_40	>40% Climate Revenue
        Revenue_50	>50% Climate Revenue
        Revenue_60	>60% Climate Revenue
        Target_A	Approved SBTi
        Target_AA	Approved SBTi or Ambitious Target
        Target_AAC	Approved/Committed SBTi or Ambitious Target
        Target_AACN	Approved/Committed SBTi or Ambitious Target or Non-Ambitious Target
        Target_AC	Approved/Committed SBTi
        Target_CA	Committed SBTi or Ambitious Target
        Target_CN	Committed SBTi or Non-Ambitious Target
        Target_N	Non-Ambitious Target
        Target_NRev	Non-Ambitious Target AND >0% Climate Revenue
        """
        for index, row in self.transition_datasource.df.iterrows():
            gics_sub = row["GICS_SUB_IND"]
            bclass4 = row["BCLASS_LEVEL4"]

            if not pd.isna(gics_sub):
                self.gics[gics_sub].transition = row.to_dict()
            if not pd.isna(bclass4):
                self.bclass[bclass4].transition = row.to_dict()
        return

    def iter_portfolios(self):
        """
        - Create Portfolio Objects
        - Save in self.portfolios
        - key is portfolio id
        - attach Sector to Portfolio object
        """
        # iterate over portfolios
        df_ = self.portfolio_datasource.df
        for index, row in (
            df_[["Portfolio", "Portfolio Name"]].drop_duplicates().iterrows()
        ):
            pf = row["Portfolio"]
            pf_store = portfolios.PortfolioStore(pf=pf, name=row["Portfolio Name"])
            holdings_df = df_[df_["Portfolio"] == pf][
                ["As Of Date", "ISIN", "Portfolio_Weight", "Base Mkt Val", "OAS"]
            ]
            pf_store.add_holdings(holdings_df)
            self.portfolios[pf] = pf_store

        # save all companies that occur in portfolios to filter down security database later on
        self.all_holdings = list(df_["ISIN"].unique())

        # attach sector to portfolio
        df_ = self.sector_datasource.df
        for index, row in df_[
            df_["Portfolio"].isin(list(self.portfolios.keys()))
        ].iterrows():
            pf = row["Portfolio"]
            self.portfolios[pf].Sector = self.sectors[row["Sector_Code"]]

        return

    def iter_securities(self):
        """
        - create Company object for each security with key ISIN
        - create Security object with key Security ISIN
        - attach analyst adjustment based on sec isin
        """
        df_ = self.security_datasource.df
        df_portfolio = self.portfolio_datasource.df
        # only iterate over securities the portfolios actually hold so save time
        for index, row in df_[df_["Security ISIN"].isin(self.all_holdings)].iterrows():
            sec_isin = row["Security ISIN"]
            issuer = row.ISIN

            # security does not have a company ISIN:
            # take security isin as company isin and name from portfolio datasource
            if pd.isna(issuer):
                issuer = row["Security ISIN"]
                row["ISIN"] = row["Security ISIN"]
                row["IssuerName"] = df_portfolio[df_portfolio["ISIN"] == sec_isin][
                    "ISSUER_NAME"
                ].values[0]
                row["Ticker"] = df_portfolio[df_portfolio["ISIN"] == sec_isin][
                    "Ticker Cd"
                ].values[0]

            # company object could already be there if company has more than 1 security --> get
            # information for each row is the same, so no need to update
            self.companies[issuer] = self.companies.get(
                issuer,
                comp.CompanyStore(
                    isin=issuer,
                    row_data=row,
                    regions_datasource=self.region_datasource,
                    adjustment_datasource=self.adjustment_datasource,
                    exclusion_datasource=self.exclusion_datasource,
                ),
            )

            # create security store --> seperate Fixed Income and Equity stores based on Security Type
            # if Security Type is NA, just create Security object
            sec_type = row["Security Type"]
            class_ = mapping_configs.security_store_mapping[sec_type]
            type_mapping = mapping_configs.security_mapping[sec_type]
            security_store = class_(isin=sec_isin, parent_store=self.companies[issuer])
            self.securities[sec_isin] = security_store
            getattr(self, type_mapping)[sec_isin] = self.securities[sec_isin]

            # attach security to company
            self.companies[issuer].add_security(sec_isin, self.securities[sec_isin])

            # attach adjustment
            adj_df = self.adjustment_datasource.df[
                self.adjustment_datasource.df["ISIN"] == sec_isin
            ]
            if not adj_df.empty:
                self.companies[issuer].Adjustment = pd.concat(
                    [self.companies[issuer].Adjustment, adj_df],
                    ignore_index=True,
                    sort=False,
                )
        return

    def iter_securitized_mapping(self):
        """
        Iterate over the securitized mapping
        """
        df_ = self.securitized_datasource.df
        for index, row in df_.iterrows():
            collat_type = row["ESG Collat Type"]
            self.securitized_mapping[collat_type] = row.to_dict()
        return

    def iter_holdings(self):
        """
        Iterate over portfolio holdings
        - attach ESG information so security
        - create Muni, Sovereign, Securitized objects
        - attach sector information to company
        - attach BCLASS to company
        - attach MSCI rating to company
        - attach holdings, OAS to self.holdings with security object
        """
        df_ = self.portfolio_datasource.df
        for index, row in df_.iterrows():
            pf = row.Portfolio  # portfolio id
            isin = row.ISIN  # security isin
            # no ISIN for security (cash, swaps, etc.)
            # --> create object with name as identifier
            if isin == "NoISIN":
                if pd.isna(row.ISSUER_NAME):
                    isin = "Cash"
                else:
                    isin = row.ISSUER_NAME
                self.companies[isin] = self.companies.get(
                    isin,
                    comp.CompanyStore(
                        isin,
                        pd.Series(self.companies["NoISIN"].msci_information),
                        regions_datasource=self.region_datasource,
                        adjustment_datasource=self.adjustment_datasource,
                        exclusion_datasource=self.exclusion_datasource,
                    ),
                )
                self.companies[isin].msci_information["IssuerName"] = isin
                self.companies[isin].msci_information["Ticker"] = row["Ticker Cd"]
                self.securities[isin] = self.securities.get(
                    isin, secs.SecurityStore(isin, self.companies[isin])
                )

            security_store = self.securities[isin]
            parent_store = security_store.parent_store

            # attach information to security
            security_store.information[
                "ESG_Collateral_Type"
            ] = self.securitized_mapping[row["ESG Collateral Type"]]
            security_store.information["Labeled_ESG_Type"] = row["Labeled ESG Type"]
            security_store.information["TCW_ESG"] = row["TCW ESG"]
            security_store.information["Issuer_ESG"] = row["Issuer ESG"]
            security_store.information["Issuer_Name"] = row["ISSUER_NAME"]

            # attach information to security's company
            # create new objects for Muni, Sovereign and Securitized
            sector_level2_securitized = ["Residential MBS", "CMBS", "ABS"]
            sector_level2_sovereign = ["Sovereign"]
            sector_level2_muni = ["Muni / Local Authority"]
            issuer_isin = parent_store.isin
            if row["Sector Level 2"] in sector_level2_securitized:
                issuer_isin = parent_store.isin
                self.securitized[issuer_isin] = self.securitized.get(
                    issuer_isin, comp.SecuritizedStore(issuer_isin)
                )
                parent_store.remove_security(isin)
                adj_df = parent_store.Adjustment
                if (not parent_store.securities) and parent_store.type == "company":
                    self.companies.pop(issuer_isin, None)
                parent_store = self.securitized[issuer_isin]
                parent_store.add_security(isin, security_store)
                parent_store.Adjustment = adj_df
                security_store.parent_store = parent_store
            elif row["Sector Level 2"] in sector_level2_muni:
                issuer_isin = parent_store.isin
                self.munis[issuer_isin] = self.munis.get(
                    issuer_isin, comp.MuniStore(issuer_isin)
                )
                parent_store.remove_security(isin)
                adj_df = parent_store.Adjustment
                if (not parent_store.securities) and parent_store.type == "company":
                    self.companies.pop(issuer_isin, None)
                parent_store = self.munis[issuer_isin]
                parent_store.add_security(isin, security_store)
                parent_store.Adjustment = adj_df
                security_store.parent_store = parent_store
            elif row["Sector Level 2"] in sector_level2_sovereign:
                issuer_isin = parent_store.isin
                self.sovereigns[issuer_isin] = self.sovereigns.get(
                    issuer_isin,
                    comp.SovereignStore(
                        issuer_isin,
                        regions_datasource=self.region_datasource,
                        adjustment_datasource=self.adjustment_datasource,
                    ),
                )
                parent_store.remove_security(isin)
                adj_df = parent_store.Adjustment
                msci_info = parent_store.msci_information
                if (not parent_store.securities) and parent_store.type == "company":
                    self.companies.pop(issuer_isin, None)
                parent_store = self.sovereigns[issuer_isin]
                parent_store.msci_information = msci_info
                parent_store.Adjustment = adj_df
                parent_store.add_security(isin, security_store)
                security_store.parent_store = parent_store

            parent_store.information["Sector_Level_1"] = row["Sector Level 1"]
            parent_store.information["Sector_Level_2"] = row["Sector Level 2"]

            # attach BCLASS object
            # if BCLASS is not in BCLASS store (covered industries), attach 'Unassigned BCLASS'
            bclass4 = row["BCLASS_Level4"]
            self.bclass[bclass4] = self.bclass.get(
                bclass4,
                sectors.BClass(
                    bclass4, pd.Series(self.bclass["Unassigned BCLASS"].information)
                ),
            )
            bclass_object = self.bclass[bclass4]

            # for first initialization of BCLASS
            parent_store.information["BCLASS_Level4"] = parent_store.information.get(
                "BCLASS_Level4", bclass_object
            )
            # sometimes same security is labeled with different BCLASS_Level4
            # --> if it was unassigned before: overwrite, else: skipp
            if not (bclass_object.class_name == "Unassigned BCLASS"):
                parent_store.information["BCLASS_Level4"] = bclass_object

            # for first initialization of MSCI Rating
            parent_store.information["Rating_Raw_MSCI"] = parent_store.information.get(
                "Rating_Raw_MSCI", row["Rating Raw MSCI"]
            )

            # sometimes same security is labeled with different MSCI Ratings
            # --> if it's not NA: overwrite, else: skipp
            if not pd.isna(row["Rating Raw MSCI"]):
                parent_store.information["Rating_Raw_MSCI"] = row["Rating Raw MSCI"]

            # attach security object, portfolio weight, OAS to portfolio
            holding_measures = row[
                ["Portfolio_Weight", "Base Mkt Val", "OAS"]
            ].to_dict()
            self.portfolios[pf].holdings[isin] = self.portfolios[pf].holdings.get(
                isin,
                {
                    "object": security_store,
                    "holding_measures": [],
                },
            )
            self.portfolios[pf].holdings[isin]["holding_measures"].append(
                holding_measures
            )

            # attach portfolio to security
            security_store.portfolio_store[pf] = self.portfolios[pf]

        self.companies["NoISIN"].information["BCLASS_Level4"] = self.bclass[
            "Unassigned BCLASS"
        ]
        return

    def iter_sdg(self):
        """
        iterate over SDG data
        - attach sdg information to company in self.sdg_information

        Returns
        -------
        pandas.core.indexes.base.Index
            columns of SDG DataFrame
        """
        df_ = self.sdg_datasource.df
        # only iterate over companies we hold in the portfolios
        for index, row in df_[df_["ISIN"].isin(self.companies.keys())].iterrows():
            isin = row["ISIN"]

            sdg_information = row.to_dict()
            self.companies[isin].sdg_information = sdg_information

        return df_.columns

    def iter_sovereigns(self):
        """
        Iterate over all sovereigns
        - attach region information
        - calculate sovereign score
        - attach analyst adjustment
        - attach GICS information
        """
        for s in self.sovereigns:
            self.sovereigns[s].attach_region(self.regions)
            self.sovereigns[s].update_sovereign_score()
            self.sovereigns[s].attach_analyst_adjustment()
            self.sovereigns[s].attach_gics(self.gics)
            self.sovereigns[s].exclusion()

    def iter_companies(self):
        """
        Iterate over all companies
        - attach sdg information
        - attach region information
        - attach GICS information
        - attach Industry and Sub-Industry information
        - attach analyst adjustment
        - run company specific calculations
        """

        # attach sdg information
        # --> not every company has these information, so create empty df with NA's for those
        cols = self.iter_sdg()
        empty_sdg = pd.Series(np.nan, index=cols).to_dict()

        for c in self.companies:
            self.companies[c].attach_region(self.regions)
            self.companies[c].update_sovereign_score()

            # assign empty sdg information to companies that dont have these information
            if not hasattr(self.companies[c], "sdg_information"):
                self.companies[c].sdg_information = empty_sdg

            # attach exclusion df
            self.companies[c].attach_exclusion()

            # attach exclusion article
            self.companies[c].exclusion()

            # attach GICS Sub industry
            self.companies[c].attach_gics(self.gics)

            # attach industry and sub industry
            self.companies[c].attach_industry(self.gics, self.bclass)

            # attach analyst adjustment
            self.companies[c].attach_analyst_adjustment()

            # calculate capex
            self.companies[c].calculate_capex()

            # calculate climate revenue
            self.companies[c].calculate_climate_revenue()

            # calculate carbon intensite --> if na, reiter and assign industry median
            reiter = self.companies[c].calculate_carbon_intensity()
            if reiter:
                self.reiter.append(c)

            # assign theme and Sustainability_Tag
            self.companies[c].check_theme_requirements(self.themes)

        self.replace_carbon_median()
        self.replace_transition_risk()
        return

    def iter_securitized(self):
        """
        Iterate over all Securitized
        - attach GICS information
        """
        for sec in self.securitized:
            self.securitized[sec].attach_gics(self.gics)
            self.securitized[sec].exclusion()

    def iter_muni(self):
        """
        Iterate over all Munis
        - attach GICS information
        """
        for m in self.munis:
            self.munis[m].attach_gics(self.gics)
            self.munis[m].exclusion()

    def replace_carbon_median(self):
        """
        For companies without 'Carbon Intensity (Scope 123)'
        --> (CARBON_EMISSIONS_SCOPE123 / SALES_USD_RECENT) couldnt be calculuated
        --> replace NA with company's industry median
        """
        for c in self.reiter:
            self.companies[c].information["Carbon Intensity (Scope 123)"] = (
                self.companies[c].information["Industry"].median
            )
        return

    def replace_transition_risk(self):
        """
        Split companies with unassigned industry and sub-industry into
        high and low transition risk
        --> check if carbon intensity is bigger or smaller than predefined threshold
        """
        # create new Industry objects for Unassigned High and Low
        self.industries["Unassigned BCLASS High"] = sectors.Industry(
            "Unassigned BCLASS High",
            transition_risk="High",
            Q_Low=self.params["transition_parameters"]["Q_Low"],
            Q_High=self.params["transition_parameters"]["Q_High"],
        )
        self.industries["Unassigned BCLASS Low"] = sectors.Industry(
            "Unassigned BCLASS Low",
            transition_risk="Low",
            Q_Low=self.params["transition_parameters"]["Q_Low"],
            Q_High=self.params["transition_parameters"]["Q_High"],
        )

        for c in self.industries["Unassigned BCLASS"].companies:
            company_store = self.companies[c]
            carb_int = company_store.information["Carbon Intensity (Scope 123)"]
            # carbon intensity greater than threshold --> high risk
            if carb_int > self.params["transition_parameters"]["High_Threshold"]:
                company_store.information["Transition_Risk_Module"] = "High"
                self.industries["Unassigned BCLASS High"].companies[c] = company_store
                company_store.information["Industry"] = self.industries[
                    "Unassigned BCLASS High"
                ]
                self.industries["Unassigned BCLASS High"].update(
                    company_store.information["Carbon Intensity (Scope 123)"]
                )
            # carbon intensity smaller than threshold --> low risk
            else:
                company_store.information["Transition_Risk_Module"] = "Low"
                self.industries["Unassigned BCLASS Low"].companies[c] = company_store
                company_store.information["Industry"] = self.industries[
                    "Unassigned BCLASS Low"
                ]
                self.industries["Unassigned BCLASS Low"].update(
                    company_store.information["Carbon Intensity (Scope 123)"]
                )

        return

    def calculate_securitized_score(self):
        """
        Calculation of Securitized Score
        """
        for sec in self.securitized:
            self.securitized[sec].calculate_securitized_score()
        return

    def analyst_adjustment(self):
        """
        Do analyst adjustments for each company.
        Different calculations for each thematic type:
            - Risk
            - Transition
            - People
            - Planet
        See quantkit.finance.adjustments for more information
        """
        for c in self.companies:
            self.companies[c].analyst_adjustment(self.themes)

        for sov in self.sovereigns:
            self.sovereigns[sov].analyst_adjustment(self.themes)

        for muni in self.munis:
            self.munis[muni].analyst_adjustment(self.themes)

        for sec in self.securitized:
            self.securitized[sec].analyst_adjustment(self.themes)
        return

    def calculate_esrm_score(self):
        """
        Calculuate esrm score for each company:
        1) For each category save indicator fields and EM and DM flag scorings
        2) For each company:
            2.1) Get ESRM Module (category)
            2.2) Iterate over category indicator fields and
                - count number of flags based on operator and flag threshold
                - save flag value in indicator_Flag
                - create ESRM score based on flag scorings and region
            2.3) Create Governance_Score based on Region_Theme
            2.4) Save flags in company_information
        """
        esrm_d = dict()
        scoring_d = dict()
        operators = {">": operator.gt, "<": operator.lt, "=": operator.eq}

        d_ = self.category_datasource.df
        for cat in d_["Sub-Sector"].unique():
            # save indicator fields, operator, thresholds
            d_ss = d_[d_["Sub-Sector"] == cat]
            esrm_d[cat] = d_ss

            # save flag scorings for EM and DM
            df_ = d_ss.drop_duplicates(subset="Sub-Sector")
            dm = list(
                df_[
                    [
                        "Well-Performing",
                        "Above Average",
                        "Average",
                        "Below Average",
                        "Concerning",
                    ]
                ].values.flatten()
            )
            em = list(
                df_[
                    [
                        "Well-Performing-EM",
                        "Above Average-EM",
                        "Average-EM",
                        "Below Average-EM",
                        "Concerning-EM",
                    ]
                ].values.flatten()
            )
            scoring_d[cat] = [dm, em]

        for c in self.companies:
            self.companies[c].calculate_esrm_score(esrm_d, scoring_d, operators)
        return

    def calculate_transition_score(self):
        """
        Calculate transition score (Transition_Score) for each company:
        0) Check if company is excempted --> set score to 0
        1) Create transition tags
        2) Calculate target score
        3) Calculate transition score
            3.1) Start with industry initial score (3 for low, 5 for high risk)
            3.2) Subtract target score
            3.3) Subtract carbon intensity credit:
                - if in lowest quantile of industry: -2
                - if in medium quantile of industry: -1
        """
        for c in self.companies:
            self.companies[c].calculate_transition_score()
        return

    def calculate_corporate_score(self):
        """
        Calculate corporate score for a company based on other scores.
        Calculation:

            (Governance Score + ESRM Score + Transition Score) / 3
        """
        for c in self.companies:
            self.companies[c].calculate_corporate_score()
        return

    def calculate_risk_overall_score(self):
        """
        Calculate risk overall score on security level:
            - if security specific score between 1 and 2: Leading
            - if security specific score between 2 and 4: Average
            - if security specific score above 4: Poor
            - if security specific score 0: not scored
        """
        for c in self.companies:
            self.companies[c].calculate_risk_overall_score()

        for sov in self.sovereigns:
            self.sovereigns[sov].calculate_risk_overall_score()

        for muni in self.munis:
            self.munis[muni].calculate_risk_overall_score()

        for sec in self.securitized:
            self.securitized[sec].calculate_risk_overall_score()
        return

    def update_sclass(self):
        """
        Set SClass_Level1, SClass_Level2, SClass_Level3, SClass_Level4, SClass_Level4-P
        and SClass_Level5 for each security rule based
        """
        for c in self.companies:
            if not self.companies[c].isin == "NoISIN":
                self.companies[c].update_sclass()

        for sov in self.sovereigns:
            self.sovereigns[sov].update_sclass()

        for muni in self.munis:
            self.munis[muni].update_sclass()

        for sec in self.securitized:
            self.securitized[sec].update_sclass()

    def run(self):
        """
        run calculations
        """
        logging.log("Start Calculations")
        self.calculate_transition_score()
        self.calculate_esrm_score()
        self.calculate_securitized_score()
        self.analyst_adjustment()
        self.calculate_corporate_score()
        self.calculate_risk_overall_score()
        self.update_sclass()
        return
