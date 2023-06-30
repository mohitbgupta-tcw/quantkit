import quantkit.runner as runner
import pandas as pd


def risk_framework():
    """
    Run risk framework

    In future: enter portfolio id and run framework

    Returns
    -------
    pd.DataFrame

    """
    r = runner.Runner()
    r.init()
    r.run()
    data = []
    for p in r.portfolio_datasource.portfolios:
        portfolio_isin = r.portfolio_datasource.portfolios[p].id
        portfolio_name = r.portfolio_datasource.portfolios[p].name
        for s in r.portfolio_datasource.portfolios[p].holdings:
            sec_store = r.portfolio_datasource.portfolios[p].holdings[s]["object"]
            comp_store = sec_store.parent_store
            issuer_name = sec_store.information["IssuerName"]
            iva_rating = comp_store.information["IVA_COMPANY_RATING"]
            r_flag = comp_store.scores["Review_Flag"]
            r_comments = comp_store.scores["Review_Comments"]
            s2 = comp_store.information["Sector_Level_2"]
            bclass = comp_store.information["BCLASS_Level4"].class_name
            gics = comp_store.information["GICS_SUB_IND"].class_name
            muni_score = comp_store.scores["Muni_Score"]
            sec_score = sec_store.scores["Securitized_Score"]
            sov_score = comp_store.scores["Sovereign_Score"]
            esrm_score = comp_store.scores["ESRM_Score"]
            gov_score = comp_store.scores["Governance_Score"]
            trans_score = comp_store.scores["Transition_Score"]
            level_1 = sec_store.information["SClass_Level1"]
            level_2 = sec_store.information["SClass_Level2"]
            level_3 = sec_store.information["SClass_Level3"]
            level_4 = sec_store.information["SClass_Level4"]
            level_4p = sec_store.information["SClass_Level4-P"]
            risk_score_overall = sec_store.scores["Risk_Score_Overall"]

            if (
                portfolio_isin in r.params["A8Funds"]
                and "Article 8" in comp_store.information["Exclusion"]
                and not (
                    sec_store.information["Labeled_ESG_Type"]
                    in [
                        "Labeled Green",
                        "Labeled Social",
                        "Labeled Sustainable",
                        "Labeled Sustainable Linked",
                    ]
                    and bclass in r.params["carve_out_sectors"]
                )
            ):
                level_1 = "Excluded"
                level_2 = "Exclusion"
                level_3 = "Exclusion"
                level_4 = "Excluded Sector"
                level_4p = "Excluded Sector"
            elif (
                portfolio_isin in r.params["A9Funds"]
                and "Article 9" in comp_store.information["Exclusion"]
                and not (
                    sec_store.information["Labeled_ESG_Type"]
                    in [
                        "Labeled Green",
                        "Labeled Social",
                        "Labeled Sustainable",
                        "Labeled Sustainable Linked",
                    ]
                    and bclass in r.params["carve_out_sectors"]
                )
            ):
                level_1 = "Excluded"
                level_2 = "Exclusion"
                level_3 = "Exclusion"
                level_4 = "Excluded Sector"
                level_4p = "Excluded Sector"

            if "INTELSAT" in issuer_name:
                level_1 = "Preferred"
                level_2 = "Sustainable Theme"
                level_3 = "People"
                level_4 = "INCLUSION"
                level_4p = "INCLUSION"
                gov_score = 4
                esrm_score = 1

            holding_measures = r.portfolio_datasource.portfolios[p].holdings[s][
                "holding_measures"
            ]
            for h in holding_measures:
                portfolio_weight = h["Portfolio_Weight"]
                oas = h["OAS"]
                data.append(
                    (
                        portfolio_isin,
                        portfolio_name,
                        s,
                        issuer_name,
                        iva_rating,
                        r_flag,
                        r_comments,
                        s2,
                        bclass,
                        gics,
                        portfolio_weight,
                        oas,
                        muni_score,
                        sec_score,
                        sov_score,
                        esrm_score,
                        gov_score,
                        risk_score_overall,
                        trans_score,
                        level_1,
                        level_2,
                        level_3,
                        level_4,
                        level_4p,
                    )
                )

    columns = [
        "Portfolio ISIN",
        "Portfolio Name",
        "Security ISIN",
        "Issuer Name",
        "IVA_COMPANY_RATING",
        "Review Flag",
        "Review Comments",
        "Sector Level 2",
        "BCLASS",
        "GICS",
        "Portfolio Weight",
        "OAS",
        "Muni Score",
        "Securitized Score",
        "Sovereign Score",
        "ESRM Score",
        "Governance Score",
        "Risk_Score_Overall",
        "Transition Score",
        "SCLASS_Level1",
        "SCLASS_Level2",
        "SCLASS_Level3",
        "SCLASS_Level4",
        "SCLASS_Level4-P",
    ]
    df = pd.DataFrame(data)
    df.columns = columns
    return df


def sector_subset(gics_list, bclass_list):
    """
    Run risk framework and return scores for specific industries

    In future: enter portfolio id and run framework

    Parameters
    ----------
    gics_list: list
        list of GICS_SUB_IND
    bclass_list: list
        list of BCLASS_LEVEL4

    Returns
    -------
    pd.DataFrame

    """
    r = runner.Runner()
    r.init()
    r.run()
    data = []
    for c in r.companies:
        comp_store = r.companies[c]
        issuer_name = comp_store.msci_information["ISSUER_NAME"]
        r_flag = comp_store.scores["Review_Flag"]
        r_comments = comp_store.scores["Review_Comments"]
        s2 = comp_store.information["Sector_Level_2"]
        bclass = comp_store.information["BCLASS_Level4"].class_name
        gics = comp_store.information["GICS_SUB_IND"].class_name
        esrm_score = comp_store.scores["ESRM_Score"]
        gov_score = comp_store.scores["Governance_Score"]
        trans_score = comp_store.scores["Transition_Score"]
        for s in comp_store.securities:
            sec_store = r.companies[c].securities[s]
            level_1 = sec_store.information["SClass_Level1"]
            level_2 = sec_store.information["SClass_Level2"]
            level_3 = sec_store.information["SClass_Level3"]
            level_4 = sec_store.information["SClass_Level4"]
            level_4p = sec_store.information["SClass_Level4-P"]
            risk_score_overall = sec_store.scores["Risk_Score_Overall"]

            if bclass in bclass_list or gics in gics_list:
                data.append(
                    (
                        s,
                        issuer_name,
                        r_flag,
                        r_comments,
                        s2,
                        bclass,
                        gics,
                        esrm_score,
                        gov_score,
                        risk_score_overall,
                        trans_score,
                        level_1,
                        level_2,
                        level_3,
                        level_4,
                        level_4p,
                    )
                )

    columns = [
        "Security ISIN",
        "Issuer Name",
        "Review Flag",
        "Review Comments",
        "Sector Level 2",
        "BCLASS",
        "GICS",
        "ESRM Score",
        "Governance Score",
        "Risk_Score_Overall",
        "Transition Score",
        "SCLASS_Level1",
        "SCLASS_Level2",
        "SCLASS_Level3",
        "SCLASS_Level4",
        "SCLASS_Level4-P",
    ]
    df = pd.DataFrame(data)
    df.columns = columns
    return df
