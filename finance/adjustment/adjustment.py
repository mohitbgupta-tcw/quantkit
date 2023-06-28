from typing import Union


def planet_people(store, adjustment: str, themes: dict, theme: str):
    """
    Do analyst adjustments for Thematic Type 'Planet' and 'People'
    Add or delete Theme and Sustainability Tag to company

    Parameters
    ----------
    store: comp.CompanyStore | comp.MuniStore | comp. SecuritizedStore | comp.SovereignScore
        company store of company to be adjusted
    adjustment: str
        either 'Deletion' or 'Addition'
    themes: dict
        dictionary of all themes
    theme: str
        theme to be added or deleted to company
    """
    if adjustment == "Deletion":
        store.scores["Sustainability_Tag"] = "X*"
        store.scores["Themes"].pop(theme, None)
    elif adjustment == "Addition":
        store.scores["Sustainability_Tag"] = "Y*"
        store.scores["Themes"][theme] = themes[theme]
        store.information["Primary_Rev_Sustainable"] = themes[theme]
    else:
        raise ValueError("Adjustment value should either be 'Addition' or 'Deletion'")
    return


def Planet(store, adjustment: str, themes: dict, theme: str, **kwargs):
    """
    Do analyst adjustments for Thematic Type 'Planet'
    Add or delete Theme and Sustainability Tag to company

    Parameters
    ----------
    store: comp.CompanyStore | comp.MuniStore | comp. SecuritizedStore | comp.SovereignScore
        company store of company to be adjusted
    adjustment: str
        either 'Deletion' or 'Addition'
    themes: dict
        dictionary of all themes
    theme: str
        theme to be added or deleted to company
    """
    return planet_people(store=store, adjustment=adjustment, themes=themes, theme=theme)


def People(store, adjustment: str, themes: dict, theme: str, **kwargs):
    """
    Do analyst adjustments for Thematic Type 'People'
    Add or delete Theme and Sustainability Tag to company

    Parameters
    ----------
    store: comp.CompanyStore | comp.MuniStore | comp. SecuritizedStore | comp.SovereignScore
        company store of company to be adjusted
    adjustment: str
        either 'Deletion' or 'Addition'
    themes: dict
        dictionary of all themes
    theme: str
        theme to be added or deleted to company
    """
    return planet_people(store=store, adjustment=adjustment, themes=themes, theme=theme)


def Risk(store, adjustment: str, theme: str, comment: str, **kwargs):
    """
    Do analyst adjustments for Thematic Type 'Risk'
    adjust scores (ESRM, Muni, Governance, Transition) for company

    Parameters
    ----------
    store: comp.CompanyStore | comp.MuniStore | comp. SecuritizedStore | comp.SovereignScore
        company store of company to be adjusted
    adjustment: str
        either 'Score_X' or 'No Change'
    theme: str
        theme to be added or deleted to company
    comment: str
        analyst comment
    """
    if adjustment[0:5] == "Score":
        adj = int(adjustment[-1])
        store.scores[theme + "_Score"] = adj
        store.scores["Review_Flag"] = "Analyst Adjustment"

    elif adjustment == "No Change":
        store.scores["Review_Flag"] = "Analyst Adjustment - May Require Action"

    else:
        raise ValueError("Adjustment should either be 'Score_X' or 'No Change'")

    if not store.scores["Review_Comments"] == "":
        store.scores["Review_Comments"] += " & "
    store.scores["Review_Comments"] += str(comment)

    return


def Transition(store, adjustment: str, theme: str, **kwargs):
    """
    Do analyst adjustments for Thematic Type 'Transition'
    Add or delete Transition Category and Transition Tag to company

    Parameters
    ----------
    store: comp.CompanyStore | comp.MuniStore | comp. SecuritizedStore | comp.SovereignScore
        company store of company to be adjusted
    adjustment: str
        either 'Addition' or 'Deletion'
    theme: str
        theme to be added or deleted to company
    """
    if adjustment == "Deletion":
        store.scores["Transition_Tag"] = "X*"
        if theme in store.scores["Transition_Category"]:
            store.scores["Transition_Category"].remove(theme)
        # company_store.company_information["Transition_Category"]
    elif adjustment == "Addition":
        store.scores["Transition_Tag"] = "Y*"
        if not theme in store.scores["Transition_Category"]:
            store.scores["Transition_Category"].append(theme)
    else:
        raise ValueError("Adjustment value should either be 'Addition' or 'Deletion'")
    return
