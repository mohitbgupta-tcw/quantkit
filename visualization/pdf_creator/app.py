import sys, os
import pandas as pd

sys.path.append(os.getcwd())
import quantkit.visualization.risk_framework.esg_characteristics as esg_characteristics


# data = pd.read_excel("C:\\Users\\bastit\\Documents\\quantkit\\scores_6739.xlsx")
data_3750 = pd.read_excel("C:\\Users\\bastit\\Documents\\quantkit\\scores_3750.xlsx")

pdf = esg_characteristics.ESGCharacteristics(
    "Financial Report",
    data_3750,
    "em",
    3750,
    "JESG EMBI Global Diversified Index"
    # "Financial Report", data, "equity", 6739, "RUSSELL 1000 VALUE"
)

if __name__ == "__main__":
    pdf.run()
    pdf.app.run_server(debug=True)
