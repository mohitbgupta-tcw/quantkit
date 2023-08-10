import sys, os
import pandas as pd

sys.path.append(os.getcwd())
import quantkit.visualization.risk_framework.esg_characteristics as esg_characteristics


data = pd.read_excel("C:\\Users\\bastit\\Documents\\quantkit\\scores_6739.xlsx")

pdf = esg_characteristics.ESGCharacteristics(
    "Financial Report", data, "fixed_income", 3720, "BLOOMBERG AGGREGATE INDEX"
)

if __name__ == "__main__":
    pdf.run()
    pdf.app.run_server(debug=True)
