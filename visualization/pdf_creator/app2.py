import sys, os

sys.path.append(os.getcwd())
import quantkit.visualization.pdf_creator.visualizor.visualizor as visualzor

pdf = visualzor.Visualizor("Financial Report")

if __name__ == "__main__":
    pdf.app.run_server(debug=True)
