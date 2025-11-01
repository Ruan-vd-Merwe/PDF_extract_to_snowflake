from reportlab.lib.pagesizes import LETTER
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle
from reportlab.lib import colors
import os

OUT_DIR = "/opt/airflow/pdf_data"
FNAME = "sample_table.pdf"

def make_pdf(path):
    doc = SimpleDocTemplate(path, pagesize=LETTER)
    data = [
        ["Name", "Dept", "Amount"],
        ["Alice", "Sales", "1200"],
        ["Bob", "Eng", "950"],
        ["Cara", "HR", "740"],
    ]
    table = Table(data)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.lightgrey),
        ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
        ("ALIGN", (0,0), (-1,-1), "CENTER"),
    ]))
    doc.build([table])

if __name__ == "__main__":
    os.makedirs(OUT_DIR, exist_ok=True)
    make_pdf(os.path.join(OUT_DIR, FNAME))
    print(f"Generated {os.path.join(OUT_DIR, FNAME)}")
