from typing import List, Any
import pdfplumber

def extract_text_lines_from_pdf(file_path: str
                                ) -> List[Any]:
    """
    This function extracts Text lines from PDF
    using PDFPlumber.

    Return Type:
        List[Any]
    """
    all_lines: List = []

    with pdfplumber.open(file_path) as pdf:
        all_pages = pdf.pages

        for page_no,page in enumerate(all_pages):
            lines: List = page.extract_text_lines(layout=True,x_tolerance=1)

            for line in lines:
                mytext: str = str(line.get('text'))
                all_lines.append(mytext)

    return all_lines