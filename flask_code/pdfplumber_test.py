import pdfplumber

def extract_text_lines_new_version(filepath, space_factor=10):
    """
    Extract text lines from a PDF while preserving visual spacing.
    Reconstructs spaces based on character x-coordinates.

    Args:
        filepath (str): Path to PDF file
        space_factor (int): Controls how many PDF units = 1 space.
                            Smaller = more spaces, larger = fewer.

    Returns:
        list[str]: List of text lines with spacing preserved
    """
    all_lines = []
    with pdfplumber.open(filepath) as pdf:
        for page in pdf.pages:
            # group chars by line (using 'top' position)
            chars = page.chars
            lines_dict = {}
            for c in chars:
                line_key = round(c["top"])  # group by y position
                lines_dict.setdefault(line_key, []).append(c)

            for _, line_chars in sorted(lines_dict.items()):
                # sort characters left → right
                line_chars = sorted(line_chars, key=lambda x: x["x0"])

                line_text = ""
                last_x = 0
                for ch in line_chars:
                    gap = int((ch["x0"] - last_x) / space_factor)
                    line_text += " " * max(0, gap) + ch["text"]
                    last_x = ch["x1"]

                all_lines.append(line_text)

    return all_lines


def extract_text_lines(filepath):
    all_lines = []
    with pdfplumber.open(filepath) as pdf:
        all_pages = pdf.pages
        for page_no,page in enumerate(all_pages):
            lines = page.extract_text_lines(layout=True,x_tolerance=1)
            for line in lines:
                mytext = str(line.get('text'))
                all_lines.append(mytext)
        return all_lines
    

def extract_text_with_spaces_v1(filepath, chars_per_inch=40):
    """
    Extract text while preserving leading spaces based on character positions.
    
    Args:
        filepath: Path to PDF file
        chars_per_inch: Approximate characters per inch (adjust based on font size)
    """
    all_lines = []
    
    with pdfplumber.open(filepath) as pdf:
        for page_no, page in enumerate(pdf.pages):
            # Get page width to calculate relative positions
            page_width = page.width
            
            lines = page.extract_text_lines(layout=True, x_tolerance=1)
            
            for line in lines:
                text = line.get('text', '')
                x0 = line.get('x0', 0)  # Left position of the text
                
                # Calculate approximate number of spaces based on position
                # Adjust this calculation based on your PDF's characteristics
                spaces_count = int(x0 / (page_width / (chars_per_inch * 8.5)))  # Assuming 8.5" width
                leading_spaces = ' ' * spaces_count
                
                formatted_line = leading_spaces + text
                all_lines.append(formatted_line)
                
    return all_lines

filepath  = "/home/whirldata/Downloads/thinkrisk-ocr-shubham-code/gpu-testing/others/XD405826.pdf"


old_res = extract_text_with_spaces_v1(filepath)
new_res = extract_text_lines_new_version(filepath)


# print("Old Result:")
# for line in old_res:
#     print(line)
    
print("\nNew Result:")
for line in new_res:
    print(line)