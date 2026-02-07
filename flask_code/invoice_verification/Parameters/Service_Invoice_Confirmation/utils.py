from typing import List, Iterable
import re

def check_vim_approval(data_lines: List,
                       sap_invoice_number: str
                       ) -> bool:
    """
    Return True only if:
      - an Approver line matching PATTERN_APPROVER is found, and
      - a Service confirmation line (PATTERN_SERVICE) is found after it.
    Order is preserved: approver must appear before service line.
    """
    # Updated regex: the bracketed ID MUST start with 'U' followed by one or more letters/digits.
    # We capture the ID in group 1 if you want to use it.
    PATTERN_APPROVER = re.compile(
        r"approver/action\s+taken\s+by\s+user\s*:\s*.+\(\s*(u[A-Za-z0-9]+)\s*\)\s*on\s*\d{1,2}/\d{1,2}/\d{2,4}\s+\d{1,2}:\d{2}:\d{2}",
        re.IGNORECASE,
    )

    PATTERN_SERVICE = re.compile(
        r"service\s+confirmation\s*[-–]\s*approved\s+for\s+po\s*\d+\s+line\s+item\s*\d+",
        re.IGNORECASE
    )

    first_idx = None

    for i, line in enumerate(data_lines):
        if PATTERN_APPROVER.search(line):
            first_idx = i
            break

    if first_idx is None:
        return False
    
    invoice_number_present = any(sap_invoice_number.lower() in str(l).strip().lower() for l in data_lines) if data_lines else False

    for line in data_lines[first_idx + 1 :]:
        if PATTERN_SERVICE.search(line):
            return True if invoice_number_present else False

    return False

def get_servie_approval(eml_lines: List, 
                        vim_comment_lines: List,
                        sap_invoice_number: str
                        ) -> bool:
    """
    Check Approval from 
        1.Vim Comments
        2.Email Data

    Note:
        - Either one should have Approval
    """
    # 1. Vim comments check
    vim_approved = check_vim_approval(data_lines=vim_comment_lines,
                                      sap_invoice_number=sap_invoice_number)
    # 2. Email Check
    eml_lines = [line for sublist in eml_lines for line in sublist] if isinstance(eml_lines, Iterable) else eml_lines
    eml_approved_flag = any("approved" in str(l).strip().lower() for l in eml_lines) if eml_lines else False
    eml_invoice_number_present = any(sap_invoice_number.lower() in str(l).strip().lower() for l in eml_lines) if eml_lines else False
    eml_approved = eml_approved_flag and eml_invoice_number_present
    
    if vim_approved or eml_approved:
        return True
    else:
        return False