# Schemas/invoice_attachments_result.py

class InvoiceAttachmentsResult:
    """Data class for invoice attachments"""
    
    def __init__(self):
        self.attachments = []
    
    def add_attachment(self, file_path: str, file_type: str|None = None):
        """Add an attachment"""
        if file_path:  # Only add if path exists
            self.attachments.append({
                'file_path': file_path,
                'file_type': file_type
            })
    
    def set_attachment_paths(self, file_path_lst: list):
        """Set the list of attachment file paths"""
        for path in file_path_lst:
            self.add_attachment(file_path=path)

    def to_db_list(self) -> list:
        """Convert to list of dicts for database insert"""
        return self.attachments
