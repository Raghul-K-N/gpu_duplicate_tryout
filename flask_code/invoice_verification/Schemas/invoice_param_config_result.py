# Schemas/invoice_param_config_result.py

class InvoiceParamConfigResult:
    """Data class for invoice parameter configuration"""
        
    def __init__(self):
        self.configs = []  # List of parameter configs
    
    def add_param_config(self, param_code: str, validation_method: str, 
                        editable: bool, highlight: bool, supporting_fields: dict):
        """Add a parameter configuration"""
        self.configs.append({
            'param_code': param_code,
            'validation_method': validation_method,
            'editable': 1 if editable else 0,
            'highlight': 1 if highlight else 0,
            'supporting_fields': supporting_fields
        })
    
    def to_db_list(self) -> list:
        """Convert to list of dicts for database insert"""
        return self.configs
