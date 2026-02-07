
class AIScoringException(Exception):
    """Custom exception with an error message."""
    def __init__(self, message):
        super().__init__(message)
        self.message = message

class StatScoringException(Exception):
    """Custom exception with an error message."""
    def __init__(self, message):
        super().__init__(message)
        self.message = message

class RulesScoringException(Exception):
    """Custom exception with an error message."""
    def __init__(self, message):
        super().__init__(message)
        self.message = message

class DuplicateScoringException(Exception):
    """Custom exception with an error message."""
    def __init__(self, message):
        super().__init__(message)
        self.message = message
        
class ScoringDataStorageException(Exception):
    """Custom exception with an error message."""
    def __init__(self, message):
        super().__init__(message)
        self.message = message