class ApprovalMatrix:
    def __init__(self, ID, minimum=None, maximum=None, level_1=None, level_2=None, level_3=None, level_4=None, level_5=None):
        self.ID = ID
        self.minimum = minimum
        self.maximum = maximum
        self.level_1 = level_1
        self.level_2 = level_2
        self.level_3 = level_3
        self.level_4 = level_4
        self.level_5 = level_5
    
    def get_value_by_id(self, ID, value_type):
        if self.ID == ID:
            if value_type == 'min':
                return self.minimum
            elif value_type == 'max':
                return self.maximum
            elif value_type in ['level_1', 'level_2', 'level_3', 'level_4', 'level_5']:
                return getattr(self, value_type)
        return None
    
    def get_levels_by_id(matrix, ID):
        """
        Returns a list of level numbers that are true (have a value of 1) for the given ID
        """
        levels = []
        for approval_matrix in matrix:
            if approval_matrix.ID == ID:
                for level in ['level_1', 'level_2', 'level_3', 'level_4', 'level_5']:
                    if getattr(approval_matrix, level) == 1:
                        levels.append(level.replace('level_', ''))
        return levels


if __name__ == '__main__':
    # Create an instance of ApprovalMatrix
    approval_matrix_array = [
        ApprovalMatrix(ID=1, minimum=10, maximum=20, level_1=0,level_2=0, level_3=1, level_4=1, level_5=1),
        ApprovalMatrix(ID=2, minimum=10, maximum=20, level_1=0,level_2=0, level_3=0, level_4=1, level_5=1),
        ]

    # Get an array of all levels of the ApprovalMatrix instance with ID=1
    levels = ApprovalMatrix.get_levels_by_id(approval_matrix_array, 1)