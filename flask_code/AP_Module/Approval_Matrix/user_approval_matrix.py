'''
This class is used to store the user approval matrix data has inbuilt functions to return min, max and level values basded on an id input
'''

class UserApprovalMatrix:
    def __init__(self,  ID, level=None, minimum=None, maximum=None):
        self.ID = ID
        self.level = level
        self.min = minimum
        self.max = maximum
    

    
    @staticmethod
    def get_value_by_id(approvals, ID, value_type):
        for user_approver in approvals:
            if user_approver.ID == ID:
                if value_type == 'min':
                    return user_approver.min
                elif value_type == 'max':
                    return user_approver.max
                elif value_type == 'level':
                    return user_approver.level
        return None


if __name__ == '__main__':
    # Create an array of UserApprovalMatrix instances
    user_approvals = [
        UserApprovalMatrix(1, level='Level1', minimum=10, maximum=20),
        UserApprovalMatrix(2, level='Level2', maximum=15),
        UserApprovalMatrix(3, level='Level3', minimum=8),
        UserApprovalMatrix(4, level='Level4')
    ]

    # Get the min value of the UserApprovalMatrix instance with ID=3
    min_value = UserApprovalMatrix.get_value_by_id(user_approvals, 3, 'min')

    # Get the max value of the UserApprovalMatrix instance with ID=2
    max_value = UserApprovalMatrix.get_value_by_id(user_approvals, 2, 'max')

    # Get the level value of a UserApprovalMatrix instance with ID=4
    non_existent_min_value = UserApprovalMatrix.get_value_by_id(user_approvals, 4, 'level')