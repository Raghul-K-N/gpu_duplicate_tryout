from AP_Module.Approval_Matrix.approval_matrix import ApprovalMatrix

def get_id_by_amount(approval_matrix_array, amount):
    """
    Return the ID of the ApprovalMatrix object that matches the given amount.
    """
    for obj in approval_matrix_array:
        if obj.minimum <= amount < obj.maximum:
            return [obj.ID, ApprovalMatrix.get_levels_by_id(approval_matrix_array,obj.ID)]
    return None # If no object matches the given amount, return None


if __name__ == '__main__':
    approval_matrix_array = [ ApprovalMatrix(ID=1, minimum=10, maximum=20, level_1=0,level_2=1, level_3=1, level_4=1, level_5=1),    
                             ApprovalMatrix(ID=2, minimum=0, maximum=10, level_1=0,level_2=0, level_3=0, level_4=1, level_5=1),]

    amount = 15
    id = get_id_by_amount(approval_matrix_array, amount)