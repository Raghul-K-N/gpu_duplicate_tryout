from AP_Module.Approval_Matrix.user_approval_matrix import UserApprovalMatrix
from AP_Module.Approval_Matrix.amount_range_validation import compare_amount
from AP_Module.Approval_Matrix.approval_matrix import ApprovalMatrix
from AP_Module.Approval_Matrix.approver_level_validation import validate_approvers
from AP_Module.Approval_Matrix.get_approval_id import get_id_by_amount
# from AP_Module.logger import logger
import AP_Module.Approval_Matrix.main as main_ap
import numpy as np
from code1.logger import capture_log_message
import utils
#check db for rule setting

_USER_MATRIX = 1
_APPROVAL_MATRIX = 2
_MIXED_MATRIX = 3

def UserMatrixOnly(amount, approvers, user_approvals):
    '''
    This function takes an amount, an array of approvers and an array of UserApprovalMatrix instances and returns True if the amount is within the range of min and max value for each approver
    '''
    flag = 0
    for approver in approvers:
        if approver != -1:
            if (compare_amount(amount, UserApprovalMatrix.get_value_by_id(user_approvals,approver,"min"), UserApprovalMatrix.get_value_by_id(user_approvals,approver,"max"))) != True:
                flag = 1
                break
    if flag == 1:
        return 1
    else:
        return 0
    
def ApprovalMatrixOnly(amount, approvers, user_approvals, approval_matrix_array):
    '''
    This function takes an amount, an array of approvers and an array of ApprovalApprovalMatrix instances and returns True if the amount is within the range of min and max value for each approver
    '''
    output = None
    if approval_matrix_array is not None and len(approval_matrix_array)>0:
        approval_id, levels = get_id_by_amount(approval_matrix_array, amount)
        approver_levels = [UserApprovalMatrix.get_value_by_id(user_approvals, approver, 'level') for approver in approvers if approver != 11111]
        output = validate_approvers(approver_levels, levels)
        flag=0
    if not output:
        flag=1
        
        
    return flag
    

#define flow for each matrix
def USER_MATRIX_MODE():

    user_approvals = main_ap.user_approval(mode='USER_MATRIX')
    Approvers_acc_doc = main_ap.approvers_acc_doc()
    if Approvers_acc_doc.shape[0] != 0:
        cols = ['APPROVED_USER_1','APPROVED_USER_2','APPROVED_USER_3','APPROVED_USER_4','APPROVED_USER_5']
        Approvers_acc_doc[cols] = Approvers_acc_doc[cols].fillna(11111).astype(int)
        Approvers_acc_doc['APPROVAL_MATRIX'] = Approvers_acc_doc.apply(lambda row: UserMatrixOnly(row['AMOUNT'], 
                                                                                row[cols].tolist(), user_approvals), axis=1)
        Approvers_acc_doc[cols] = Approvers_acc_doc[cols].replace(11111,np.nan)
    else:
        Approvers_acc_doc['APPROVAL_MATRIX'] = np.nan
    return Approvers_acc_doc

def APPROVAL_MATRIX_MODE():

    user_approvals = main_ap.user_approval(mode='APPROVAL_MATRIX')
    approval_matrix_array = main_ap.approval_matrix()
    Approvers_acc_doc = main_ap.approvers_acc_doc()
    if Approvers_acc_doc.shape[0] != 0:
        cols = ['APPROVED_USER_1','APPROVED_USER_2','APPROVED_USER_3','APPROVED_USER_4','APPROVED_USER_5']
        Approvers_acc_doc[cols] = Approvers_acc_doc[cols].fillna(11111).astype(int)
        Approvers_acc_doc['APPROVAL_MATRIX'] = Approvers_acc_doc.apply(lambda row: ApprovalMatrixOnly(row['AMOUNT'], row[cols].tolist(), 
                                                                                user_approvals, approval_matrix_array), axis=1)
        Approvers_acc_doc[cols] = Approvers_acc_doc[cols].replace(11111,np.nan)
    else:
        Approvers_acc_doc['APPROVAL_MATRIX'] = np.nan
    return Approvers_acc_doc

def MIXED_MATRIX_MODE():

    user_approvals = main_ap.user_approval(mode='MIXED_MATRIX')
    approval_matrix_array = main_ap.approval_matrix()
    Approvers_acc_doc = main_ap.approvers_acc_doc()
    if Approvers_acc_doc.shape[0] != 0:
        cols = ['APPROVED_USER_1','APPROVED_USER_2','APPROVED_USER_3','APPROVED_USER_4','APPROVED_USER_5']
        Approvers_acc_doc[cols] = Approvers_acc_doc[cols].fillna(11111).astype(int)
        Approvers_acc_doc['result_appr'] = Approvers_acc_doc.apply(lambda row: ApprovalMatrixOnly(row['AMOUNT'], 
                                                                        row[cols].tolist(), user_approvals, approval_matrix_array), axis=1)
        Approvers_acc_doc['result_user'] = Approvers_acc_doc.apply(lambda row: UserMatrixOnly(row['AMOUNT'],
                                                                        row[cols].tolist(), user_approvals), axis=1)
        Approvers_acc_doc[cols] = Approvers_acc_doc[cols].replace(11111,np.nan)
        Approvers_acc_doc['APPROVAL_MATRIX'] = np.where((Approvers_acc_doc['result_appr'] == 1) & (Approvers_acc_doc['result_user'] == 1), 1, 0)
    else:
        Approvers_acc_doc['APPROVAL_MATRIX'] = np.nan
    return Approvers_acc_doc

def mode(db_value):
    '''
    Function to call the corresponding matrix mode
    '''
    if db_value == _USER_MATRIX:
        capture_log_message(log_message="USER_MATRIX Mode Enabled")
        result = USER_MATRIX_MODE()
    elif db_value == _APPROVAL_MATRIX:
        result = APPROVAL_MATRIX_MODE()
    elif db_value == _MIXED_MATRIX:
        result = MIXED_MATRIX_MODE()
    
    return result
