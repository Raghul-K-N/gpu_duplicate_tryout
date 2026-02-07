import AP_Module.Approval_Matrix.approval_matrix_rule
from AP_Module.db_connector import MySQL_DB
from AP_Module.Approval_Matrix.user_approval_matrix import UserApprovalMatrix
from AP_Module.Approval_Matrix.approval_matrix import ApprovalMatrix
# from AP_Module.logger import logger
from AP_Module.Approval_Matrix.approval_matrix_rule import mode
from code1.logger import capture_log_message
import utils

DB = MySQL_DB('AP_Module/DB.json')


def user_approval(mode):
    '''
    Function to get the user_approval_list objects
    '''
    capture_log_message(log_message="user_approval function called")
    user_matrix = DB.get_user_matrix()
    user_approval_list = []
    if mode == 'USER_MATRIX':
        capture_log_message(log_message="Fetching user approval list for USER_MATRIX mode")
        for user in user_matrix:
            user_approval_list.append(UserApprovalMatrix(ID=user['USERID'],minimum=user['MIN_AMOUNT'],
                                                       maximum=user['MAX_AMOUNT']))
        capture_log_message(log_message="Completed fetching user approval list for USER_MATRIX mode")
    elif mode == 'APPROVAL_MATRIX':
        capture_log_message(log_message="Fetching user approval list for APPROVAL_MATRIX mode")
        for user in user_matrix:
            user_approval_list.append(UserApprovalMatrix(ID=user['USERID'],level=user['LEVEL']))
        capture_log_message(log_message="Completed fetching user approval list for APPROVAL_MATRIX mode")
    elif mode == 'MIXED_MATRIX':
        capture_log_message(log_message="Fetching user approval list for MIXED_MATRIX mode")
        for user in user_matrix:
            user_approval_list.append(UserApprovalMatrix(ID=user['USERID'],level=user['LEVEL'],minimum=user['MIN_AMOUNT'],
                                                        maximum=user['MAX_AMOUNT']))
        capture_log_message(log_message="Completed fetching user approval list for MIXED_MATRIX mode")
    return user_approval_list
    
def approvers_acc_doc():
    '''
    Function get the approvers user id list and amount
    '''
    capture_log_message(log_message="Fetching the approvers user id list and amount from approvers_acc_doc function")
    approvers_accdoc = DB.get_approvers_accdoc()
    capture_log_message(log_message="Completed fetching the approvers user id list and amount from approvers_acc_doc function")
    return approvers_accdoc

def approval_matrix():
    '''
    Function to get the approval_list objects for _APPROVAL_MATRIX
    '''
    capture_log_message(log_message="Fetching the approval_list objects for _APPROVAL_MATRIX")
    approval_matrix =DB.get_approval_matrix()
    approval_list = []
    capture_log_message(log_message="Started Iterating the approval_matrix and adding that to ApprovalMatrix Class")
    for value in approval_matrix:
            approval_list.append(ApprovalMatrix(ID=value['ID'],minimum=value['MIN_AMOUNT'], maximum=value['MAX_AMOUNT'],
                                                level_1=value['LEVEL_1'],level_2=value['LEVEL_2'], level_3=value['LEVEL_3'],
                                                  level_4=value['LEVEL_4'], level_5=value['LEVEL_5']))
    return approval_list

def approval_matrix_mode(audit_id):
    '''
    Function to fetch the mode(USER-1,APPROVAL-2,MIXED-3) of the approval matrix rule
    '''
    MODE = DB.get_approval_matrix_mode(audit_id)
    MODE = int(MODE['KEYVALUE'][0])

    return MODE

def process(audit_id):
    db_value = approval_matrix_mode(audit_id)
    capture_log_message(log_message=f"Approval Matrix Mode {db_value}")
    result = mode(db_value)
    result = result[['ACCOUNT_DOC_ID','APPROVAL_MATRIX']]
    return result
