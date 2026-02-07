from code1.logger import capture_log_message

'''
This module contains the function to validate if all required approvers have provided their approvals.
'''

def validate_approvers(approvers, required_approvers):
    for required_approver in required_approvers:
        if required_approver not in approvers:
            return False
    return True

if __name__ == '__main__':
    approvers = ['Level1', 'Level2', 'Level3', 'Level2']
    required_approvers = ['Level1', 'Level3', 'Level4']
    # from code1.logger import logger
    if validate_approvers(approvers, required_approvers):
        capture_log_message(log_message="All required approvers have provided their approvals.")
    else:
        capture_log_message(log_message="Not all required approvers have provided their approvals.")
