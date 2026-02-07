from code1.logger import capture_log_message


'''
This function take an amount, min value and max value and returns True if the amount is within the range of min and max value
'''

def compare_amount(amount, min_value, max_value):
    if max_value < min_value:
        # return None or raise an exception, depending on what's appropriate for your use case
        return None 
    else:
        return min_value <= amount <= max_value
    

if __name__ == '__main__':
    capture_log_message(log_message=compare_amount(10, 5, 15))
    #True
    capture_log_message(log_message=compare_amount(20, 5, 15))
    #False
    capture_log_message(log_message=compare_amount(10, 15, 5))
    #True
