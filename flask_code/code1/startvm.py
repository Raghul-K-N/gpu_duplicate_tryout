'''
Program to start the azure VM
'''
import json
import os
import requests
from code1.logger import capture_log_message
        
subId =  "3949d836-8bb7-4ce3-9f21-a1d0575174bb" #"9ecc50ff-666d-4c69-8aec-b5d70f12c584"
rgrp = "east-us-01" #"qa-ubuntu20"
vmachine = "ml-vm-ubuntu20"
appid = "97987208-1fc0-4130-92d6-08a8417d81b5"


def get_access_token():
        """
        Function to get headers
        """

        authorize_req = "https://login.microsoftonline.com/" + appid + "/oauth2/token"
        payload  = {'grant_type': 'client_credentials','client_id': 'c4b631ae-65f6-461c-81b6-b20787fe54af',
        'client_secret': '.gu-wELX8t4S~IL3epH-63kTJ6xMjTxv79','resource': 'https://management.azure.com/'}

        auth_out = requests.post(authorize_req, payload)
        out      = json.loads(auth_out.text)
        # auth_out = self.post_turnON_auth(authorize_req, payload)
        access_token = "Bearer " + out['access_token']
        headers      = {'Authorization': access_token,'Content-type': 'application/json'}
        return headers

def post_turnON():
    """
    Function to Turn On ML VM
    """

    headers = get_access_token()
    url = "https://management.azure.com/subscriptions/"+ subId +"/resourceGroups/"+rgrp+"/providers/Microsoft.Compute/virtualMachines/" + vmachine + "/start?api-version=2022-08-01"
    req= requests.post(url, headers=headers)
    # from code1.logger import logger
    capture_log_message(log_message="VM Starting now...")

#post_turnON()
