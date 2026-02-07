'''
Program to stop the ML VM
'''

import json
import os
import requests
# from code1.logger import logger
        
subId =  os.getenv("SUB_ID") #"9ecc50ff-666d-4c69-8aec-b5d70f12c584"
rgrp = os.getenv("RGRP") #"qa-ubuntu20"
vmachine = os.getenv("VMACHINE")
appid = os.getenv("APPID")


def get_access_token():
        """
        Function to get headers
        """

        authorize_req = "https://login.microsoftonline.com/" + appid + "/oauth2/token"
        payload  = {'grant_type': 'client_credentials','client_id': os.getenv("CLIENT_ID"),
        'client_secret': os.getenv("CLIENT_SECRET"),'resource': 'https://management.azure.com/'}

        auth_out = requests.post(authorize_req, payload)
        out      = json.loads(auth_out.text)
        # auth_out = self.post_turnON_auth(authorize_req, payload)
        access_token = "Bearer " + out['access_token']
        headers      = {'Authorization': access_token,'Content-type': 'application/json'}
        return headers

def post_turnOFF():
    """
    Function to Turn off ML VM
    """

    headers = get_access_token()
    url = "https://management.azure.com/subscriptions/"+ subId +"/resourceGroups/"+rgrp+"/providers/Microsoft.Compute/virtualMachines/" + vmachine + "/deallocate?api-version=2022-08-01"
    req= requests.post(url, headers=headers)
    


# post_turnOFF()
