import os
import json
import pycurl
import certifi
from io import BytesIO
from functools import lru_cache

@lru_cache(maxsize=None)
def get_access_token():
    try:
        buffer = BytesIO()
        curl = pycurl.Curl()
        curl.setopt(curl.URL, os.getenv("SECRETS_TOKEN_URL"))
        curl.setopt(curl.HTTPHEADER, ['Content-Type: application/x-www-form-urlencoded', 'Accept: application/json'])
        curl.setopt(pycurl.SSL_VERIFYPEER, 0)
        curl.setopt(pycurl.SSL_VERIFYHOST, 0)
        curl.setopt(pycurl.POST, 1)
        post_fields = [
            ("grant_type", "urn:ibm:params:oauth:grant-type:apikey"),
            ("apikey",  os.getenv("SECRETS_API_KEY")),
        ]
        from urllib.parse import urlencode
        encoded_post_fields = urlencode(post_fields)
        curl.setopt(pycurl.POSTFIELDS, encoded_post_fields.encode('utf-8'))
        curl.setopt(curl.WRITEDATA, buffer)
        curl.setopt(curl.CAINFO, certifi.where())
        curl.perform()
        curl.close()
        response = json.loads(buffer.getvalue())
        return response["access_token"]
    except Exception as e:
        from code1.logger import logger
        logger.error("Error occurred in getting access token for secret manager:{}".format(e))

def get_credentials(cred_type):
    try:
        secret_key = os.getenv("APP_USER_SECRET_ID") if cred_type == "DB" else os.getenv("APP_EMAIL_NOTIFICATION")
        buffer = BytesIO()
        curl = pycurl.Curl()
        curl.setopt(curl.URL, os.getenv("SECRETS_URL") + secret_key)
        curl.setopt(curl.HTTPHEADER, ['Content-Type: application/json', f'Authorization: Bearer {get_access_token()}'])
        curl.setopt(pycurl.SSL_VERIFYPEER, 0)
        curl.setopt(pycurl.SSL_VERIFYHOST, 0)
        curl.setopt(curl.WRITEDATA, buffer)
        curl.setopt(curl.CAINFO, certifi.where())
        curl.perform()
        curl.close()
        credentials = json.loads(buffer.getvalue())
        return { "username" : credentials["username"], "password": credentials["password"] }
        
    except Exception as e:
        from code1.logger import logger
        logger.error("Error occurred in getting {0} credentials from secret manager:{1}".format(cred_type, e))
        return {}
