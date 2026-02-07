data_for_external_api_call = {}

from datetime import datetime
EXPIRY_DATE = datetime(2026, 3, 31).date()

EXTERNAL_CALL_URL_FOR_LICENSE = 'http://4.156.56.67:8000/license/verify'
EXTERNAL_CALL_URL_FOR_LOGS = 'http://4.156.56.67:8000/logs'

SECRETS_TOKEN_URL = "https://iam.cloud.ibm.com/identity/token"
SECRETS_URL = "https://9d2b0ff0-1fd3-4267-9a21-223ba0eb505f.private.eu-de.secrets-manager.appdomain.cloud/api/v2/secrets/"
SECRETS_API_KEY = "Vvwu21CgoinCSvmc2WaUIeWTzyDyimBi7eoccC8No-g8"
APP_USER_SECRET_ID = "1f720c41-3375-259d-2ab0-10ea759febf9"