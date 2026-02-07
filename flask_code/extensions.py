from flask_sqlalchemy import SQLAlchemy
from flask_bcrypt import Bcrypt
from local_database import get_database_credentials
import os
from sqlalchemy import create_engine

credentials = get_database_credentials()
DB_USERNAME = credentials["username"]
DB_PASSWORD = credentials["password"]
mysql_uri = "mysql+pymysql://{0}:{1}@{2}/{3}".format(
   DB_USERNAME, DB_PASSWORD, os.getenv('DB_HOST'), os.getenv('DB_NAME')
)

engine = create_engine(mysql_uri)

db = SQLAlchemy()
bcrypt = Bcrypt()