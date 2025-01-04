import credentials
from sqlalchemy import create_engine
from models import Base

DB = f"postgresql://{credentials.dbuser}:{credentials.dbpass}@{credentials.dbhost}:{credentials.dbport}/{credentials.dbname}"
Engine = create_engine(DB)
Base.metadata.drop_all(Engine)
