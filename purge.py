import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from models import Base

load_dotenv()
DB = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)
Engine = create_engine(DB)
Base.metadata.drop_all(Engine)
