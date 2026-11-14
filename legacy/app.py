from flask import Flask, redirect
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

app = Flask(__name__)
load_dotenv()

_required_db_vars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASS']
_missing = [v for v in _required_db_vars if not os.getenv(v)]
if _missing:
    raise RuntimeError(
        f"Missing required environment variable(s): {', '.join(_missing)}. "
        f"Check your .env file (see example.env)."
    )

engine = create_engine(
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    ,
    pool_pre_ping=True,
    pool_recycle=int(os.getenv('DB_POOL_RECYCLE', '1800')),
    # Avoid dumping huge bound-parameter payloads in exception text when a statement fails.
    hide_parameters=True,
)

from api_v1 import api_v1
app.register_blueprint(api_v1)

from explorer import explorer
app.register_blueprint(explorer)

@app.route("/")
def index():
    return redirect("/explorer/")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=12121, debug=True)
