import os

SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "change-me")

SQLALCHEMY_DATABASE_URI = os.getenv(
    "SQLALCHEMY_DATABASE_URI",
    "postgresql+psycopg2://superset:superset@postgres-superset:5432/superset",
)

WTF_CSRF_ENABLED = True
TALISMAN_ENABLED = False
