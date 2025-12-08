from dotenv import load_dotenv
import os
from pathlib import Path

load_dotenv()


class Settings:
    DATABASE_URL = os.getenv("DATABASE_URL")
    # DATABASE_URL_FOR_ALEMBIC = os.getenv("DATABASE_URL_FOR_ALEMBIC")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = int(
        os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES"))
    REFRESH_TOKEN_EXPIRE_DAYS: int = int(
        os.getenv("REFRESH_TOKEN_EXPIRE_DAYS"))
    JWT_SECRET_KEY: str = os.getenv("JWT_SECRET_KEY")
    JWT_ALGORITHM: str = os.getenv("JWT_ALGORITHM")
    

settings = Settings()