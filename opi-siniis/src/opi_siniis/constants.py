import os

from dotenv import load_dotenv

load_dotenv()

SINIIS_PG_FILE_PATH = os.getenv("SINIIS_PG_FILE_PATH")

ORACLE_DSN = os.getenv("ORACLE_DSN")
ORACLE_USER = os.getenv("ORACLE_USER")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD")
ORACLE_OWNER = os.getenv("ORACLE_OWNER", "SPTOWNER")

NEGATIVE_SIGNED_MAP = {
    "A": "0",
    "J": "1",
    "K": "2",
    "L": "3",
    "M": "4",
    "N": "5",
    "O": "6",
    "P": "7",
    "Q": "8",
    "R": "9",
}
