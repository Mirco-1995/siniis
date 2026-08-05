from __future__ import annotations

import os
from configparser import ConfigParser
from pathlib import Path

DEFAULT_PROPERTIES_FILE = "opi-siniis.properties"

def load_properties(props_path: str | None = None) -> dict:
    path = Path(props_path) if props_path else Path(DEFAULT_PROPERTIES_FILE)
    config = {}
    if path.exists():
        parser = ConfigParser()
        parser.read(path)
        if parser.has_section("default"):
            config = dict(parser.items("default"))
    return config

ORACLE_DSN = os.getenv("ORACLE_DSN")
ORACLE_USER = os.getenv("ORACLE_USER")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD")
ORACLE_HOME = os.getenv("ORACLE_HOME")
ORACLE_OWNER = os.getenv("ORACLE_OWNER", "SPTOWNER")
SINIIS_PG_PATH = os.getenv("SINIIS_PG_PATH")
RATA_VERSAMENTO = os.getenv("RATA_VERSAMENTO")

COBOL_NUMERIC_MAP = {
    "p": "0",
    "q": "1",
    "r": "2",
    "s": "3",
    "t": "4",
    "u": "5",
    "v": "6",
    "w": "7",
    "x": "8",
    "y": "9",
}
