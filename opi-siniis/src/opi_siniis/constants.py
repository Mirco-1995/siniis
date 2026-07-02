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
