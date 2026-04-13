
"""Costanti base (MinIO rimosso; si lavora solo con Mongo + Oracle)."""
import os

# Mongo
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:Password1@192.168.200.34:32117/")
MONGO_DB = os.getenv("MONGO_DB", "opi")

# Oracle
ORACLE_DSN = os.getenv("ORACLE_DSN", "192.168.200.31:1521/pdb_opi")
ORACLE_USER = os.getenv("ORACLE_USER", "opiowner")
ORACLE_PASS = os.getenv("ORACLE_PASS", "Pa$$w0rdOPI")
ORACLE_OWNER = os.getenv("ORACLE_OWNER", "OPIOWNER")
ORACLE_SEQ_ORIGINE = os.getenv("ORACLE_SEQ_ORIGINE", "OPIOWNER.SEQ_OPI_DISP_VARIAZIONI_ORIGINE")
