import os
from pathlib import Path

try:
    REMOTE_DIR_FILES = Path(os.environ["REMOTE_DIR_FILES"])
except KeyError as exc:
    raise EnvironmentError("Variabile d'ambiente non definita: REMOTE_DIR_FILES") from exc


# Mongo
MONGO_URI = os.getenv("MONGO_URI", "mongodb://opiuser:opi-password123@10.23.196.148:27017/opi-int"
    "?directConnection=true"
    "&serverSelectionTimeoutMS=300000"   # 5 min
    "&connectTimeoutMS=300000"           # 5 min
    "&socketTimeoutMS=0"                 # 0 = disabilita il time-out I/O lato client
    "&waitQueueTimeoutMS=900000"         # 15 min
    "&maxIdleTimeMS=3600000"             # 60 min #"mongodb://opiuser:opi-password123@10.23.196.148:27017/opi-int?directConnection=true")
   )
MONGO_DB = os.getenv("MONGO_DB", "opi-int")

# Oracle
#ORACLE_DSN = os.getenv("ORACLE_DSN", "scan-cloud19coll.osp16cr01.collosp.tesoro.it:1521/OPIINTNOIPDB")
#ORACLE_USER = os.getenv("ORACLE_USER", "opiowner")
#ORACLE_PASS = os.getenv("ORACLE_PASSWORD", "Owneropi")



ORACLE_DSN = os.getenv("ORACLE_DSN","scan-cloud19coll.osp16cr01.collosp.tesoro.it") # "c1v-orc-snpc10.coll.tesoro.it:1521/SPTES.TESORO.IT")
ORACLE_USER = os.getenv("ORACLE_USER", "sptowner")
ORACLE_PASS = os.getenv("ORACLE_PASSWORD", "svilsnpc10$")
ORACLE_OWNER = os.getenv("ORACLE_OWNER", "SPTOWNER")

