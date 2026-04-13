#!/bin/bash
#
# Script per eseguire pipeline202503.json con opiRunner
# Configurazione RATA 202503 con ORDINARIA, 3 SPECIALE, SINIIS, RITENUTE
#

# ========================================
# 1. CONFIGURAZIONE VARIABILI AMBIENTE
# ========================================

# MongoDB credentials (NON committare questi valori in Git!)
export MONGO_HOST="10.23.196.148"
export MONGO_PORT="27017"
export MONGO_DB="opi-int"
export MONGO_USER="opiuser"
export MONGO_PASS="opi-password123"
export MONGODB_URI="mongodb://opiuser:opi-password123@10.23.196.148:27017/opi-int?directConnection=true&serverSelectionTimeoutMS=300000&connectTimeoutMS=300000&socketTimeoutMS=0&waitQueueTimeoutMS=900000&maxIdleTimeMS=3600000"

#python opirunner.py -c  --verbose
#pipeline202502.json
python opirunner.py -c pipeline202601_solo_emi_speciali.json --verbose
##python opirunner.py -c pipeline202506_no_da_anasti.json --verbose
#python opirunner.py -c pipeline202502.json --verbose
#python opirunner.py -c pipeline202503.json --verbose
#python opirunner.py -c pipeline202505.json --verbose
#python opirunner.py -c pipeline202507.json --verbose
