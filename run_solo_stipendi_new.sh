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
export MONGO_DB="opi-coll"
export MONGO_USER="opiuser"
export MONGO_PASS="opi-password123"
export MONGODB_URI="mongodb://opiuser:opi-password123@c1o-npa-mdbc00.mongo.internal:27017,c1o-npa-mdbc01.mongo.internal:27017,c1o-npa-mdbc02.mongo.internal:27017/opi-coll?replicaSet=rs0&socketTimeoutMS=600000&maxIdleTimeMS=300000"
#export MONGODB_URI="mongodb://opiuser:opi-password123@10.23.196.148:27017/opi-coll?directConnection=true&serverSelectionTimeoutMS=300000&connectTimeoutMS=300000&socketTimeoutMS=0&waitQueueTimeoutMS=900000&maxIdleTimeMS=3600000"
export MONGODB_DB="opi-coll"
export MONGODB_COLLECTION="flussoEMISTI"

#python opirunner.py -c pipeline202501_ordinaria.json --verbose

#python opirunner.py -c pipeline202501.json --verbose
#python opirunner.py -c pipeline202503.json --verbose
#python opirunner.py -c pipeline202505.json --verbose
python opirunner.py -c pipeline202506.json --verbose
#python opirunner.py -c pipeline202507.json --verbose
#python opirunner.py -c pipeline202508.json --verbose
#python opirunner.py -c pipeline202601_solo_emi_speciali_rata202601_new.json --verbose

