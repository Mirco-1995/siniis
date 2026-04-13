import json

def create_pipeline_for_months(year, start_month, end_month, filename):
    """Crea un file pipeline per un range di mesi con 3 SPECIALE per mensilità"""

    steps = []
    step_number = 1  # Contatore step

    for month in range(start_month, end_month + 1):
        rata = f"{year}{month:02d}"

        # ORDINARIA - FILE_UTILITY
        steps.append({
            "stepNumber": step_number,
            "name": f"ORDINARIA-{rata} - Transfer FILE_UTILITY",
            "run": f"python3_launcher.sh opi-storage-transfer -r {rata} -t ORDINARIA -s SPT -f FILE_UTILITY",
            "env": {
                "REMOTE_HOME": f"/home6/pemco/{rata}/ordinaria",
                "REMOTE_ROOT": "/"
            },
            "expect": {
                "exitCode": 0,
                "preCheck": {
                    "type": "mongodb_files",
                    "rata": rata,
                    "remoteHome": f"/home6/pemco/{rata}/ordinaria"
                },
                "postCheck": {
                    "type": "mongodb_polling_nonzero",
                    "rata": rata,
                    "mongoHost": "${MONGO_HOST}",
                    "mongoPort": "${MONGO_PORT}",
                    "mongoDb": "${MONGO_DB}",
                    "mongoUser": "${MONGO_USER}",
                    "mongoPass": "${MONGO_PASS}",
                    "mongoUri": "${MONGODB_URI}",
                    "pollIntervalSec": 10,
                    "stabilityTimeoutMin": 1,
                    "maxWaitMin": 1
                }
            },
            "cwd": "/home6/pemco"
        })

        # ORDINARIA - FILE_EMISTI
        steps.append({
            "name": f"ORDINARIA-{rata} - Transfer FILE_EMISTI",
            "run": f"python3_launcher.sh opi-storage-transfer -r {rata} -t ORDINARIA -s SPT -f FILE_EMISTI",
            "env": {
                "REMOTE_HOME": f"/home6/pemco/{rata}/ordinaria",
                "REMOTE_ROOT": "/"
            },
            "expect": {
                "exitCode": 0,
                "preCheck": {
                    "type": "mongodb_emisti",
                    "rata": rata,
                    "remoteHome": f"/home6/pemco/{rata}/ordinaria"
                },
                "postCheck": {
                    "type": "mongodb_emisti_quality",
                    "rata": rata,
                    "tipoFlusso": "ORDINARIA",
                    "mongoHost": "${MONGO_HOST}",
                    "mongoPort": "${MONGO_PORT}",
                    "mongoDb": "${MONGO_DB}",
                    "mongoUser": "${MONGO_USER}",
                    "mongoPass": "${MONGO_PASS}",
                    "mongoUri": "${MONGODB_URI}",
                    "pollIntervalSec": 30,
                    "stabilityTimeoutMin": 5,
                    "maxWaitMin": 60
                }
            },
            "cwd": "/home6/pemco"
        })

        # ORDINARIA - FILE_22000X
        steps.append({
            "name": f"ORDINARIA-{rata} - Transfer FILE_22000X",
            "run": f"python3_launcher.sh opi-storage-transfer -r {rata} -t ORDINARIA -s SPT -f FILE_22000X",
            "env": {
                "REMOTE_HOME": f"/home6/pemco/{rata}/ordinaria",
                "REMOTE_ROOT": "/"
            },
            "expect": {
                "exitCode": 0,
                "preCheck": {
                    "type": "mongodb_22000x",
                    "rata": rata,
                    "remoteHome": f"/home6/pemco/{rata}/ordinaria"
                },
                "postCheck": {
                    "type": "mongodb_22000x_quality",
                    "rata": rata,
                    "remoteHome": f"/home6/pemco/{rata}/ordinaria",
                    "tipoFlusso": "ORDINARIA",
                    "mongoHost": "${MONGO_HOST}",
                    "mongoPort": "${MONGO_PORT}",
                    "mongoDb": "${MONGO_DB}",
                    "mongoUser": "${MONGO_USER}",
                    "mongoPass": "${MONGO_PASS}",
                    "mongoUri": "${MONGODB_URI}",
                    "pollIntervalSec": 30,
                    "stabilityTimeoutMin": 5,
                    "maxWaitMin": 60
                }
            },
            "cwd": "/home6/pemco"
        })

        # 3 SPECIALI per ogni mensilità
        for spec_num in range(1, 4):
            # SPECIALE - FILE_UTILITY
            steps.append({
                "name": f"SPECIALE{spec_num}-{rata} - Transfer FILE_UTILITY",
                "run": f"python3_launcher.sh opi-storage-transfer -r {rata} -t SPECIALE -s SPT -f FILE_UTILITY",
                "env": {
                    "REMOTE_HOME": f"/home6/pemco/{rata}/speciale{spec_num}",
                    "REMOTE_ROOT": "/"
                },
                "expect": {
                    "exitCode": 0,
                    "preCheck": {
                        "type": "mongodb_files",
                        "rata": rata,
                        "remoteHome": f"/home6/pemco/{rata}/speciale{spec_num}"
                    },
                    "postCheck": {
                        "type": "mongodb_polling_nonzero",
                        "rata": rata,
                        "mongoHost": "${MONGO_HOST}",
                        "mongoPort": "${MONGO_PORT}",
                        "mongoDb": "${MONGO_DB}",
                        "mongoUser": "${MONGO_USER}",
                        "mongoPass": "${MONGO_PASS}",
                        "mongoUri": "${MONGODB_URI}",
                        "pollIntervalSec": 10,
                        "stabilityTimeoutMin": 1,
                        "maxWaitMin": 1
                    }
                },
                "cwd": "/home6/pemco"
            })

            # SPECIALE - FILE_EMISTI
            steps.append({
                "name": f"SPECIALE{spec_num}-{rata} - Transfer FILE_EMISTI",
                "run": f"python3_launcher.sh opi-storage-transfer -r {rata} -t SPECIALE -s SPT -f FILE_EMISTI",
                "env": {
                    "REMOTE_HOME": f"/home6/pemco/{rata}/speciale{spec_num}",
                    "REMOTE_ROOT": "/"
                },
                "expect": {
                    "exitCode": 0,
                    "preCheck": {
                        "type": "mongodb_emisti_speciale",
                        "rata": rata,
                        "remoteHome": f"/home6/pemco/{rata}/speciale{spec_num}"
                    },
                    "postCheck": {
                        "type": "mongodb_emisti_quality",
                        "rata": rata,
                        "tipoFlusso": "SPECIALE",
                        "progressivoSpeciale": spec_num,
                        "mongoHost": "${MONGO_HOST}",
                        "mongoPort": "${MONGO_PORT}",
                        "mongoDb": "${MONGO_DB}",
                        "mongoUser": "${MONGO_USER}",
                        "mongoPass": "${MONGO_PASS}",
                        "mongoUri": "${MONGODB_URI}",
                        "pollIntervalSec": 30,
                        "stabilityTimeoutMin": 5,
                        "maxWaitMin": 60
                    }
                },
                "cwd": "/home6/pemco"
            })

            # SPECIALE - FILE_22000X
            steps.append({
                "name": f"SPECIALE{spec_num}-{rata} - Transfer FILE_22000X",
                "run": f"python3_launcher.sh opi-storage-transfer -r {rata} -t SPECIALE -s SPT -f FILE_22000X",
                "env": {
                    "REMOTE_HOME": f"/home6/pemco/{rata}/speciale{spec_num}",
                    "REMOTE_ROOT": "/"
                },
                "expect": {
                    "exitCode": 0,
                    "preCheck": {
                        "type": "mongodb_22000x",
                        "rata": rata,
                        "remoteHome": f"/home6/pemco/{rata}/speciale{spec_num}"
                    },
                    "postCheck": {
                        "type": "mongodb_22000x_quality",
                        "rata": rata,
                        "remoteHome": f"/home6/pemco/{rata}/speciale{spec_num}",
                        "tipoFlusso": "SPECIALE",
                        "progressivoSpeciale": spec_num,
                        "mongoHost": "${MONGO_HOST}",
                        "mongoPort": "${MONGO_PORT}",
                        "mongoDb": "${MONGO_DB}",
                        "mongoUser": "${MONGO_USER}",
                        "mongoPass": "${MONGO_PASS}",
                        "mongoUri": "${MONGODB_URI}",
                        "pollIntervalSec": 30,
                        "stabilityTimeoutMin": 5,
                        "maxWaitMin": 60
                    }
                },
                "cwd": "/home6/pemco"
            })

        # SINIIS per ogni mensilità
        steps.append({
            "name": f"SINIIS-{rata} - Caricamento SINIIS",
            "run": "${SINIIS_PYTHON} gestione_siniis.py -d ${SINIIS_REMOTE_DIR_FILES}",
            "env": {
                "REMOTE_HOME": "/home6/pemco/elaborazioni/files/mese",
                "REMOTE_ROOT": "/"
            },
            "expect": {
                "exitCode": 0,
                "preCheck": {
                    "fileExists": "${SINIIS_FILE_PATH}"
                }
            },
            "cwd": "${SINIIS_CWD}"
        })

        # RITENUTE - FILE_UTILITY
        steps.append({
            "name": f"RITENUTE-{rata} - Transfer FILE_UTILITY",
            "run": f"python3_launcher.sh opi-storage-transfer -r {rata} -t RITENUTE -s SPT -f FILE_UTILITY",
            "env": {
                "REMOTE_HOME": f"/home6/pemco/{rata}/ritenute",
                "REMOTE_ROOT": "/"
            },
            "expect": {
                "exitCode": 0,
                "preCheck": {
                    "type": "mongodb_files",
                    "rata": rata,
                    "remoteHome": f"/home6/pemco/{rata}/ritenute"
                },
                "postCheck": {
                    "type": "mongodb_polling_nonzero",
                    "rata": rata,
                    "mongoHost": "${MONGO_HOST}",
                    "mongoPort": "${MONGO_PORT}",
                    "mongoDb": "${MONGO_DB}",
                    "mongoUser": "${MONGO_USER}",
                    "mongoPass": "${MONGO_PASS}",
                    "mongoUri": "${MONGODB_URI}",
                    "pollIntervalSec": 10,
                    "stabilityTimeoutMin": 1,
                    "maxWaitMin": 1
                }
            },
            "cwd": "/home6/pemco"
        })

        # RITENUTE - FILE_ANASTI
        steps.append({
            "name": f"RITENUTE-{rata} - Transfer FILE_ANASTI",
            "run": f"python3_launcher.sh opi-storage-transfer -r {rata} -t RITENUTE -s SPT -f FILE_ANASTI",
            "env": {
                "REMOTE_HOME": f"/home6/pemco/{rata}/ritenute",
                "REMOTE_ROOT": "/"
            },
            "expect": {
                "exitCode": 0,
                "preCheck": {
                    "type": "mongodb_anasti",
                    "rata": rata,
                    "remoteHome": f"/home6/pemco/{rata}/ritenute"
                },
                "postCheck": {
                    "type": "mongodb_anasti_quality",
                    "rata": rata,
                    "mongoHost": "${MONGO_HOST}",
                    "mongoPort": "${MONGO_PORT}",
                    "mongoDb": "${MONGO_DB}",
                    "mongoUser": "${MONGO_USER}",
                    "mongoPass": "${MONGO_PASS}",
                    "mongoUri": "${MONGODB_URI}",
                    "pollIntervalSec": 30,
                    "stabilityTimeoutMin": 5,
                    "maxWaitMin": 60
                }
            },
            "cwd": "/home6/pemco"
        })

        # RITENUTE - FILE_22000X
        steps.append({
            "name": f"RITENUTE-{rata} - Transfer FILE_22000X",
            "run": f"python3_launcher.sh opi-storage-transfer -r {rata} -t RITENUTE -s SPT -f FILE_22000X",
            "env": {
                "REMOTE_HOME": f"/home6/pemco/{rata}/ritenute",
                "REMOTE_ROOT": "/"
            },
            "expect": {
                "exitCode": 0,
                "preCheck": {
                    "type": "mongodb_22000x",
                    "rata": rata,
                    "remoteHome": f"/home6/pemco/{rata}/ritenute"
                },
                "postCheck": {
                    "type": "mongodb_22000x_quality",
                    "rata": rata,
                    "remoteHome": f"/home6/pemco/{rata}/ritenute",
                    "tipoFlusso": "RITENUTE",
                    "mongoHost": "${MONGO_HOST}",
                    "mongoPort": "${MONGO_PORT}",
                    "mongoDb": "${MONGO_DB}",
                    "mongoUser": "${MONGO_USER}",
                    "mongoPass": "${MONGO_PASS}",
                    "mongoUri": "${MONGODB_URI}",
                    "pollIntervalSec": 30,
                    "stabilityTimeoutMin": 5,
                    "maxWaitMin": 60
                }
            },
            "cwd": "/home6/pemco"
        })

    config = {
        "defaultShell": "sh",
        "defaultTimeout": 5400,
        "defaultExitCode": 0,
        "stopOnFailure": False,
        "variables": {
            "SINIIS_CWD": "/home6/pemco/opi/gestione_siniis",
            "SINIIS_PYTHON": "/home6/pemco/opi/.transf_venv/bin/python",
            "SINIIS_REMOTE_DIR_FILES": "/home6/pemco/elaborazioni/files/mese",
            "SINIIS_FILE_PATH": "/home6/pemco/elaborazioni/files/mese/SINIIS"
        },
        "steps": steps
    }

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

    print(f"Creato {filename} con {len(steps)} step per {end_month - start_month + 1} mensilità")

# Crea i due file
create_pipeline_for_months(2025, 1, 6, 'pipelineGennaio_Giugno.json')
create_pipeline_for_months(2025, 7, 12, 'pipelineLuglio_Dicembre.json')

print("\n✓ File creati con successo!")
