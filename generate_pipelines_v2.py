import json

def create_step(step_number, name, run, env, expect, cwd="/home6/pemco"):
    """Helper per creare uno step con numero"""
    step = {
        "stepNumber": step_number,
        "name": name,
        "run": run,
        "env": env,
        "expect": expect,
        "cwd": cwd
    }
    return step

def create_pipeline_for_months(year, start_month, end_month, filename):
    """Crea un file pipeline per un range di mesi con 3 SPECIALE per mensilità"""

    steps = []
    step_num = 1

    for month in range(start_month, end_month + 1):
        rata = f"{year}{month:02d}"

        # ORDINARIA - FILE_UTILITY
        steps.append(create_step(
            step_num, f"ORDINARIA-{rata} - Transfer FILE_UTILITY",
            f"python3_launcher.sh opi-storage-transfer -r {rata} -t ORDINARIA -s SPT -f FILE_UTILITY",
            {"REMOTE_HOME": f"/home6/pemco/{rata}/ordinaria", "REMOTE_ROOT": "/"},
            {
                "exitCode": 0,
                "preCheck": {"type": "mongodb_files", "rata": rata, "remoteHome": f"/home6/pemco/{rata}/ordinaria"},
                "postCheck": {
                    "type": "mongodb_polling_nonzero", "rata": rata,
                    "mongoHost": "${MONGO_HOST}", "mongoPort": "${MONGO_PORT}", "mongoDb": "${MONGO_DB}",
                    "mongoUser": "${MONGO_USER}", "mongoPass": "${MONGO_PASS}", "mongoUri": "${MONGODB_URI}",
                    "pollIntervalSec": 10, "stabilityTimeoutMin": 1, "maxWaitMin": 1
                }
            }
        ))
        step_num += 1

        # ORDINARIA - FILE_EMISTI
        steps.append(create_step(
            step_num, f"ORDINARIA-{rata} - Transfer FILE_EMISTI",
            f"python3_launcher.sh opi-storage-transfer -r {rata} -t ORDINARIA -s SPT -f FILE_EMISTI",
            {"REMOTE_HOME": f"/home6/pemco/{rata}/ordinaria", "REMOTE_ROOT": "/"},
            {
                "exitCode": 0,
                "preCheck": {"type": "mongodb_emisti", "rata": rata, "remoteHome": f"/home6/pemco/{rata}/ordinaria"},
                "postCheck": {
                    "type": "mongodb_emisti_quality", "rata": rata, "tipoFlusso": "ORDINARIA",
                    "mongoHost": "${MONGO_HOST}", "mongoPort": "${MONGO_PORT}", "mongoDb": "${MONGO_DB}",
                    "mongoUser": "${MONGO_USER}", "mongoPass": "${MONGO_PASS}", "mongoUri": "${MONGODB_URI}",
                    "pollIntervalSec": 30, "stabilityTimeoutMin": 5, "maxWaitMin": 60
                }
            }
        ))
        step_num += 1

        # ORDINARIA - FILE_22000X
        steps.append(create_step(
            step_num, f"ORDINARIA-{rata} - Transfer FILE_22000X",
            f"python3_launcher.sh opi-storage-transfer -r {rata} -t ORDINARIA -s SPT -f FILE_22000X",
            {"REMOTE_HOME": f"/home6/pemco/{rata}/ordinaria", "REMOTE_ROOT": "/"},
            {
                "exitCode": 0,
                "preCheck": {"type": "mongodb_22000x", "rata": rata, "remoteHome": f"/home6/pemco/{rata}/ordinaria"},
                "postCheck": {
                    "type": "mongodb_22000x_quality", "rata": rata, "remoteHome": f"/home6/pemco/{rata}/ordinaria",
                    "tipoFlusso": "ORDINARIA",
                    "mongoHost": "${MONGO_HOST}", "mongoPort": "${MONGO_PORT}", "mongoDb": "${MONGO_DB}",
                    "mongoUser": "${MONGO_USER}", "mongoPass": "${MONGO_PASS}", "mongoUri": "${MONGODB_URI}",
                    "pollIntervalSec": 30, "stabilityTimeoutMin": 5, "maxWaitMin": 60
                }
            }
        ))
        step_num += 1

        # 3 SPECIALI
        for spec_num in range(1, 4):
            # SPECIALE - FILE_UTILITY
            steps.append(create_step(
                step_num, f"SPECIALE{spec_num}-{rata} - Transfer FILE_UTILITY",
                f"python3_launcher.sh opi-storage-transfer -r {rata} -t SPECIALE -s SPT -f FILE_UTILITY",
                {"REMOTE_HOME": f"/home6/pemco/{rata}/speciale{spec_num}", "REMOTE_ROOT": "/"},
                {
                    "exitCode": 0,
                    "preCheck": {"type": "mongodb_files", "rata": rata, "remoteHome": f"/home6/pemco/{rata}/speciale{spec_num}"},
                    "postCheck": {
                        "type": "mongodb_polling_nonzero", "rata": rata,
                        "mongoHost": "${MONGO_HOST}", "mongoPort": "${MONGO_PORT}", "mongoDb": "${MONGO_DB}",
                        "mongoUser": "${MONGO_USER}", "mongoPass": "${MONGO_PASS}", "mongoUri": "${MONGODB_URI}",
                        "pollIntervalSec": 10, "stabilityTimeoutMin": 1, "maxWaitMin": 1
                    }
                }
            ))
            step_num += 1

            # SPECIALE - FILE_EMISTI
            steps.append(create_step(
                step_num, f"SPECIALE{spec_num}-{rata} - Transfer FILE_EMISTI",
                f"python3_launcher.sh opi-storage-transfer -r {rata} -t SPECIALE -s SPT -f FILE_EMISTI",
                {"REMOTE_HOME": f"/home6/pemco/{rata}/speciale{spec_num}", "REMOTE_ROOT": "/"},
                {
                    "exitCode": 0,
                    "preCheck": {"type": "mongodb_emisti_speciale", "rata": rata, "remoteHome": f"/home6/pemco/{rata}/speciale{spec_num}"},
                    "postCheck": {
                        "type": "mongodb_emisti_quality", "rata": rata, "tipoFlusso": "SPECIALE", "progressivoSpeciale": spec_num,
                        "mongoHost": "${MONGO_HOST}", "mongoPort": "${MONGO_PORT}", "mongoDb": "${MONGO_DB}",
                        "mongoUser": "${MONGO_USER}", "mongoPass": "${MONGO_PASS}", "mongoUri": "${MONGODB_URI}",
                        "pollIntervalSec": 30, "stabilityTimeoutMin": 5, "maxWaitMin": 60
                    }
                }
            ))
            step_num += 1

            # SPECIALE - FILE_22000X
            steps.append(create_step(
                step_num, f"SPECIALE{spec_num}-{rata} - Transfer FILE_22000X",
                f"python3_launcher.sh opi-storage-transfer -r {rata} -t SPECIALE -s SPT -f FILE_22000X",
                {"REMOTE_HOME": f"/home6/pemco/{rata}/speciale{spec_num}", "REMOTE_ROOT": "/"},
                {
                    "exitCode": 0,
                    "preCheck": {"type": "mongodb_22000x", "rata": rata, "remoteHome": f"/home6/pemco/{rata}/speciale{spec_num}"},
                    "postCheck": {
                        "type": "mongodb_22000x_quality", "rata": rata, "remoteHome": f"/home6/pemco/{rata}/speciale{spec_num}",
                        "tipoFlusso": "SPECIALE", "progressivoSpeciale": spec_num,
                        "mongoHost": "${MONGO_HOST}", "mongoPort": "${MONGO_PORT}", "mongoDb": "${MONGO_DB}",
                        "mongoUser": "${MONGO_USER}", "mongoPass": "${MONGO_PASS}", "mongoUri": "${MONGODB_URI}",
                        "pollIntervalSec": 30, "stabilityTimeoutMin": 5, "maxWaitMin": 60
                    }
                }
            ))
            step_num += 1

        # SINIIS
        steps.append(create_step(
            step_num, f"SINIIS-{rata} - Caricamento SINIIS",
            "${SINIIS_PYTHON} gestione_siniis.py -d ${SINIIS_REMOTE_DIR_FILES}",
            {"REMOTE_HOME": "/home6/pemco/elaborazioni/files/mese", "REMOTE_ROOT": "/"},
            {"exitCode": 0, "preCheck": {"fileExists": "${SINIIS_FILE_PATH}"}},
            "${SINIIS_CWD}"
        ))
        step_num += 1

        # RITENUTE - FILE_UTILITY
        steps.append(create_step(
            step_num, f"RITENUTE-{rata} - Transfer FILE_UTILITY",
            f"python3_launcher.sh opi-storage-transfer -r {rata} -t RITENUTE -s SPT -f FILE_UTILITY",
            {"REMOTE_HOME": f"/home6/pemco/{rata}/ritenute", "REMOTE_ROOT": "/"},
            {
                "exitCode": 0,
                "preCheck": {"type": "mongodb_files", "rata": rata, "remoteHome": f"/home6/pemco/{rata}/ritenute"},
                "postCheck": {
                    "type": "mongodb_polling_nonzero", "rata": rata,
                    "mongoHost": "${MONGO_HOST}", "mongoPort": "${MONGO_PORT}", "mongoDb": "${MONGO_DB}",
                    "mongoUser": "${MONGO_USER}", "mongoPass": "${MONGO_PASS}", "mongoUri": "${MONGODB_URI}",
                    "pollIntervalSec": 10, "stabilityTimeoutMin": 1, "maxWaitMin": 1
                }
            }
        ))
        step_num += 1

        # RITENUTE - FILE_ANASTI
        steps.append(create_step(
            step_num, f"RITENUTE-{rata} - Transfer FILE_ANASTI",
            f"python3_launcher.sh opi-storage-transfer -r {rata} -t RITENUTE -s SPT -f FILE_ANASTI",
            {"REMOTE_HOME": f"/home6/pemco/{rata}/ritenute", "REMOTE_ROOT": "/"},
            {
                "exitCode": 0,
                "preCheck": {"type": "mongodb_anasti", "rata": rata, "remoteHome": f"/home6/pemco/{rata}/ritenute"},
                "postCheck": {
                    "type": "mongodb_anasti_quality", "rata": rata,
                    "mongoHost": "${MONGO_HOST}", "mongoPort": "${MONGO_PORT}", "mongoDb": "${MONGO_DB}",
                    "mongoUser": "${MONGO_USER}", "mongoPass": "${MONGO_PASS}", "mongoUri": "${MONGODB_URI}",
                    "pollIntervalSec": 30, "stabilityTimeoutMin": 5, "maxWaitMin": 60
                }
            }
        ))
        step_num += 1

        # RITENUTE - FILE_22000X
        steps.append(create_step(
            step_num, f"RITENUTE-{rata} - Transfer FILE_22000X",
            f"python3_launcher.sh opi-storage-transfer -r {rata} -t RITENUTE -s SPT -f FILE_22000X",
            {"REMOTE_HOME": f"/home6/pemco/{rata}/ritenute", "REMOTE_ROOT": "/"},
            {
                "exitCode": 0,
                "preCheck": {"type": "mongodb_22000x", "rata": rata, "remoteHome": f"/home6/pemco/{rata}/ritenute"},
                "postCheck": {
                    "type": "mongodb_22000x_quality", "rata": rata, "remoteHome": f"/home6/pemco/{rata}/ritenute",
                    "tipoFlusso": "RITENUTE",
                    "mongoHost": "${MONGO_HOST}", "mongoPort": "${MONGO_PORT}", "mongoDb": "${MONGO_DB}",
                    "mongoUser": "${MONGO_USER}", "mongoPass": "${MONGO_PASS}", "mongoUri": "${MONGODB_URI}",
                    "pollIntervalSec": 30, "stabilityTimeoutMin": 5, "maxWaitMin": 60
                }
            }
        ))
        step_num += 1

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

    print(f"Creato {filename} con {len(steps)} step per {end_month - start_month + 1} mensilita (step 1-{step_num-1})")

# Crea i due file
create_pipeline_for_months(2025, 1, 6, 'pipelineGennaio_Giugno.json')
create_pipeline_for_months(2025, 7, 12, 'pipelineLuglio_Dicembre.json')

print("\nFile creati con successo!")
