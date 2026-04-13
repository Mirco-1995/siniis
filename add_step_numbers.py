import json
import glob

def add_step_numbers_to_pipeline(filename):
    """Aggiunge stepNumber a ogni step di un file pipeline esistente"""

    with open(filename, 'r', encoding='utf-8') as f:
        pipeline = json.load(f)

    # Controlla se ha una chiave 'steps'
    if 'steps' not in pipeline:
        print(f"Saltato {filename} - non contiene 'steps'")
        return False

    # Controlla se il primo step ha già stepNumber
    if pipeline['steps'] and 'stepNumber' in pipeline['steps'][0]:
        print(f"Saltato {filename} - stepNumber già presente")
        return False

    # Aggiungi stepNumber a ogni step
    for idx, step in enumerate(pipeline['steps'], start=1):
        # Crea un nuovo dizionario con stepNumber come primo campo
        new_step = {'stepNumber': idx}
        new_step.update(step)
        pipeline['steps'][idx - 1] = new_step

    # Salva il file aggiornato
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(pipeline, f, indent=2, ensure_ascii=False)

    print(f"Aggiornato {filename} con {len(pipeline['steps'])} step numerati")
    return True

# Trova tutti i file pipeline (escludendo lo schema)
pipeline_files = glob.glob('pipeline*.json')
pipeline_files = [f for f in pipeline_files if f != 'pipeline_schema.json']

print(f"Trovati {len(pipeline_files)} file pipeline da processare\n")

# Aggiorna tutti i file
updated = 0
for filename in sorted(pipeline_files):
    if add_step_numbers_to_pipeline(filename):
        updated += 1

print(f"\n{updated}/{len(pipeline_files)} file aggiornati con successo!")
