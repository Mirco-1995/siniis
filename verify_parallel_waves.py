import json
import re
from typing import Dict, Set, List

def parse_step_info(step_name: str):
    match = re.match(r'^(ORDINARIA|SPECIALE\d+|RITENUTE)(?:-(\d+))?\s*-\s*Transfer\s+(.+)$', step_name)
    if match:
        return {'flow': match.group(1), 'rata': match.group(2), 'file_type': match.group(3)}
    match_siniis = re.match(r'^(SINIIS|RITENUTE)(?:-(\d+))?\s*-\s*Caricamento\s+SINIIS$', step_name)
    if match_siniis:
        return {'flow': 'SINIIS', 'rata': match_siniis.group(2), 'file_type': 'SINIIS'}
    return None

def build_dependencies(steps: List[Dict]) -> Dict[int, Set[int]]:
    dependencies: Dict[int, Set[int]] = {}
    step_map: Dict[str, int] = {}

    for idx, step in enumerate(steps):
        name = step.get('name', '')
        info = parse_step_info(name)
        if info:
            rata = info['rata'] or 'default'
            key = f"{info['flow']}:{rata}:{info['file_type']}"
            step_map[key] = idx

    for idx, step in enumerate(steps):
        dependencies[idx] = set()
        name = step.get('name', '')
        info = parse_step_info(name)

        if not info:
            continue

        flow = info['flow']
        file_type = info['file_type']
        rata = info['rata'] or 'default'

        # Regola 0: SINIIS dipende da tutti i FILE_EMISTI di ORDINARIA e SPECIALE (stessa rata)
        if file_type == 'SINIIS':
            for other_idx, other_step in enumerate(steps):
                other_name = other_step.get('name', '')
                other_info = parse_step_info(other_name)
                if other_info:
                    other_flow = other_info['flow']
                    other_file = other_info['file_type']
                    other_rata = other_info['rata'] or 'default'
                    if other_rata == rata:
                        if other_flow in ['ORDINARIA'] or other_flow.startswith('SPECIALE'):
                            if other_file == 'FILE_EMISTI':
                                dependencies[idx].add(other_idx)

        # FILE_EMISTI dipende da FILE_UTILITY
        elif file_type == 'FILE_EMISTI':
            dep_key = f"{flow}:{rata}:FILE_UTILITY"
            if dep_key in step_map:
                dependencies[idx].add(step_map[dep_key])

        # FILE_22000X e ANASTI
        elif file_type in ['FILE_22000X', 'ANASTI']:
            if file_type == 'ANASTI':
                dep_key = f"{flow}:{rata}:FILE_UTILITY"
                if dep_key in step_map:
                    dependencies[idx].add(step_map[dep_key])
            elif file_type == 'FILE_22000X':
                if flow == 'RITENUTE':
                    dep_key = f"{flow}:{rata}:ANASTI"
                else:
                    dep_key = f"{flow}:{rata}:FILE_EMISTI"
                if dep_key in step_map:
                    dependencies[idx].add(step_map[dep_key])

        # SPECIALE FILE_UTILITY dipende da ORDINARIA FILE_UTILITY
        elif flow.startswith('SPECIALE') and file_type == 'FILE_UTILITY':
            dep_key = f"ORDINARIA:{rata}:FILE_UTILITY"
            if dep_key in step_map:
                dependencies[idx].add(step_map[dep_key])

        # RITENUTE dipende da SINIIS
        if flow == 'RITENUTE' and file_type != 'SINIIS':
            siniis_key = f"SINIIS:{rata}:SINIIS"
            if siniis_key in step_map:
                dependencies[idx].add(step_map[siniis_key])

    return dependencies, step_map

def simulate_parallel_execution_with_constraint(filename: str):
    """Simula esecuzione parallela con constraint: solo 1 FILE_EMISTI per wave"""

    print(f'\n{"="*80}')
    print(f'Simulazione Esecuzione Parallela: {filename}')
    print(f'CONSTRAINT: Solo 1 FILE_EMISTI può eseguire per wave')
    print('='*80)

    with open(filename, 'r', encoding='utf-8') as f:
        pipeline = json.load(f)

    steps = pipeline['steps']
    deps, step_map = build_dependencies(steps)

    completed = set()
    wave = 0

    while len(completed) < len(steps):
        wave += 1

        # Trova step pronti (dipendenze soddisfatte)
        ready = []
        for idx in range(len(steps)):
            if idx in completed:
                continue

            deps_satisfied = deps.get(idx, set()).issubset(completed)
            if deps_satisfied:
                ready.append(idx)

        if not ready:
            print(f"\nERRORE: Nessun step pronto ma {len(steps) - len(completed)} step rimanenti!")
            break

        # Identifica FILE_EMISTI nei ready steps
        emisti_steps = []
        non_emisti_steps = []

        for idx in ready:
            name = steps[idx].get('name', '')
            info = parse_step_info(name)
            if info and info['file_type'] == 'FILE_EMISTI':
                emisti_steps.append(idx)
            else:
                non_emisti_steps.append(idx)

        # Applica constraint: solo 1 FILE_EMISTI per wave
        to_execute = []

        if emisti_steps:
            # Prendi solo il primo FILE_EMISTI
            to_execute.append(emisti_steps[0])
            # Aggiungi tutti i non-EMISTI
            to_execute.extend(non_emisti_steps)
        else:
            # Nessun FILE_EMISTI, esegui tutto
            to_execute = ready

        # Esegui wave
        print(f"\n{'='*60}")
        print(f"Wave {wave}: {len(to_execute)} step(s) in parallelo")
        print(f"{'='*60}")

        for idx in to_execute:
            name = steps[idx].get('name', '')
            step_num = steps[idx].get('stepNumber', idx + 1)
            info = parse_step_info(name)
            file_type = info['file_type'] if info else 'UNKNOWN'

            marker = " [FILE_EMISTI]" if file_type == 'FILE_EMISTI' else ""
            print(f"  Step {step_num:2d}: {name}{marker}")
            completed.add(idx)

        # Mostra FILE_EMISTI rimandati
        if len(emisti_steps) > 1:
            print(f"\n  RIMANDATI alla prossima wave ({len(emisti_steps) - 1} FILE_EMISTI):")
            for idx in emisti_steps[1:]:
                name = steps[idx].get('name', '')
                step_num = steps[idx].get('stepNumber', idx + 1)
                print(f"    Step {step_num:2d}: {name}")

    print(f"\n{'='*80}")
    print(f"Esecuzione completata in {wave} waves")
    print('='*80)

    return wave

# Simula entrambi i pipeline
waves_202503 = simulate_parallel_execution_with_constraint('pipeline202503.json')
waves_202510 = simulate_parallel_execution_with_constraint('pipeline202510.json')

print(f"\n{'='*80}")
print("RIEPILOGO")
print('='*80)
print(f"pipeline202503.json: {waves_202503} waves")
print(f"pipeline202510.json: {waves_202510} waves")
