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

        # Regola 1: FILE_EMISTI dipende da FILE_UTILITY
        elif file_type == 'FILE_EMISTI':
            dep_key = f"{flow}:{rata}:FILE_UTILITY"
            if dep_key in step_map:
                dependencies[idx].add(step_map[dep_key])

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

        if flow == 'RITENUTE' and file_type != 'SINIIS':
            siniis_key = f"SINIIS:{rata}:SINIIS"
            if siniis_key in step_map:
                dependencies[idx].add(step_map[siniis_key])

    return dependencies, step_map

# Test
with open('pipeline202503.json', 'r', encoding='utf-8') as f:
    pipeline = json.load(f)

deps, step_map = build_dependencies(pipeline['steps'])

print('Grafo delle dipendenze:')
print('=' * 70)

for idx, step in enumerate(pipeline['steps']):
    name = step.get('name', '')
    info = parse_step_info(name)

    if info and (info['flow'] == 'SINIIS' or info['flow'] == 'RITENUTE'):
        print(f"Step {idx}: {name}")
        if deps[idx]:
            print(f"  Dipende da:")
            for dep_idx in sorted(deps[idx]):
                dep_name = pipeline['steps'][dep_idx]['name']
                print(f"    - Step {dep_idx}: {dep_name}")
        else:
            print(f"  Nessuna dipendenza")
        print()

print('\nMappa step (chiavi rilevanti):')
print('=' * 70)
for key, idx in sorted(step_map.items()):
    if 'RITENUTE' in key or 'SINIIS' in key:
        print(f"{key} -> Step {idx}")
