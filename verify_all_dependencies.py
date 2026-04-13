#!/usr/bin/env python3
"""Verify that all monthly pipelines have identical dependency structures."""

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

    return dependencies

def get_dependency_structure(steps: List[Dict]) -> List[tuple]:
    """Get normalized dependency structure (step_type -> [dep_step_types])."""
    dependencies = build_dependencies(steps)
    structure = []

    for idx, step in enumerate(steps):
        name = step.get('name', '')
        info = parse_step_info(name)
        if not info:
            continue

        # Normalize step type (remove rata)
        step_type = f"{info['flow']} - {info['file_type']}"

        # Get dependency types
        dep_types = []
        for dep_idx in sorted(dependencies[idx]):
            dep_name = steps[dep_idx].get('name', '')
            dep_info = parse_step_info(dep_name)
            if dep_info:
                dep_type = f"{dep_info['flow']} - {dep_info['file_type']}"
                dep_types.append(dep_type)

        structure.append((step_type, tuple(dep_types)))

    return structure

def main():
    months = [f'0{i}' if i < 10 else str(i) for i in range(1, 13)]
    rate = [f'2025{m}' for m in months]

    # Skip 202510 as it has different structure (SPECIALE3)
    rate_to_check = [r for r in rate if r != '202510']

    print('=' * 70)
    print('Verifica Dipendenze - Tutti i Pipeline Mensili (13 steps)')
    print('=' * 70)

    structures = {}
    all_same = True
    reference_structure = None
    reference_rata = None

    for rata in rate_to_check:
        filename = f'pipeline{rata}.json'
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                pipeline = json.load(f)

            structure = get_dependency_structure(pipeline['steps'])
            structures[rata] = structure

            if reference_structure is None:
                reference_structure = structure
                reference_rata = rata
            else:
                if structure != reference_structure:
                    all_same = False
                    print(f'\n[ERRORE] {filename} ha dipendenze DIVERSE da {reference_rata}!')

        except FileNotFoundError:
            print(f'[SKIP] {filename} non trovato')
        except Exception as e:
            print(f'[ERROR] {filename}: {e}')

    if all_same:
        print('\n[OK] TUTTE le rate hanno le STESSE IDENTICHE dipendenze!')
        print(f'\nStruttura di riferimento (da {reference_rata}):')
        print('-' * 70)

        with open(f'pipeline{reference_rata}.json', 'r', encoding='utf-8') as f:
            pipeline = json.load(f)

        dependencies = build_dependencies(pipeline['steps'])

        for idx, step in enumerate(pipeline['steps']):
            name = step.get('name', '')
            info = parse_step_info(name)
            if info:
                step_type = f"{info['flow']} - {info['file_type']}"
                print(f'\nStep {idx+1:2d}: {step_type}')
                if dependencies[idx]:
                    for dep_idx in sorted(dependencies[idx]):
                        dep_name = pipeline['steps'][dep_idx].get('name', '')
                        dep_info = parse_step_info(dep_name)
                        if dep_info:
                            dep_type = f"{dep_info['flow']} - {dep_info['file_type']}"
                            print(f'         -> Dipende da Step {dep_idx+1}: {dep_type}')
    else:
        print('\n[ERRORE] Le rate hanno dipendenze DIVERSE!')
        print('\nDettaglio differenze:')
        for rata, structure in structures.items():
            if structure != reference_structure:
                print(f'\n{rata} differisce da {reference_rata}:')
                for i, (step_type, deps) in enumerate(structure):
                    ref_deps = reference_structure[i][1] if i < len(reference_structure) else ()
                    if deps != ref_deps:
                        print(f'  Step {i+1} ({step_type}):')
                        print(f'    Atteso: {ref_deps}')
                        print(f'    Trovato: {deps}')

    print('\n' + '=' * 70)
    print(f'Verificate {len(structures)} rate (escluso 202510 con SPECIALE3)')
    print('=' * 70)

if __name__ == '__main__':
    main()
