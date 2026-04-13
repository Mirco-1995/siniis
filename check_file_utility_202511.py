#!/usr/bin/env python3
"""Check FILE_UTILITY dependencies in pipeline 202511."""

import json
import sys
sys.path.insert(0, '.')
from verify_all_dependencies import parse_step_info, build_dependencies

with open('pipeline202511.json', 'r', encoding='utf-8') as f:
    pipeline = json.load(f)

deps = build_dependencies(pipeline['steps'])

print('Pipeline 202511 - Tutti gli step FILE_UTILITY:')
print('=' * 70)

for idx, step in enumerate(pipeline['steps']):
    name = step.get('name', '')
    info = parse_step_info(name)
    if info and info['file_type'] == 'FILE_UTILITY':
        print(f'\nStep {idx+1}: {name}')
        if deps[idx]:
            print(f'  Dipendenze:')
            for dep_idx in sorted(deps[idx]):
                dep_name = pipeline['steps'][dep_idx]['name']
                print(f'    - Step {dep_idx+1}: {dep_name}')
        else:
            print(f'  Nessuna dipendenza (può partire subito)')
