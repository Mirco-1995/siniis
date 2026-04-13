#!/usr/bin/env python3
"""Timeline visualization for independent layer execution"""

import json
from pathlib import Path
from dependency_loader import build_dependencies_from_config

# Load pipeline
pipeline_path = Path(__file__).parent / 'pipeline202503.json'
with open(pipeline_path, 'r') as f:
    pipeline = json.load(f)

steps = pipeline['steps']
deps = build_dependencies_from_config(steps)

# Calculate waves
completed = set()
wave = 1
timeline = []

while len(completed) < len(steps):
    ready = []
    for idx in range(len(steps)):
        if idx in completed:
            continue
        if all(dep in completed for dep in deps[idx]):
            ready.append(idx)

    if not ready:
        break

    wave_info = {'wave': wave, 'steps': []}
    for idx in ready:
        step_name = steps[idx]['name']

        # Extract flow and layer
        parts = step_name.split(' - ')
        flow = parts[0].strip()

        if 'FILE_UTILITY' in step_name:
            layer = 'UTILITY'
        elif 'FILE_EMISTI' in step_name:
            layer = 'EMISTI'
        elif 'FILE_22000X' in step_name:
            layer = '22000X'
        elif 'ANASTI' in step_name:
            layer = 'ANASTI'
        elif 'SINIIS' in step_name:
            layer = 'SINIIS'
        else:
            layer = 'UNKNOWN'

        wave_info['steps'].append({
            'idx': idx,
            'flow': flow,
            'layer': layer,
            'name': step_name
        })
        completed.add(idx)

    timeline.append(wave_info)
    wave += 1

# Print timeline by layer
print('=== TIMELINE BY LAYER (Independent Execution) ===\n')
print('Legend: Each layer proceeds independently at its own speed\n')

# Track what's executing in each layer
layer_timeline = {
    'UTILITY': [],
    'EMISTI': [],
    '22000X': [],
    'ANASTI': [],
    'SINIIS': []
}

for wave_info in timeline:
    wave_num = wave_info['wave']
    for step in wave_info['steps']:
        layer = step['layer']
        flow = step['flow']
        layer_timeline[layer].append((wave_num, flow))

# Print layer progression
print('LAYER UTILITY:')
for wave, flow in layer_timeline['UTILITY']:
    print(f'  Wave {wave:2d}: {flow}')

print('\nLAYER EMISTI:')
for wave, flow in layer_timeline['EMISTI']:
    print(f'  Wave {wave:2d}: {flow}')

print('\nLAYER 22000X:')
for wave, flow in layer_timeline['22000X']:
    print(f'  Wave {wave:2d}: {flow}')

print('\nLAYER ANASTI:')
for wave, flow in layer_timeline['ANASTI']:
    print(f'  Wave {wave:2d}: {flow}')

print('\nLAYER SINIIS:')
for wave, flow in layer_timeline['SINIIS']:
    print(f'  Wave {wave:2d}: {flow}')

# Show parallel execution
print('\n\n=== PARALLEL EXECUTION DEMONSTRATION ===\n')
print('Wave | UTILITY         | EMISTI          | 22000X          | ANASTI  | SINIIS')
print('-----|-----------------|-----------------|-----------------|---------|-------')

for wave_info in timeline:
    wave_num = wave_info['wave']
    layers_active = {
        'UTILITY': '',
        'EMISTI': '',
        '22000X': '',
        'ANASTI': '',
        'SINIIS': ''
    }

    for step in wave_info['steps']:
        layers_active[step['layer']] = step['flow'][:15]  # Truncate long names

    print(f" {wave_num:2d}  | {layers_active['UTILITY']:15s} | "
          f"{layers_active['EMISTI']:15s} | "
          f"{layers_active['22000X']:15s} | "
          f"{layers_active['ANASTI']:7s} | "
          f"{layers_active['SINIIS']}")

print('\n\nKey insight:')
print('- UTILITY layer completes all flows by Wave 3 (ORDINARIA -> SPECIALE1 -> SPECIALE2)')
print('- EMISTI layer completes all flows by Wave 4')
print('- 22000X layer completes all flows by Wave 5')
print('- Each layer proceeds independently without waiting for other layers!')
