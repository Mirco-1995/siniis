#!/usr/bin/env python3
"""Test wave execution with layer analysis"""

import json
from pathlib import Path
from dependency_loader import build_dependencies_from_config

# Load pipeline
pipeline_path = Path(__file__).parent / 'pipeline202503.json'
with open(pipeline_path, 'r') as f:
    pipeline = json.load(f)

steps = pipeline['steps']

# Build dependencies
deps = build_dependencies_from_config(steps)

# Calculate waves
completed = set()
wave = 1

print('=== WAVE EXECUTION ANALYSIS ===')
print('Verifica: nessuna wave deve avere 2 step dello stesso layer\n')

while len(completed) < len(steps):
    ready = []
    for idx in range(len(steps)):
        if idx in completed:
            continue
        if all(dep in completed for dep in deps[idx]):
            ready.append(idx)

    if not ready:
        break

    # Collect layers in this wave
    wave_layers = []

    print(f'Wave {wave}:')
    for idx in ready:
        step_name = steps[idx]['name']

        # Extract layer
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

        wave_layers.append(layer)
        print(f'  Step {idx:2d}: [{layer:7s}] {step_name}')
        completed.add(idx)

    # Check for duplicate layers
    unique_layers = set(wave_layers)
    if len(unique_layers) < len(wave_layers):
        print(f'  [X] WARNING: Duplicate layers in wave {wave}!')
        for layer in unique_layers:
            count = wave_layers.count(layer)
            if count > 1:
                print(f'     Layer {layer}: {count} steps')
    else:
        print(f'  [OK] All layers are unique in this wave')

    print()
    wave += 1

print(f'\nTotal waves: {wave - 1}')
print(f'Total steps: {len(steps)}')
