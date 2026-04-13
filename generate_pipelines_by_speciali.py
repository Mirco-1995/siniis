#!/usr/bin/env python3
"""
Generate monthly pipeline files with correct number of SPECIALE flows.
Uses pipeline202503.json (2 SPECIALI) or pipeline202510.json (3 SPECIALI) as templates.
"""

import json
from pathlib import Path

def replace_rata_in_value(value, old_rata: str, new_rata: str):
    """Recursively replace rata in strings, lists, and dicts."""
    if isinstance(value, str):
        return value.replace(old_rata, new_rata)
    elif isinstance(value, list):
        return [replace_rata_in_value(item, old_rata, new_rata) for item in value]
    elif isinstance(value, dict):
        return {k: replace_rata_in_value(v, old_rata, new_rata) for k, v in value.items()}
    else:
        return value

def generate_pipeline_for_rata(template_file: str, source_rata: str, target_rata: str, output_file: str):
    """Generate a new pipeline file by replacing rata in template."""

    # Read template
    with open(template_file, 'r', encoding='utf-8') as f:
        pipeline = json.load(f)

    # Replace rata in entire pipeline structure
    pipeline_updated = replace_rata_in_value(pipeline, source_rata, target_rata)

    # Write output
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(pipeline_updated, f, indent=2, ensure_ascii=False)

    return len(pipeline_updated['steps'])

def main():
    """Generate pipeline files based on number of SPECIALI per rata."""

    # Configuration: rata -> number of SPECIALI
    speciali_config = {
        '202501': 3,
        '202502': 3,
        '202503': 2,  # Template, skip
        '202504': 3,
        '202505': 2,
        '202506': 3,
        '202507': 2,
        '202508': 3,
        '202509': 3,
        '202510': 3,  # Template, skip
        '202511': 2,
        '202512': 2,
    }

    template_2_speciali = 'pipeline202503.json'
    template_3_speciali = 'pipeline202510.json'

    print('=' * 70)
    print('Generazione Pipeline con numero corretto di SPECIALI')
    print('=' * 70)
    print(f'\nTemplate 2 SPECIALI: {template_2_speciali} (13 steps)')
    print(f'Template 3 SPECIALI: {template_3_speciali} (16 steps)')
    print()

    results = []

    for rata, num_speciali in speciali_config.items():
        output_file = f'pipeline{rata}.json'

        # Skip templates
        if rata in ['202503', '202510']:
            print(f'[SKIP] {output_file} (template)')
            continue

        # Select template based on number of SPECIALI
        if num_speciali == 2:
            template = template_2_speciali
            source_rata = '202503'
        elif num_speciali == 3:
            template = template_3_speciali
            source_rata = '202510'
        else:
            print(f'[ERROR] {rata}: Invalid number of SPECIALI ({num_speciali})')
            continue

        try:
            steps = generate_pipeline_for_rata(template, source_rata, rata, output_file)
            print(f'[OK] {output_file} - {num_speciali} SPECIALI ({steps} steps)')
            results.append((rata, num_speciali, steps, 'OK'))
        except Exception as e:
            print(f'[ERROR] {output_file}: {e}')
            results.append((rata, num_speciali, 0, f'ERROR: {e}'))

    print()
    print('=' * 70)
    print('Riepilogo')
    print('=' * 70)
    print(f'{"Rata":<10} {"SPECIALI":<10} {"Steps":<10} {"Status":<10}')
    print('-' * 70)

    for rata, num_speciali, steps, status in results:
        print(f'{rata:<10} {num_speciali:<10} {steps:<10} {status:<10}')

    # Add templates to summary
    print(f'202503 (T) {2:<10} {13:<10} {"Template":<10}')
    print(f'202510 (T) {3:<10} {16:<10} {"Template":<10}')

    print()
    print(f'Totale: {len(results)} file generati')
    print('=' * 70)

if __name__ == '__main__':
    main()
