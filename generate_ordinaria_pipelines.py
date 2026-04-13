#!/usr/bin/env python3
"""
Generate "ordinaria" pipeline files containing only ORDINARIA and SPECIALE flows.
Excludes SINIIS and RITENUTE steps.
"""

import json
from pathlib import Path

def create_ordinaria_pipeline(source_file: str, output_file: str, rata: str):
    """
    Create ordinaria pipeline by filtering only ORDINARIA and SPECIALE steps.

    Args:
        source_file: Source pipeline file (complete pipeline)
        output_file: Output pipeline file (ordinaria only)
        rata: RATA value for verification
    """
    # Read source pipeline
    with open(source_file, 'r', encoding='utf-8') as f:
        pipeline = json.load(f)

    # Filter steps: keep only ORDINARIA and SPECIALE
    ordinaria_steps = []
    step_number = 1

    for step in pipeline['steps']:
        name = step['name']

        # Keep ORDINARIA and SPECIALE steps only
        if name.startswith('ORDINARIA-') or name.startswith('SPECIALE'):
            # Update step number
            step['stepNumber'] = step_number
            ordinaria_steps.append(step)
            step_number += 1

    # Create new pipeline with filtered steps
    ordinaria_pipeline = {
        **pipeline,
        'steps': ordinaria_steps
    }

    # Write output
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(ordinaria_pipeline, f, indent=2, ensure_ascii=False)

    return len(ordinaria_steps)

def main():
    """Generate ordinaria pipelines for specified rates."""

    # Configuration: rata -> number of SPECIALI
    # Exclude 202510, 202511, 202512 as requested
    ordinaria_config = {
        '202501': 3,
        '202502': 3,
        '202503': 2,
        '202504': 3,
        '202505': 2,
        '202506': 3,
        '202507': 2,
        '202508': 3,
        '202509': 3,
    }

    print('=' * 70)
    print('Generazione Pipeline ORDINARIA (solo ORDINARIA + SPECIALI)')
    print('=' * 70)
    print('\nEscluse: 202510, 202511, 202512')
    print()

    results = []

    for rata, num_speciali in ordinaria_config.items():
        source_file = f'pipeline{rata}.json'
        output_file = f'pipeline{rata}_ordinaria.json'

        try:
            steps = create_ordinaria_pipeline(source_file, output_file, rata)
            expected_steps = (num_speciali + 1) * 3  # (ORDINARIA + SPECIALEx) * 3 file types each

            print(f'[OK] {output_file}')
            print(f'     {num_speciali} SPECIALI + ORDINARIA = {steps} steps (atteso: {expected_steps})')

            results.append((rata, num_speciali, steps, 'OK'))

        except FileNotFoundError:
            print(f'[ERROR] {source_file} non trovato')
            results.append((rata, num_speciali, 0, 'File non trovato'))
        except Exception as e:
            print(f'[ERROR] {output_file}: {e}')
            results.append((rata, num_speciali, 0, f'Errore: {e}'))

    print()
    print('=' * 70)
    print('Riepilogo')
    print('=' * 70)
    print(f'{"Rata":<10} {"SPECIALI":<12} {"Steps":<10} {"Status":<15}')
    print('-' * 70)

    for rata, num_speciali, steps, status in results:
        print(f'{rata:<10} {num_speciali:<12} {steps:<10} {status:<15}')

    print()
    print(f'Totale: {len(results)} file ordinaria generati')
    print('=' * 70)

if __name__ == '__main__':
    main()
