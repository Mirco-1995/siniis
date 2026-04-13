#!/usr/bin/env python3
"""Verify all pipelines have correct dependencies based on their SPECIALI count."""

import json
import sys
sys.path.insert(0, '.')
from verify_all_dependencies import parse_step_info, build_dependencies

def main():
    # Configuration
    pipelines_config = {
        '202501': 3,
        '202502': 3,
        '202503': 2,
        '202504': 3,
        '202505': 2,
        '202506': 3,
        '202507': 2,
        '202508': 3,
        '202509': 3,
        '202510': 3,
        '202511': 2,
        '202512': 2,
    }

    print('=' * 70)
    print('Verifica Dipendenze - Tutti i Pipeline')
    print('=' * 70)
    print()

    all_ok = True

    for rata, num_speciali in pipelines_config.items():
        filename = f'pipeline{rata}.json'

        try:
            with open(filename, 'r', encoding='utf-8') as f:
                pipeline = json.load(f)

            deps = build_dependencies(pipeline['steps'])

            # Find SINIIS step
            siniis_idx = next((idx for idx, s in enumerate(pipeline['steps']) if 'SINIIS' in s['name']), None)

            if siniis_idx is None:
                print(f'[ERROR] {rata}: SINIIS step not found!')
                all_ok = False
                continue

            # Count expected FILE_EMISTI dependencies
            expected_emisti = num_speciali + 1  # ORDINARIA + SPECIALEx
            actual_emisti = len(deps[siniis_idx])

            status = 'OK' if actual_emisti == expected_emisti else 'ERROR'
            symbol = '[OK]' if actual_emisti == expected_emisti else '[ERROR]'

            print(f'{symbol} {rata} ({num_speciali} SPECIALI, {len(pipeline["steps"])} steps):')
            print(f'      SINIIS dipende da {actual_emisti} FILE_EMISTI (atteso: {expected_emisti})')

            if actual_emisti != expected_emisti:
                all_ok = False
                print(f'      Dipendenze trovate:')
                for dep_idx in sorted(deps[siniis_idx]):
                    print(f'        - Step {dep_idx+1}: {pipeline["steps"][dep_idx]["name"]}')

        except FileNotFoundError:
            print(f'[SKIP] {rata}: File {filename} not found')
        except Exception as e:
            print(f'[ERROR] {rata}: {e}')
            all_ok = False

    print()
    print('=' * 70)
    if all_ok:
        print('VERIFICA COMPLETATA: Tutte le dipendenze sono corrette!')
    else:
        print('VERIFICA FALLITA: Alcune dipendenze non sono corrette')
    print('=' * 70)

if __name__ == '__main__':
    main()
