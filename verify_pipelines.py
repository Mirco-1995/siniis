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

        if file_type == 'FILE_EMISTI':
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

def verify_pipeline(filename):
    print(f'\n{"="*80}')
    print(f'Verifica {filename}')
    print('='*80)

    with open(filename, 'r', encoding='utf-8') as f:
        pipeline = json.load(f)

    deps, step_map = build_dependencies(pipeline['steps'])

    issues = []
    warnings = []
    stability_timeout_issues = []

    # Verifica ogni step
    for idx, step in enumerate(pipeline['steps']):
        step_num = step.get('stepNumber', idx+1)
        name = step.get('name', '')

        # Verifica consistenza stepNumber
        if step.get('stepNumber') != idx + 1:
            issues.append(f"Step {idx}: stepNumber={step.get('stepNumber')} ma dovrebbe essere {idx+1}")

        # Verifica presenza run command
        if not step.get('run'):
            issues.append(f"Step {step_num} ({name}): manca comando 'run'")

        # Verifica expect
        expect = step.get('expect', {})

        # Verifica exitCode
        if 'exitCode' not in expect:
            warnings.append(f"Step {step_num} ({name}): manca 'exitCode' in expect")

        # Verifica preCheck se presente
        precheck = expect.get('preCheck')
        if precheck:
            pc_type = precheck.get('type') if isinstance(precheck, dict) else None
            if pc_type in ['mongodb_files', 'mongodb_emisti', 'mongodb_emisti_speciale', 'mongodb_anasti', 'mongodb_22000x']:
                if 'rata' not in precheck:
                    issues.append(f"Step {step_num} ({name}): preCheck type '{pc_type}' manca 'rata'")
                if 'remoteHome' not in precheck:
                    issues.append(f"Step {step_num} ({name}): preCheck type '{pc_type}' manca 'remoteHome'")

        # Verifica postCheck se presente
        postcheck = expect.get('postCheck')
        if postcheck:
            pc_type = postcheck.get('type')
            if pc_type:
                if 'rata' not in postcheck:
                    issues.append(f"Step {step_num} ({name}): postCheck type '{pc_type}' manca 'rata'")
                if 'mongoHost' not in postcheck:
                    warnings.append(f"Step {step_num} ({name}): postCheck manca 'mongoHost'")
                if 'mongoDb' not in postcheck:
                    warnings.append(f"Step {step_num} ({name}): postCheck manca 'mongoDb'")

                # Verifica stabilityTimeoutMin
                if 'stabilityTimeoutMin' in postcheck:
                    timeout = postcheck['stabilityTimeoutMin']
                    if timeout < 10:
                        stability_timeout_issues.append(f"Step {step_num} ({name}): stabilityTimeoutMin={timeout}, dovrebbe essere 10")

    # Verifica dipendenze
    print(f"\nDIPENDENZE:")
    for idx, step in enumerate(pipeline['steps']):
        name = step.get('name', '')
        info = parse_step_info(name)
        if info and info['flow'] == 'RITENUTE' and info['file_type'] != 'SINIIS':
            print(f"  Step {idx+1} ({name}):")
            if deps[idx]:
                for dep_idx in sorted(deps[idx]):
                    dep_name = pipeline['steps'][dep_idx]['name']
                    print(f"    -> Step {dep_idx+1}: {dep_name}")
            else:
                issues.append(f"Step {idx+1} ({name}): NESSUNA DIPENDENZA (dovrebbe dipendere da SINIIS)")

    # Stampa risultati
    if issues:
        print(f"\nERRORI TROVATI ({len(issues)}):")
        for issue in issues:
            print(f"  - {issue}")

    if warnings:
        print(f"\nWARNING ({len(warnings)}):")
        for warning in warnings:
            print(f"  - {warning}")

    if stability_timeout_issues:
        print(f"\nSTABILITY TIMEOUT DA AGGIORNARE ({len(stability_timeout_issues)}):")
        for issue in stability_timeout_issues:
            print(f"  - {issue}")

    if not issues and not warnings and not stability_timeout_issues:
        print(f"\nNessun problema trovato!")

    return len(issues) == 0, stability_timeout_issues

# Verifica entrambi i pipeline
ok1, st1 = verify_pipeline('pipeline202503.json')
ok2, st2 = verify_pipeline('pipeline202510.json')

print(f"\n{'='*80}")
print("RIEPILOGO")
print('='*80)
print(f"pipeline202503.json: {'OK' if ok1 else 'ERRORI TROVATI'}")
print(f"pipeline202510.json: {'OK' if ok2 else 'ERRORI TROVATI'}")
