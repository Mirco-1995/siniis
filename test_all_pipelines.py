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

def test_pipeline(filename: str):
    print(f"\n{'='*80}")
    print(f"Testing {filename}")
    print('='*80)

    try:
        with open(filename, 'r', encoding='utf-8') as f:
            pipeline = json.load(f)
    except FileNotFoundError:
        print(f"File not found: {filename}")
        return

    deps, step_map = build_dependencies(pipeline['steps'])

    # Count RITENUTE and SINIIS steps
    ritenute_steps = []
    siniis_steps = []

    for idx, step in enumerate(pipeline['steps']):
        name = step.get('name', '')
        info = parse_step_info(name)

        if info:
            if info['flow'] == 'RITENUTE':
                ritenute_steps.append((idx, name, info))
            elif info['flow'] == 'SINIIS':
                siniis_steps.append((idx, name, info))

    print(f"\nFound {len(siniis_steps)} SINIIS steps and {len(ritenute_steps)} RITENUTE steps")

    # Check dependencies
    issues = []
    for idx, name, info in ritenute_steps:
        rata = info['rata'] or 'default'

        # Check if depends on SINIIS with same rata
        has_siniis_dep = False
        for dep_idx in deps[idx]:
            dep_name = pipeline['steps'][dep_idx]['name']
            dep_info = parse_step_info(dep_name)
            if dep_info and dep_info['flow'] == 'SINIIS':
                dep_rata = dep_info['rata'] or 'default'
                if dep_rata == rata:
                    has_siniis_dep = True
                    break

        if not has_siniis_dep and info['file_type'] != 'SINIIS':
            issues.append(f"  Step {idx} ({name}): MISSING SINIIS dependency")
        else:
            print(f"  Step {idx} ({name}): OK - depends on SINIIS")

    if issues:
        print("\nISSUES FOUND:")
        for issue in issues:
            print(issue)
    else:
        print("\nAll RITENUTE steps have correct SINIIS dependencies!")

# Test all pipeline files
test_pipeline('pipeline202503.json')
test_pipeline('pipeline202506.json')
test_pipeline('pipeline202510.json')
test_pipeline('pipeline.json')
test_pipeline('pipelineGennaio_Giugno.json')
test_pipeline('pipelineLuglio_Dicembre.json')
