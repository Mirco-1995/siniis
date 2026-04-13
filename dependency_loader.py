#!/usr/bin/env python3
"""
Dependency loader module for opiRunner
Loads and applies dependency rules from external JSON configuration
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Set, Optional, Any


def parse_step_info(step_name: str) -> Optional[Dict[str, Any]]:
    """Parse step name to extract flow, file type, and rata information.

    Args:
        step_name: Step name string (e.g., "ORDINARIA - Transfer FILE_UTILITY")

    Returns:
        Dict with flow, file_type, and rata, or None if parsing fails
    """
    # Expected format: "FLOW - ... FILE_TYPE" or "FLOW - ... SINIIS"
    parts = step_name.split(' - ')
    if len(parts) < 2:
        return None

    flow = parts[0].strip()
    rest = parts[1].strip()

    # Extract file type
    file_type = None
    if 'FILE_UTILITY' in rest:
        file_type = 'FILE_UTILITY'
    elif 'FILE_EMISTI' in rest:
        file_type = 'FILE_EMISTI'
    elif 'FILE_22000X' in rest:
        file_type = 'FILE_22000X'
    elif 'FILE_ANASTI' in rest or 'ANASTI' in rest:
        file_type = 'ANASTI'
    elif 'SINIIS' in rest:
        file_type = 'SINIIS'

    if not file_type:
        return None

    # Try to extract RATA from step name (e.g., "202503")
    import re
    rata_match = re.search(r'\b(20\d{4})\b', step_name)
    rata = rata_match.group(1) if rata_match else None

    return {
        'flow': flow,
        'file_type': file_type,
        'rata': rata
    }


def load_dependency_rules(config_path: Path, logger: Optional[logging.Logger] = None) -> List[Dict]:
    """Load dependency rules from JSON configuration file.

    Args:
        config_path: Path to dependencies.json
        logger: Optional logger instance

    Returns:
        List of enabled dependency rules

    Raises:
        FileNotFoundError: If config file not found
        ValueError: If config file is invalid
    """
    if not config_path.exists():
        raise FileNotFoundError(f"Dependency config not found: {config_path}")

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in dependency config: {e}")

    rules = config.get('rules', [])
    enabled_rules = [rule for rule in rules if rule.get('enabled', True)]

    if logger:
        logger.info(f"Loaded {len(enabled_rules)}/{len(rules)} enabled dependency rules from {config_path}")

    return enabled_rules


def match_condition(step_info: Dict, condition: Dict) -> bool:
    """Check if step matches a rule condition.

    Args:
        step_info: Dict with flow, file_type, rata
        condition: Rule condition dict

    Returns:
        True if step matches condition, False otherwise
    """
    # Check current_file_type
    if 'current_file_type' in condition:
        if step_info['file_type'] != condition['current_file_type']:
            return False

    # Check current_file_type_not
    if 'current_file_type_not' in condition:
        if step_info['file_type'] == condition['current_file_type_not']:
            return False

    # Check current_flow
    if 'current_flow' in condition:
        if step_info['flow'] != condition['current_flow']:
            return False

    # Check current_flow_starts_with
    if 'current_flow_starts_with' in condition:
        if not step_info['flow'].startswith(condition['current_flow_starts_with']):
            return False

    # Check exclude_flows
    if 'exclude_flows' in condition:
        if step_info['flow'] in condition['exclude_flows']:
            return False

    return True


def find_dependencies(current_idx: int, current_step: Dict, all_steps: List[Dict],
                     step_map: Dict[str, int], rule: Dict) -> Set[int]:
    """Find step dependencies based on a single rule.

    Args:
        current_idx: Current step index
        current_step: Current step dict
        all_steps: List of all steps
        step_map: Map of "flow:rata:file_type" to step index
        rule: Dependency rule dict

    Returns:
        Set of step indices that current step depends on
    """
    dependencies = set()
    current_info = parse_step_info(current_step.get('name', ''))

    if not current_info:
        return dependencies

    depends_on = rule.get('depends_on', {})
    current_rata = current_info['rata'] or 'default'

    # Check if we need same_rata
    same_rata = depends_on.get('same_rata', False)

    # Handle multiple flows (e.g., SINIIS depends on ORDINARIA and all SPECIALE*)
    if 'flows' in depends_on:
        flows = depends_on['flows']
        file_type = depends_on.get('file_type')

        for other_idx, other_step in enumerate(all_steps):
            if other_idx == current_idx:
                continue

            other_info = parse_step_info(other_step.get('name', ''))
            if not other_info:
                continue

            # Check rata match if required
            other_rata = other_info['rata'] or 'default'
            if same_rata and other_rata != current_rata:
                continue

            # Check if flow matches
            flow_matches = False
            for flow_pattern in flows:
                if flow_pattern.endswith('*'):
                    # Wildcard match (e.g., SPECIALE*)
                    prefix = flow_pattern[:-1]
                    if other_info['flow'].startswith(prefix):
                        flow_matches = True
                        break
                else:
                    # Exact match
                    if other_info['flow'] == flow_pattern:
                        flow_matches = True
                        break

            if flow_matches and other_info['file_type'] == file_type:
                dependencies.add(other_idx)

    # Handle single flow dependency
    elif 'flow' in depends_on:
        target_flow = depends_on['flow']
        target_file = depends_on['file_type']
        target_rata = current_rata if same_rata else 'default'

        dep_key = f"{target_flow}:{target_rata}:{target_file}"
        if dep_key in step_map:
            dependencies.add(step_map[dep_key])

    # Handle same flow dependency
    elif depends_on.get('same_flow', False):
        target_flow = current_info['flow']
        target_file = depends_on['file_type']
        target_rata = current_rata if same_rata else 'default'

        dep_key = f"{target_flow}:{target_rata}:{target_file}"
        if dep_key in step_map:
            dependencies.add(step_map[dep_key])

    return dependencies


def build_dependencies_from_config(steps: List[Dict], config_path: Optional[Path] = None,
                                   logger: Optional[logging.Logger] = None) -> Dict[int, Set[int]]:
    """Build dependency graph from external JSON configuration.

    Args:
        steps: List of step configurations
        config_path: Path to dependencies.json (default: ./dependencies.json)
        logger: Optional logger instance

    Returns:
        Dictionary mapping step index to set of step indices it depends on
    """
    if config_path is None:
        # Default to dependencies.json in same directory as this script
        config_path = Path(__file__).parent / 'dependencies.json'

    # Load rules
    try:
        rules = load_dependency_rules(config_path, logger)
    except (FileNotFoundError, ValueError) as e:
        if logger:
            logger.warning(f"Failed to load dependency config: {e}")
            logger.warning("Falling back to empty dependency graph")
        return {idx: set() for idx in range(len(steps))}

    # Build step map
    step_map: Dict[str, int] = {}
    for idx, step in enumerate(steps):
        name = step.get('name', '')
        info = parse_step_info(name)
        if info:
            rata = info['rata'] or 'default'
            key = f"{info['flow']}:{rata}:{info['file_type']}"
            step_map[key] = idx

    # Build dependencies
    dependencies: Dict[int, Set[int]] = {idx: set() for idx in range(len(steps))}

    for idx, step in enumerate(steps):
        name = step.get('name', '')
        info = parse_step_info(name)

        if not info:
            continue

        # Apply each rule
        for rule in rules:
            condition = rule.get('condition', {})

            # Check if this rule applies to current step
            if not match_condition(info, condition):
                continue

            # Find dependencies for this rule
            rule_deps = find_dependencies(idx, step, steps, step_map, rule)
            dependencies[idx].update(rule_deps)

            if logger and rule_deps:
                logger.debug(f"Rule '{rule['name']}' added {len(rule_deps)} dependencies for step {idx}: {name}")

    return dependencies


if __name__ == '__main__':
    # Test the dependency loader
    import sys

    logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')
    logger = logging.getLogger(__name__)

    # Load sample pipeline
    pipeline_path = Path(__file__).parent / 'pipeline202503.json'
    if not pipeline_path.exists():
        logger.error(f"Test pipeline not found: {pipeline_path}")
        sys.exit(1)

    with open(pipeline_path, 'r', encoding='utf-8') as f:
        pipeline = json.load(f)

    steps = pipeline.get('steps', [])

    # Build dependencies
    deps = build_dependencies_from_config(steps, logger=logger)

    # Print results
    print("\n=== DEPENDENCY GRAPH ===")
    for idx, dep_set in deps.items():
        step_name = steps[idx].get('name', f'Step {idx}')
        if dep_set:
            print(f"\nStep {idx}: {step_name}")
            print(f"  Depends on: {sorted(dep_set)}")
            for dep_idx in sorted(dep_set):
                dep_name = steps[dep_idx].get('name', f'Step {dep_idx}')
                print(f"    - Step {dep_idx}: {dep_name}")
        else:
            print(f"\nStep {idx}: {step_name}")
            print(f"  No dependencies (can run in wave 1)")
