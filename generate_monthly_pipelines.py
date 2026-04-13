#!/usr/bin/env python3
"""
Generate monthly pipeline files (202501-202512) from pipeline202503.json template.
Replaces rata in all step names, paths, variables, and configurations.
"""

import json
import re
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

    print(f"[OK] Generated {output_file} (rata {target_rata})")

def main():
    """Generate pipeline files for all months (January-December)."""

    template_file = 'pipeline202503.json'
    source_rata = '202503'

    # Generate for months 01-12
    months = [f'0{i}' if i < 10 else str(i) for i in range(1, 13)]

    print(f"Generating monthly pipeline files from {template_file}...")
    print(f"Source rata: {source_rata}")
    print("=" * 70)

    for month in months:
        target_rata = f'2025{month}'
        output_file = f'pipeline{target_rata}.json'

        # Skip if already exists and is the template
        if output_file == template_file:
            print(f"[SKIP] Skipping {output_file} (template file)")
            continue

        try:
            generate_pipeline_for_rata(template_file, source_rata, target_rata, output_file)
        except Exception as e:
            print(f"[ERROR] Error generating {output_file}: {e}")

    print("=" * 70)
    print(f"Done! Generated {len(months) - 1} pipeline files.")
    print("\nFiles created:")
    for month in months:
        target_rata = f'2025{month}'
        output_file = f'pipeline{target_rata}.json'
        if output_file != template_file and Path(output_file).exists():
            print(f"  - {output_file}")

if __name__ == '__main__':
    main()
