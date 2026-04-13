#!/usr/bin/env python3
"""Test that pipeline files pass validation with environment variables"""

import os
import json
import subprocess
import sys

# Set environment variables
os.environ['MONGO_HOST'] = '10.23.196.148'
os.environ['MONGO_PORT'] = '27017'
os.environ['MONGO_DB'] = 'opi-int'
os.environ['MONGO_USER'] = 'opiuser'
os.environ['MONGO_PASS'] = 'password123'
os.environ['MONGODB_URI'] = 'mongodb://opiuser:password123@10.23.196.148:27017/opi-int?directConnection=true&serverSelectionTimeoutMS=300000&connectTimeoutMS=300000&socketTimeoutMS=0&waitQueueTimeoutMS=900000&maxIdleTimeMS=3600000'

print("Testing pipeline validation with environment variables...")
print()

# Test pipeline202503.json
print("=" * 60)
print("TEST 1: pipeline202503.json")
print("=" * 60)

result = subprocess.run(
    ['python', 'opirunner.py', '-c', 'pipeline202503.json', '--dry-run'],
    capture_output=True,
    text=True,
    env=os.environ
)

if 'SECURITY WARNING' in result.stderr or 'SECURITY WARNING' in result.stdout:
    print("FAILED: Still contains security warnings")
    print(result.stderr)
    print(result.stdout)
    sys.exit(1)
elif 'Configuration validation failed' in result.stderr or 'Configuration validation failed' in result.stdout:
    print("FAILED: Configuration validation failed")
    print(result.stderr)
    print(result.stdout)
    sys.exit(1)
else:
    print("OK: No security warnings detected")
    print(f"Exit code: {result.returncode}")

print()

# Test pipeline202506.json
print("=" * 60)
print("TEST 2: pipeline202506.json")
print("=" * 60)

result = subprocess.run(
    ['python', 'opirunner.py', '-c', 'pipeline202506.json', '--dry-run'],
    capture_output=True,
    text=True,
    env=os.environ
)

if 'SECURITY WARNING' in result.stderr or 'SECURITY WARNING' in result.stdout:
    print("FAILED: Still contains security warnings")
    print(result.stderr)
    print(result.stdout)
    sys.exit(1)
elif 'Configuration validation failed' in result.stderr or 'Configuration validation failed' in result.stdout:
    print("FAILED: Configuration validation failed")
    print(result.stderr)
    print(result.stdout)
    sys.exit(1)
else:
    print("OK: No security warnings detected")
    print(f"Exit code: {result.returncode}")

print()
print("=" * 60)
print("ALL TESTS PASSED")
print("=" * 60)
