#!/usr/bin/env python3
"""
MongoDB checker module for opiRunner
Provides polling-based document count verification with configurable timeout
"""

import os
import re
import time
import logging
import hashlib
from pathlib import Path
from typing import Tuple, Optional, Dict, Any
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure

# Import circuit breaker
from circuit_breaker import get_circuit_breaker, CircuitBreakerOpen


# Global MongoDB connection pool (singleton)
_mongo_clients: Dict[str, MongoClient] = {}

# Global logger (can be set from opirunner)
_logger: Optional[logging.Logger] = None


# ============================================================================
# INPUT VALIDATION (Security: Prevent MongoDB Injection)
# ============================================================================

def validate_rata_format(rata: str) -> bool:
    """Validate RATA format (YYYYMM).

    Args:
        rata: RATA string to validate

    Returns:
        True if valid format, False otherwise
    """
    if not isinstance(rata, str):
        return False
    if not re.fullmatch(r'\d{6}', rata):
        return False
    try:
        year = int(rata[:4])
        month = int(rata[4:6])
        return 2000 <= year <= 2100 and 1 <= month <= 12
    except ValueError:
        return False


def sanitize_query_param(value: Any, param_name: str = "parameter") -> Any:
    """Sanitize query parameters to prevent MongoDB injection.

    Args:
        value: Parameter value to sanitize
        param_name: Parameter name for error messages

    Returns:
        Sanitized value

    Raises:
        ValueError: If value type is not allowed
    """
    # Reject dict/list types that could contain MongoDB operators
    if isinstance(value, dict):
        raise ValueError(
            f"Invalid query parameter type for '{param_name}': dict not allowed "
            f"(potential MongoDB injection risk)"
        )
    if isinstance(value, list):
        raise ValueError(
            f"Invalid query parameter type for '{param_name}': list not allowed "
            f"(potential MongoDB injection risk)"
        )

    # Only allow primitive types
    if not isinstance(value, (str, int, float, bool, type(None))):
        raise ValueError(
            f"Unsupported query parameter type for '{param_name}': {type(value).__name__}"
        )

    return value


def validate_tipo_flusso(tipo_flusso: str) -> bool:
    """Validate tipoFlusso parameter.

    Args:
        tipo_flusso: Type of flow to validate

    Returns:
        True if valid, False otherwise
    """
    valid_types = {'ORDINARIA', 'SPECIALE', 'RITENUTE', 'SINIIS'}
    return isinstance(tipo_flusso, str) and tipo_flusso in valid_types


def set_logger(logger: logging.Logger):
    """Set logger for circuit breaker and MongoDB operations.

    Args:
        logger: Logger instance to use
    """
    global _logger
    _logger = logger


def get_mongo_client(mongo_config: Dict) -> MongoClient:
    """Get or create MongoDB client from connection pool with circuit breaker protection.

    Args:
        mongo_config: MongoDB configuration dict with host, port, user, password

    Returns:
        MongoClient instance from pool

    Raises:
        CircuitBreakerOpen: If circuit breaker is open
        ConnectionFailure: If connection fails
    """
    mongo_uri = mongo_config.get('uri') or os.getenv('MONGODB_URI', '')
    mongo_host = mongo_config.get('host', 'localhost')
    mongo_port = mongo_config.get('port', 27017)
    mongo_user = mongo_config.get('user', '')
    mongo_pass = mongo_config.get('password', '')

    # Create connection key (avoid keeping credentials in clear)
    if mongo_uri:
        conn_key = f"uri:{hashlib.sha256(mongo_uri.encode('utf-8')).hexdigest()}"
    else:
        conn_key = f"{mongo_host}:{mongo_port}:{mongo_user}"

    # Get circuit breaker for this connection
    breaker = get_circuit_breaker(
        name=f"mongodb_{conn_key}",
        failure_threshold=5,
        timeout=60,
        logger=_logger
    )

    # Return existing client if available
    if conn_key in _mongo_clients:
        try:
            # Test connection is alive through circuit breaker
            def ping_test():
                _mongo_clients[conn_key].admin.command('ping')
                return _mongo_clients[conn_key]

            return breaker.call(ping_test)
        except CircuitBreakerOpen:
            # Circuit is open, propagate exception
            raise
        except Exception:
            # Connection dead, remove from pool
            try:
                _mongo_clients[conn_key].close()
            except:
                pass
            del _mongo_clients[conn_key]

    # Create new client through circuit breaker
    def create_client():
        if mongo_uri:
            client = MongoClient(
                mongo_uri,
                maxPoolSize=50,
                minPoolSize=10,
            )
        elif mongo_user and mongo_pass:
            client = MongoClient(
                mongo_host,
                mongo_port,
                username=mongo_user,
                password=mongo_pass,
                maxPoolSize=50,
                minPoolSize=10,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=10000
            )
        else:
            client = MongoClient(
                mongo_host,
                mongo_port,
                maxPoolSize=50,
                minPoolSize=10,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=10000
            )

        # Test connection with ping
        client.admin.command('ping')
        return client

    try:
        client = breaker.call(create_client)
        _mongo_clients[conn_key] = client
        return client
    except CircuitBreakerOpen:
        # Circuit is open, propagate exception
        raise


def close_all_mongo_clients():
    """Close all MongoDB clients in the pool."""
    for client in _mongo_clients.values():
        try:
            client.close()
        except:
            pass
    _mongo_clients.clear()


def check_files_and_count_lines(rata: str, remote_home: str) -> Tuple[bool, int, str]:
    """
    Pre-check: Verify presence of files and count total lines.

    Args:
        rata: RATA value (YYYYMM format)
        remote_home: Path to REMOTE_HOME directory

    Returns:
        Tuple of (success: bool, total_lines: int, message: str)
    """
    files_dir = Path(remote_home) / "elaborazioni" / "files"
    files_to_check = ["capitoli.txt", "dptall", "comuni"]

    # Check if directory exists
    if not files_dir.exists():
        return False, 0, f"Directory not found: {files_dir}"

    if not files_dir.is_dir():
        return False, 0, f"Path is not a directory: {files_dir}"

    # Check each file and count lines
    total_lines = 0
    messages = []

    for filename in files_to_check:
        file_path = files_dir / filename
        if not file_path.exists():
            return False, 0, f"File not found: {file_path}"

        if not file_path.is_file():
            return False, 0, f"Path is not a file: {file_path}"

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = sum(1 for _ in f)
            messages.append(f"File {filename}: {lines} lines")
            total_lines += lines
        except Exception as e:
            return False, 0, f"Failed to read file {file_path}: {e}"

    messages.append(f"Total lines across all files: {total_lines}")
    return True, total_lines, "\n".join(messages)


def check_emisti_files_and_count_lines(rata: str, remote_home: str) -> Tuple[bool, int, str]:
    """
    Pre-check for FILE_EMISTI: Verify presence of emi132 files and count lines starting with "01".

    Args:
        rata: RATA value (YYYYMM format)
        remote_home: Path to REMOTE_HOME directory

    Returns:
        Tuple of (success: bool, total_count_01: int, message: str)
    """
    # Validate RATA format
    if len(rata) != 6:
        return False, 0, f"Invalid RATA format: {rata} (expected YYYYMM)"

    # Build path: $REMOTE_HOME/elaborazioni/files/mese/ (literal "mese" directory)
    # emi132
    files_dir = Path(remote_home) / "elaborazioni" / "files" / "mese" / "emi132"

    # Files to check
    files_to_check = [
        "emi132-BO1",
        "emi132-BO2",
        "emi132-FI1",
        "emi132-NA1",
        "emi132-RGS",
        "emi132-RO1",
        "emi132-RO2"
    ]

    # Check if directory exists
    if not files_dir.exists():
        return False, 0, f"Directory not found: {files_dir}"

    if not files_dir.is_dir():
        return False, 0, f"Path is not a directory: {files_dir}"

    # Check each file and count lines starting with "01"
    total_count_01 = 0
    messages = []

    for filename in files_to_check:
        file_path = files_dir / filename
        if not file_path.exists():
            return False, 0, f"File not found: {file_path}"

        if not file_path.is_file():
            return False, 0, f"Path is not a file: {file_path}"

        try:
            # Tolleranza caratteri non UTF-8 per contare le righe
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                count_01 = sum(1 for line in f if line.startswith('01'))
            messages.append(f"File {filename}: {count_01} lines starting with '01'")
            total_count_01 += count_01
        except Exception as e:
            return False, 0, f"Failed to read file {file_path}: {e}"

    messages.append(f"Total lines starting with '01' across all files: {total_count_01}")
    return True, total_count_01, "\n".join(messages)


def check_emisti_speciale_files_and_count_lines(rata: str, remote_home: str) -> Tuple[bool, int, str]:
    """
    Pre-check for FILE_EMISTI SPECIALE: Verify presence of emi132-spe file and count lines starting with "01".

    Args:
        rata: RATA value (YYYYMM format)
        remote_home: Path to REMOTE_HOME directory

    Returns:
        Tuple of (success: bool, total_count_01: int, message: str)
    """
    # Validate RATA format
    if len(rata) != 6:
        return False, 0, f"Invalid RATA format: {rata} (expected YYYYMM)"

    # Build path: $REMOTE_HOME/elaborazioni/files/mese/ (literal "mese" directory)
    # emi132
    files_dir = Path(remote_home) / "elaborazioni" / "files" / "mese" / "emi132"

    # Single file for SPECIALE
    filename = "emi132-spe"

    # Check if directory exists
    if not files_dir.exists():
        return False, 0, f"Directory not found: {files_dir}"

    if not files_dir.is_dir():
        return False, 0, f"Path is not a directory: {files_dir}"

    # Check file and count lines starting with "01"
    file_path = files_dir / filename
    if not file_path.exists():
        return False, 0, f"File not found: {file_path}"

    if not file_path.is_file():
        return False, 0, f"Path is not a file: {file_path}"

    try:
        # Tolleranza caratteri non UTF-8 per contare le righe
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            count_01 = sum(1 for line in f if line.startswith('01'))

        message = f"File {filename}: {count_01} lines starting with '01'\n"
        message += f"Total lines starting with '01': {count_01}"

        return True, count_01, message
    except Exception as e:
        return False, 0, f"Failed to read file {file_path}: {e}"


def check_anasti_file_and_count_lines(
    rata: str,
    remote_home: str,
    mongo_config: Optional[dict] = None,
    poll_interval_sec: int = 10,
    max_wait_min: int = 60
) -> Tuple[bool, int, str]:
    """
    Pre-check for ANASTI:
    1. Verify all documents in 'execution' collection are in 'END' state (if mongo_config provided)
    2. Verify presence of anasti-tel1-rit file and count all lines

    Args:
        rata: RATA value (YYYYMM format)
        remote_home: Path to REMOTE_HOME directory
        mongo_config: Optional MongoDB configuration for execution state check
        poll_interval_sec: Polling interval in seconds (default: 10)
        max_wait_min: Maximum wait time in minutes (default: 60)

    Returns:
        Tuple of (success: bool, total_lines: int, message: str)
    """
    messages = []

    # STEP 1: Check execution state if MongoDB config is provided
    if mongo_config:
        messages.append("=" * 60)
        messages.append("STEP 1: Checking execution state in MongoDB")
        messages.append("=" * 60)

        exec_ok, _, exec_msg = check_execution_state_all_end(
            rata=rata,
            mongo_config=mongo_config,
            poll_interval_sec=poll_interval_sec,
            max_wait_min=max_wait_min
        )

        messages.append(exec_msg)

        if not exec_ok:
            return False, 0, "\n".join(messages)

        messages.append("")
        messages.append("=" * 60)
        messages.append("STEP 2: Checking ANASTI file existence and counting lines")
        messages.append("=" * 60)

    # Validate RATA format
    if len(rata) != 6:
        messages.append(f"Invalid RATA format: {rata} (expected YYYYMM)")
        return False, 0, "\n".join(messages)

    # Build path: $REMOTE_HOME/elaborazioni/files/mese/ (literal "mese" directory)
    files_dir = Path(remote_home) / "elaborazioni" / "files" / "mese"

    # File to check
    filename = "anasti-tel1-rit.txt"

    # Check if directory exists
    if not files_dir.exists():
        messages.append(f"Directory not found: {files_dir}")
        return False, 0, "\n".join(messages)

    if not files_dir.is_dir():
        messages.append(f"Path is not a directory: {files_dir}")
        return False, 0, "\n".join(messages)

    # Check file existence and count lines
    file_path = files_dir / filename
    if not file_path.exists():
        messages.append(f"File not found: {file_path}")
        return False, 0, "\n".join(messages)

    if not file_path.is_file():
        messages.append(f"Path is not a file: {file_path}")
        return False, 0, "\n".join(messages)

    try:
        # Prova prima con UTF-8, poi con Latin-1 (ISO-8859-1)
        encodings = ['utf-8', 'latin-1', 'cp1252']
        total_lines = 0
        encoding_used = None

        for encoding in encodings:
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    total_lines = sum(1 for line in f)
                encoding_used = encoding
                break
            except UnicodeDecodeError:
                continue

        if encoding_used is None:
            messages.append(f"Failed to decode file {file_path} with any supported encoding (utf-8, latin-1, cp1252)")
            return False, 0, "\n".join(messages)

        messages.append(f"File {filename}: {total_lines} lines (encoding: {encoding_used})")
        messages.append(f"Total lines: {total_lines}")

        return True, total_lines, "\n".join(messages)
    except Exception as e:
        messages.append(f"Failed to read file {file_path}: {e}")
        return False, 0, "\n".join(messages)


def check_22000x_files_and_count_lines(rata: str, remote_home: str) -> Tuple[bool, int, str]:
    """
    Pre-check for FILE_22000X: Read parametri_firma_digitale.txt, verify file presence, count lines starting with "Q".

    Args:
        rata: RATA value (YYYYMM format)
        remote_home: Path to REMOTE_HOME directory

    Returns:
        Tuple of (success: bool, total_count_Q: int, message: str)
    """
    # Validate RATA format
    if len(rata) != 6:
        return False, 0, f"Invalid RATA format: {rata} (expected YYYYMM)"

    # Build path: $REMOTE_HOME/elaborazioni/files/mese/ (literal "mese" directory)
    # 
    files_dir = Path(remote_home) / "elaborazioni" / "files" / "mese"

    # File to read
    param_file = files_dir / "parametri_firma_digitale.txt"

    # Check if parametri_firma_digitale.txt exists
    if not param_file.exists():
        return False, 0, f"File not found: {param_file}"

    if not param_file.is_file():
        return False, 0, f"Path is not a file: {param_file}"

    # Read parametri_firma_digitale.txt and extract FILE= lines
    file_paths = []
    try:
        with open(param_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line.startswith('FILE='):
                    # Extract path after FILE=
                    file_path_str = line[5:]  # Remove 'FILE='
                    file_paths.append(file_path_str)
    except Exception as e:
        return False, 0, f"Failed to read {param_file}: {e}"

    if not file_paths:
        return False, 0, f"No FILE= entries found in {param_file}"

    # Check each file and count lines starting with "Q"
    total_count_q = 0
    messages = []
    messages.append(f"Found {len(file_paths)} file(s) in parametri_firma_digitale.txt")

    for file_path_str in file_paths:
        file_path = Path(file_path_str)

        # Extract filename from path
        filename = file_path.name

        # Check if file exists
        if not file_path.exists():
            return False, 0, f"File not found: {file_path}"

        if not file_path.is_file():
            return False, 0, f"Path is not a file: {file_path}"

        # Count lines starting with "Q"
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                count_q = sum(1 for line in f if line.startswith('Q'))
            messages.append(f"File {filename}: {count_q} lines starting with 'Q'")
            total_count_q += count_q
        except Exception as e:
            return False, 0, f"Failed to read file {file_path}: {e}"

    messages.append(f"Total lines starting with 'Q' across all files: {total_count_q}")
    return True, total_count_q, "\n".join(messages)


def check_execution_state_all_end(
    rata: str,
    mongo_config: dict,
    poll_interval_sec: int = 10,
    max_wait_min: int = 60
) -> Tuple[bool, int, str]:
    """
    Pre-check for ANASTI: Verify all documents in 'execution' collection are in 'END' state.
    Polls until all documents reach END state or max wait time is exceeded.

    Query pattern: {
        masterMachineId: 'RITENUTE-SPT-{rata}',
        workflowType: 'ACQUISIZIONE_AGGREGAZIONE',
        state: {$ne: 'END'}
    }

    Args:
        rata: RATA value (YYYYMM format)
        mongo_config: MongoDB connection configuration
        poll_interval_sec: Polling interval in seconds (default: 10)
        max_wait_min: Maximum wait time in minutes (default: 60)

    Returns:
        Tuple of (success: bool, count: int, message: str)
        - success: True if all documents are in END state
        - count: Always 0 (for consistency with other preCheck functions)
        - message: Status message
    """
    mongo_host = mongo_config.get('host', 'localhost')
    mongo_port = mongo_config.get('port', 27017)
    mongo_db = mongo_config.get('database', 'your_database')

    # SECURITY: Validate inputs to prevent MongoDB injection
    if not validate_rata_format(rata):
        return False, 0, f"Invalid RATA format: {rata} (expected YYYYMM)"

    try:
        rata = sanitize_query_param(rata, "rata")
    except ValueError as e:
        return False, 0, f"Parameter validation failed: {e}"

    messages = []
    messages.append("Expected: All execution documents in END state")
    messages.append(f"MongoDB: {mongo_host}:{mongo_port}/{mongo_db}")
    messages.append(f"Collection: execution")
    messages.append(f"masterMachineId: RITENUTE-SPT-{rata}")
    messages.append(f"workflowType: ACQUISIZIONE_AGGREGAZIONE")
    messages.append(f"Poll interval: {poll_interval_sec}s, Max wait: {max_wait_min}min")
    messages.append("-" * 60)

    client = None
    try:
        client = get_mongo_client(mongo_config)

        db = client[mongo_db]
        collection = db['execution']

        start_time = time.time()
        max_wait_sec = max_wait_min * 60
        poll_count = 0

        while True:
            poll_count += 1
            elapsed_sec = time.time() - start_time

            if elapsed_sec > max_wait_sec:
                msg = "\n".join(messages)
                msg += f"\n\nERROR: Maximum wait time ({max_wait_min} minutes) exceeded"
                msg += f"\nDocuments still not in END state after {elapsed_sec:.1f}s"
                return False, 0, msg

            try:
                query = {
                    "masterMachineId": f"RITENUTE-SPT-{rata}",
                    "workflowType": "ACQUISIZIONE_AGGREGAZIONE",
                    "state": {"$ne": "END"}
                }
                non_end_count = collection.count_documents(query)
                poll_msg = f"Poll #{poll_count} ({elapsed_sec:.0f}s): {mongo_db}.execution query={query} non-END count={non_end_count}"
                messages.append(poll_msg)
                if _logger:
                    _logger.info(poll_msg)

                if non_end_count == 0:
                    msg = "\n".join(messages)
                    msg += f"\n\nSUCCESS: All execution documents are in END state"
                    msg += f"\nTotal time: {elapsed_sec:.1f}s ({poll_count} polls)"
                    return True, 0, msg

                poll_msg_detail = f"  Waiting for {non_end_count} document(s) to reach END state..."
                messages.append(poll_msg_detail)
                if _logger:
                    _logger.info(poll_msg_detail)

            except (OperationFailure, Exception) as e:
                messages.append(f"Poll #{poll_count}: Query error: {e}")
                if _logger:
                    _logger.error(f"Query error: {e}")

            time.sleep(poll_interval_sec)

    except ConnectionFailure as e:
        return False, 0, f"Failed to connect to MongoDB: {e}"
    except Exception as e:
        return False, 0, f"Unexpected error: {e}"
    # No finally block - connection pool manages lifecycle


def verify_emisti_data_quality(
    rata: str,
    expected_count: int,
    mongo_config: dict,
    tipo_flusso: str = "ORDINARIA",
    progressivo_speciale: Optional[int] = None
) -> Tuple[bool, str]:
    """
    Post-check for FILE_EMISTI: Verify data quality in flussoEMISTI collection.

    Performs multiple validation checks:
    1. Count verification (expected vs actual)
    2. PG field validation (empty or missing)
    3. IBAN field validation (empty or missing)
    4. Amount validation (negative or zero importoNetto)
    5. Balance verification (importoNettoCedolino vs sum of vociAddebito)

    Args:
        rata: RATA value (YYYYMM format)
        expected_count: Expected number of documents
        mongo_config: MongoDB configuration dict
        tipo_flusso: Tipo flusso (default: "ORDINARIA")
        progressivo_speciale: Progressivo speciale (optional)

    Returns:
        Tuple of (success: bool, message: str)
    """
    mongo_host = mongo_config.get('host', 'localhost')
    mongo_port = mongo_config.get('port', 27017)
    mongo_db = mongo_config.get('database', 'your_database')
    mongo_user = mongo_config.get('user', '')
    mongo_pass = mongo_config.get('password', '')

    client = None  # Initialize to avoid UnboundLocalError in finally block
    try:
        if mongo_user and mongo_pass:
            uri = f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}:{mongo_port}/{mongo_db}"
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        else:
            client = MongoClient(mongo_host, mongo_port, serverSelectionTimeoutMS=5000)

        client.admin.command('ping')

        db = client[mongo_db]
        collection = db['flussoEMISTI']

        messages = []
        messages.append(f"Expected document count: {expected_count}")
        messages.append(f"MongoDB: {mongo_host}:{mongo_port}/{mongo_db}")
        messages.append(f"Collection: flussoEMISTI")
        messages.append("-" * 60)

        # Build base match query with tipoFlusso and optional progressivoSpeciale
        def build_match(additional_filters=None):
            """Helper to build match query with base filters"""
            match_query = {
                "rataEmissione": rata,
                "tipoSpesa": "SPT",
                "tipoFlusso": tipo_flusso
            }
            # Add progressivoSpeciale filter if provided (exists in flussoEMISTI collection)
            if progressivo_speciale is not None:
                match_query["progressivoSpeciale"] = progressivo_speciale
            if additional_filters:
                match_query.update(additional_filters)
            return match_query

        # Execute aggregate query with all checks
        pipeline = [
            {
                "$facet": {
                    "countAddPgVuoto": [
                        {"$match": build_match({"vociAddebito": {"$elemMatch": {"pg": ""}}})},
                        {"$count": "total"}
                    ],
                    "countADDpgNotExists": [
                        {"$match": build_match({"vociAddebito": {"$elemMatch": {"pg": {"$exists": False}}}})},
                        {"$count": "total"}
                    ],
                    "countAddIbanVuoto": [
                        {"$match": build_match({"vociAddebito": {"$elemMatch": {"iban": ""}}})},
                        {"$count": "total"}
                    ],
                    "countAddIbanNotExists": [
                        {"$match": build_match({"vociAddebito": {"$elemMatch": {"iban": {"$exists": False}}}})},
                        {"$count": "total"}
                    ],
                    "countVociImpoMinZero": [
                        {"$match": build_match({"vociAddebito": {"$elemMatch": {"importoNetto": {"$lt": 0}}}})},
                        {"$count": "total"}
                    ],
                    "countVociImpoEqZero": [
                        {"$match": build_match({"vociAddebito": {"$elemMatch": {"importoNetto": {"$eq": 0}}}})},
                        {"$count": "total"}
                    ],
                    "countCedoliniSenzaIbanConImporto": [
                        {"$match": build_match({
                            "vociAddebito.iban": {"$exists": False},
                            "importoNettoCedolino": {"$gt": 0}
                        })},
                        {"$count": "total"}
                    ],
                    "countImportoNettoNegativo": [
                        {"$match": build_match({"importoNettoCedolino": {"$lt": 0}})},
                        {"$count": "total"}
                    ],
                    "countQuadraturaNonCorretta": [
                        {"$match": build_match()},
                        {
                            "$addFields": {
                                "sommaVociAddebito": {
                                    "$sum": "$vociAddebito.importoNetto"
                                }
                            }
                        },
                        {
                            "$match": {
                                "$expr": {
                                    "$ne": ["$importoNettoCedolino", "$sommaVociAddebito"]
                                }
                            }
                        },
                        {"$count": "total"}
                    ],
                    "totalCount": [
                        {"$match": build_match()},
                        {"$count": "total"}
                    ]
                }
            }
        ]

        result = list(collection.aggregate(pipeline))
        if not result:
            return False, "Aggregate query returned no results"

        facets = result[0]

        # Extract counts (handle empty lists from facet queries)
        total_count = facets.get('totalCount', [{}])[0].get('total', 0) if facets.get('totalCount') else 0
        pg_empty = facets.get('countAddPgVuoto', [{}])[0].get('total', 0) if facets.get('countAddPgVuoto') else 0
        pg_not_exists = facets.get('countADDpgNotExists', [{}])[0].get('total', 0) if facets.get('countADDpgNotExists') else 0
        iban_empty = facets.get('countAddIbanVuoto', [{}])[0].get('total', 0) if facets.get('countAddIbanVuoto') else 0
        iban_not_exists = facets.get('countAddIbanNotExists', [{}])[0].get('total', 0) if facets.get('countAddIbanNotExists') else 0
        impo_negative = facets.get('countVociImpoMinZero', [{}])[0].get('total', 0) if facets.get('countVociImpoMinZero') else 0
        impo_zero = facets.get('countVociImpoEqZero', [{}])[0].get('total', 0) if facets.get('countVociImpoEqZero') else 0
        cedolini_senza_iban_con_importo = facets.get('countCedoliniSenzaIbanConImporto', [{}])[0].get('total', 0) if facets.get('countCedoliniSenzaIbanConImporto') else 0
        importo_netto_negativo = facets.get('countImportoNettoNegativo', [{}])[0].get('total', 0) if facets.get('countImportoNettoNegativo') else 0
        quadratura_non_corretta = facets.get('countQuadraturaNonCorretta', [{}])[0].get('total', 0) if facets.get('countQuadraturaNonCorretta') else 0

        messages.append(f"Total documents: {total_count}")
        messages.append("")
        messages.append("Data Quality Checks:")
        messages.append(f"  - vociAddebito.pg empty: {pg_empty}")
        messages.append(f"  - vociAddebito.pg not exists: {pg_not_exists}")
        messages.append(f"  - vociAddebito.iban empty: {iban_empty}")
        messages.append(f"  - vociAddebito.iban not exists: {iban_not_exists}")
        messages.append(f"  - vociAddebito.importoNetto < 0: {impo_negative}")
        messages.append(f"  - vociAddebito.importoNetto = 0: {impo_zero}")
        messages.append(f"  - Cedolini senza IBAN con importo > 0: {cedolini_senza_iban_con_importo}")
        messages.append(f"  - importoNettoCedolino < 0: {importo_netto_negativo}")
        messages.append(f"  - Quadratura non corretta (importo != somma voci): {quadratura_non_corretta}")

        # Check for errors
        errors = []

        if total_count != expected_count:
            errors.append(f"Document count mismatch: expected {expected_count}, got {total_count}")

        if pg_empty > 0:
            errors.append(f"{pg_empty} documents with empty vociAddebito.pg field")

        if pg_not_exists > 0:
            errors.append(f"{pg_not_exists} documents with missing vociAddebito.pg field")

        if iban_empty > 0:
            errors.append(f"{iban_empty} documents with empty vociAddebito.iban field")

        if iban_not_exists > 0:
            errors.append(f"{iban_not_exists} documents with missing vociAddebito.iban field")

        if impo_negative > 0:
            errors.append(f"{impo_negative} documents with negative vociAddebito.importoNetto")

        if impo_zero > 0:
            errors.append(f"{impo_zero} documents with zero vociAddebito.importoNetto")

        if cedolini_senza_iban_con_importo > 0:
            errors.append(f"{cedolini_senza_iban_con_importo} cedolini without IBAN but with importo > 0")

        if importo_netto_negativo > 0:
            errors.append(f"{importo_netto_negativo} documents with negative importoNettoCedolino")

        if quadratura_non_corretta > 0:
            errors.append(f"{quadratura_non_corretta} documents with balance mismatch (importoNettoCedolino != sum of vociAddebito)")

        if errors:
            messages.append("")
            messages.append("ERRORS FOUND:")
            for error in errors:
                messages.append(f"  ❌ {error}")
            return False, "\n".join(messages)

        messages.append("")
        messages.append("✅ All data quality checks passed")
        return True, "\n".join(messages)

    except ConnectionFailure as e:
        return False, f"Failed to connect to MongoDB: {e}"
    except OperationFailure as e:
        return False, f"MongoDB operation failed: {e}"
    except Exception as e:
        return False, f"Unexpected error: {e}"
    finally:
        if client is not None:
            try:
                client.close()
            except:
                pass


def verify_emisti_data_quality_with_polling(
    rata: str,
    expected_count: int,
    mongo_config: dict,
    tipo_flusso: str,
    progressivo_speciale: Optional[int] = None,
    poll_interval_sec: int = 30,
    stability_timeout_min: int = 5,
    max_wait_min: int = 60
) -> Tuple[bool, str]:
    """
    Post-check for FILE_EMISTI with polling: Wait for correct document count, then verify data quality.

    PHASE 1 - Document Count Polling:
    Polls MongoDB periodically until document count matches expected.
    - Count matches expected -> proceed to PHASE 2 (quality checks)
    - Count doesn't change for stability_timeout_min minutes -> FAILURE
    - Total wait time exceeds max_wait_min minutes -> FAILURE

    PHASE 2 - Quality Checks (only if count is correct):
    Executes detailed data quality checks. If count is not correct, this phase is NEVER executed.

    Args:
        rata: RATA value (YYYYMM format)
        expected_count: Expected number of documents
        mongo_config: MongoDB configuration dict
        tipo_flusso: Tipo flusso (e.g., "ORDINARIA", "SPECIALE")
        progressivo_speciale: Progressivo speciale (required for SPECIALE, optional otherwise)
        poll_interval_sec: Seconds between polls (default: 30)
        stability_timeout_min: Minutes of stability before giving up (default: 5)
        max_wait_min: Maximum total wait time in minutes (default: 60)

    Returns:
        Tuple of (success: bool, message: str)
    """
    import time

    mongo_host = mongo_config.get('host', 'localhost')
    mongo_port = mongo_config.get('port', 27017)
    mongo_db = mongo_config.get('database', 'your_database')
    mongo_user = mongo_config.get('user', '')
    mongo_pass = mongo_config.get('password', '')

    # SECURITY: Validate inputs to prevent MongoDB injection
    if not validate_rata_format(rata):
        return False, f"Invalid RATA format: {rata} (expected YYYYMM)"

    if not validate_tipo_flusso(tipo_flusso):
        return False, f"Invalid tipoFlusso: {tipo_flusso} (expected ORDINARIA, SPECIALE, RITENUTE, or SINIIS)"

    try:
        rata = sanitize_query_param(rata, "rata")
        tipo_flusso = sanitize_query_param(tipo_flusso, "tipo_flusso")
        if progressivo_speciale is not None:
            progressivo_speciale = sanitize_query_param(progressivo_speciale, "progressivo_speciale")
    except ValueError as e:
        return False, f"Parameter validation failed: {e}"

    messages = []
    messages.append(f"Expected document count: {expected_count}")
    messages.append(f"MongoDB: {mongo_host}:{mongo_port}/{mongo_db}")
    messages.append(f"Collection: flussoEMISTI")
    messages.append(f"Poll interval: {poll_interval_sec}s, Stability timeout: {stability_timeout_min}min, Max wait: {max_wait_min}min")
    messages.append("-" * 60)

    try:
        # Use connection pool with circuit breaker protection
        client = get_mongo_client(mongo_config)
        if client is None:
            return False, "Failed to get MongoDB client (circuit breaker may be OPEN)"

        db = client[mongo_db]
        collection = db['flussoEMISTI']

        # ===== PHASE 1: Document Count Polling =====
        messages.append("PHASE 1: Waiting for correct document count...")

        start_time = time.time()
        max_wait_sec = max_wait_min * 60
        stability_timeout_sec = stability_timeout_min * 60

        last_count = -1
        last_change_time = start_time
        poll_count = 0

        while True:
            poll_count += 1
            elapsed_sec = time.time() - start_time

            # Build query with tipoFlusso and optional progressivoSpeciale
            query = {
                "rataEmissione": rata,
                "tipoSpesa": "SPT",
                "tipoFlusso": tipo_flusso
            }
            # Add progressivoSpeciale filter if provided (exists in flussoEMISTI collection)
            if progressivo_speciale is not None:
                query["progressivoSpeciale"] = progressivo_speciale

            # Simple count query
            total_count = collection.count_documents(query)

            # Track count changes for stability detection
            if total_count != last_count:
                last_count = total_count
                last_change_time = time.time()

            # Log poll result
            poll_msg = f"Poll #{poll_count} ({int(elapsed_sec)}s): {total_count} documents"
            messages.append(poll_msg)
            if _logger:
                _logger.info(poll_msg)

            # SUCCESS: count matches expected -> proceed to PHASE 2
            if total_count == expected_count:
                messages.append("")
                messages.append(f"✓ Document count matches expected ({expected_count})")
                messages.append(f"Total polling time: {elapsed_sec:.1f}s ({poll_count} polls)")
                messages.append("")
                break

            # FAILURE: stability timeout (count hasn't changed)
            time_since_change = time.time() - last_change_time
            if time_since_change >= stability_timeout_sec:
                messages.append("")
                messages.append(f"FAILURE: Document count stable at {total_count} for {stability_timeout_min} min")
                messages.append(f"Expected: {expected_count}, Got: {total_count}")
                messages.append("❌ Document count mismatch is a critical error - quality checks not executed")
                return False, "\n".join(messages)

            # FAILURE: max wait timeout
            if elapsed_sec >= max_wait_sec:
                messages.append("")
                messages.append(f"FAILURE: Maximum wait time ({max_wait_min} min) exceeded")
                messages.append(f"Expected: {expected_count}, Got: {total_count}")
                messages.append("❌ Document count mismatch is a critical error - quality checks not executed")
                return False, "\n".join(messages)

            # Continue polling
            time.sleep(poll_interval_sec)

        # ===== PHASE 2: Data Quality Checks =====
        messages.append("PHASE 2: Executing data quality checks...")

        # Build base match query with tipoFlusso and optional progressivoSpeciale
        def build_match(additional_filters=None):
            """Helper to build match query with base filters"""
            match_query = {
                "rataEmissione": rata,
                "tipoSpesa": "SPT",
                "tipoFlusso": tipo_flusso
            }
            # Add progressivoSpeciale filter if provided (exists in flussoEMISTI collection)
            if progressivo_speciale is not None:
                match_query["progressivoSpeciale"] = progressivo_speciale
            if additional_filters:
                match_query.update(additional_filters)
            return match_query

        # Execute aggregate query with all quality checks
        pipeline = [
            {
                "$facet": {
                    "countAddPgVuoto": [
                        {"$match": build_match({"vociAddebito": {"$elemMatch": {"pg": ""}}})},
                        {"$count": "total"}
                    ],
                    "countADDpgNotExists": [
                        {"$match": build_match({"vociAddebito": {"$elemMatch": {"pg": {"$exists": False}}}})},
                        {"$count": "total"}
                    ],
                    "countAddIbanVuoto": [
                        {"$match": build_match({"vociAddebito": {"$elemMatch": {"iban": ""}}})},
                        {"$count": "total"}
                    ],
                    "countAddIbanNotExists": [
                        {"$match": build_match({"vociAddebito": {"$elemMatch": {"iban": {"$exists": False}}}})},
                        {"$count": "total"}
                    ],
                    "countVociImpoMinZero": [
                        {"$match": build_match({"vociAddebito": {"$elemMatch": {"importoNetto": {"$lt": 0}}}})},
                        {"$count": "total"}
                    ],
                    "countVociImpoEqZero": [
                        {"$match": build_match({"vociAddebito": {"$elemMatch": {"importoNetto": {"$eq": 0}}}})},
                        {"$count": "total"}
                    ],
                    "countCedoliniSenzaIbanConImporto": [
                        {"$match": build_match({
                            "vociAddebito.iban": {"$exists": False},
                            "importoNettoCedolino": {"$gt": 0}
                        })},
                        {"$count": "total"}
                    ],
                    "countImportoNettoNegativo": [
                        {"$match": build_match({"importoNettoCedolino": {"$lt": 0}})},
                        {"$count": "total"}
                    ],
                    "countQuadraturaNonCorretta": [
                        {"$match": build_match()},
                        {
                            "$addFields": {
                                "sommaVociAddebito": {
                                    "$sum": "$vociAddebito.importoNetto"
                                }
                            }
                        },
                        {
                            "$match": {
                                "$expr": {
                                    "$ne": ["$importoNettoCedolino", "$sommaVociAddebito"]
                                }
                            }
                        },
                        {"$count": "total"}
                    ]
                }
            }
        ]

        result = list(collection.aggregate(pipeline))
        if not result:
            return False, "\n".join(messages) + "\n\nFAILURE: Aggregate query returned no results"

        facets = result[0]

        # Extract quality check counts (handle empty lists from facet queries)
        pg_empty = facets.get('countAddPgVuoto', [{}])[0].get('total', 0) if facets.get('countAddPgVuoto') else 0
        pg_not_exists = facets.get('countADDpgNotExists', [{}])[0].get('total', 0) if facets.get('countADDpgNotExists') else 0
        iban_empty = facets.get('countAddIbanVuoto', [{}])[0].get('total', 0) if facets.get('countAddIbanVuoto') else 0
        iban_not_exists = facets.get('countAddIbanNotExists', [{}])[0].get('total', 0) if facets.get('countAddIbanNotExists') else 0
        impo_negative = facets.get('countVociImpoMinZero', [{}])[0].get('total', 0) if facets.get('countVociImpoMinZero') else 0
        impo_zero = facets.get('countVociImpoEqZero', [{}])[0].get('total', 0) if facets.get('countVociImpoEqZero') else 0
        cedolini_senza_iban = facets.get('countCedoliniSenzaIbanConImporto', [{}])[0].get('total', 0) if facets.get('countCedoliniSenzaIbanConImporto') else 0
        importo_netto_neg = facets.get('countImportoNettoNegativo', [{}])[0].get('total', 0) if facets.get('countImportoNettoNegativo') else 0
        quadratura_err = facets.get('countQuadraturaNonCorretta', [{}])[0].get('total', 0) if facets.get('countQuadraturaNonCorretta') else 0

        messages.append("")
        messages.append("Data Quality Check Results:")
        messages.append(f"  - vociAddebito.pg empty: {pg_empty}")
        messages.append(f"  - vociAddebito.pg not exists: {pg_not_exists}")
        messages.append(f"  - vociAddebito.iban empty: {iban_empty}")
        messages.append(f"  - vociAddebito.iban not exists: {iban_not_exists}")
        messages.append(f"  - vociAddebito.importoNetto < 0: {impo_negative}")
        messages.append(f"  - vociAddebito.importoNetto = 0: {impo_zero}")
        messages.append(f"  - Cedolini senza IBAN con importo > 0: {cedolini_senza_iban}")
        messages.append(f"  - importoNettoCedolino < 0: {importo_netto_neg}")
        messages.append(f"  - Quadratura non corretta: {quadratura_err}")

        # Check for quality errors
        errors = []
        if pg_empty > 0:
            errors.append(f"{pg_empty} documents with empty vociAddebito.pg")
        if pg_not_exists > 0:
            errors.append(f"{pg_not_exists} documents with missing vociAddebito.pg")
        if iban_empty > 0:
            errors.append(f"{iban_empty} documents with empty vociAddebito.iban")
        if iban_not_exists > 0:
            errors.append(f"{iban_not_exists} documents with missing vociAddebito.iban")
        if impo_negative > 0:
            errors.append(f"{impo_negative} documents with negative vociAddebito.importoNetto")
        if impo_zero > 0:
            errors.append(f"{impo_zero} documents with zero vociAddebito.importoNetto")
        if cedolini_senza_iban > 0:
            errors.append(f"{cedolini_senza_iban} cedolini without IBAN but with importo > 0")
        if importo_netto_neg > 0:
            errors.append(f"{importo_netto_neg} documents with negative importoNettoCedolino")
        if quadratura_err > 0:
            errors.append(f"{quadratura_err} documents with balance mismatch")

        if errors:
            messages.append("")
            messages.append("FAILURE: Quality check errors found:")
            for error in errors:
                messages.append(f"  ❌ {error}")
            return False, "\n".join(messages)

        messages.append("")
        messages.append("✅ SUCCESS: All quality checks passed")
        return True, "\n".join(messages)

    except ConnectionFailure as e:
        return False, f"Failed to connect to MongoDB: {e}"
    except OperationFailure as e:
        return False, f"MongoDB operation failed: {e}"
    except Exception as e:
        return False, f"Unexpected error: {e}"


def verify_mongodb_count_with_polling(
    rata: str,
    expected_count: int,
    mongo_config: dict,
    poll_interval_sec: int = 30,
    stability_timeout_min: int = 5,
    max_wait_min: int = 60
) -> Tuple[bool, str]:
    """
    Post-check: Verify MongoDB document count with polling and stability check.

    Polls MongoDB until:
    - Document count matches expected count (success), OR
    - Document count is stable for stability_timeout_min minutes (failure), OR
    - Maximum wait time is reached (failure)

    Args:
        rata: RATA value (YYYYMM format)
        expected_count: Expected number of documents
        mongo_config: MongoDB configuration dict with keys:
            - host, port, database, user (optional), password (optional)
        poll_interval_sec: Seconds between polls (default: 30)
        stability_timeout_min: Minutes of stable count before giving up (default: 5)
        max_wait_min: Maximum total wait time in minutes (default: 60)

    Returns:
        Tuple of (success: bool, message: str)
    """
    mongo_host = mongo_config.get('host', 'localhost')
    mongo_port = mongo_config.get('port', 27017)
    mongo_db = mongo_config.get('database', 'your_database')

    # SECURITY: Validate inputs to prevent MongoDB injection
    if not validate_rata_format(rata):
        return False, f"Invalid RATA format: {rata} (expected YYYYMM)"

    try:
        rata = sanitize_query_param(rata, "rata")
    except ValueError as e:
        return False, f"Parameter validation failed: {e}"

    # Use connection pool
    client = None  # ensure defined for finally
    try:
        client = get_mongo_client(mongo_config)

        # Get collection
        db = client[mongo_db]
        collection = db['anagrafiche']

        # Query filter
        query = {
            "rataEmissione": rata,
            "tipoSpesa": "SPT"
        }

        # Polling variables
        start_time = time.time()
        max_wait_sec = max_wait_min * 60
        stability_timeout_sec = stability_timeout_min * 60

        last_count = None
        last_count_time = None
        poll_count = 0

        messages = []
        messages.append(f"Expected document count: {expected_count}")
        messages.append(f"MongoDB: {mongo_host}:{mongo_port}/{mongo_db}")
        messages.append(f"Collection: anagrafiche, Query: {query}")
        messages.append(f"Poll interval: {poll_interval_sec}s, Stability timeout: {stability_timeout_min}min, Max wait: {max_wait_min}min")
        messages.append("-" * 60)

        while True:
            poll_count += 1
            elapsed_sec = time.time() - start_time

            # Check max wait time
            if elapsed_sec > max_wait_sec:
                msg = "\n".join(messages)
                msg += f"\n\nERROR: Maximum wait time ({max_wait_min} minutes) exceeded"
                msg += f"\nFinal count: {last_count}, Expected: {expected_count}"
                return False, msg

            # Query MongoDB
            try:
                actual_count = collection.count_documents(query)
                poll_msg = f"Poll #{poll_count} ({elapsed_sec:.0f}s): {mongo_db}.anagrafiche query={query} count={actual_count}"
                messages.append(poll_msg)
                if _logger:
                    _logger.info(poll_msg)

                # Check if we reached expected count
                if actual_count == expected_count:
                    msg = "\n".join(messages)
                    msg += f"\n\nSUCCESS: Document count matches expected count ({expected_count})"
                    msg += f"\nTotal time: {elapsed_sec:.1f}s ({poll_count} polls)"
                    return True, msg

                # Check for stability (count hasn't changed)
                if last_count is not None and actual_count == last_count:
                    time_stable = time.time() - last_count_time
                    if time_stable >= stability_timeout_sec:
                        msg = "\n".join(messages)
                        msg += f"\n\nERROR: Document count stable at {actual_count} for {stability_timeout_min} minutes"
                        msg += f"\nExpected: {expected_count}, Actual: {actual_count}, Difference: {actual_count - expected_count}"
                        return False, msg
                else:
                    # Count changed, reset stability timer
                    last_count = actual_count
                    last_count_time = time.time()

            except (OperationFailure, Exception) as e:
                messages.append(f"Poll #{poll_count}: Query error: {e}")

            # Wait before next poll
            time.sleep(poll_interval_sec)

    except ConnectionFailure as e:
        return False, f"Failed to connect to MongoDB: {e}"
    except Exception as e:
        return False, f"Unexpected error: {e}"
    # No finally block - connection pool manages lifecycle


def verify_mongodb_count_nonzero_with_polling(
    rata: str,
    mongo_config: dict,
    poll_interval_sec: int = 30,
    stability_timeout_min: int = 5,
    max_wait_min: int = 60
) -> Tuple[bool, str]:
    """
    Post-check: verify that MongoDB document count becomes > 0.
    Polls until count > 0 (success), or stays 0 for stability_timeout_min, or exceeds max_wait_min.
    """
    mongo_host = mongo_config.get('host', 'localhost')
    mongo_port = mongo_config.get('port', 27017)
    mongo_db = mongo_config.get('database', 'your_database')

    # SECURITY: Validate inputs to prevent MongoDB injection
    if not validate_rata_format(rata):
        return False, f"Invalid RATA format: {rata} (expected YYYYMM)"

    try:
        rata = sanitize_query_param(rata, "rata")
    except ValueError as e:
        return False, f"Parameter validation failed: {e}"

    messages = []
    messages.append("Expected document count: > 0 (non-zero)")
    messages.append(f"MongoDB: {mongo_host}:{mongo_port}/{mongo_db}")
    messages.append(f"Collection: anagrafiche")
    messages.append(f"Poll interval: {poll_interval_sec}s, Stability timeout: {stability_timeout_min}min, Max wait: {max_wait_min}min")
    messages.append("-" * 60)

    client = None
    try:
        client = get_mongo_client(mongo_config)

        db = client[mongo_db]
        collection = db['anagrafiche']

        start_time = time.time()
        max_wait_sec = max_wait_min * 60
        stability_timeout_sec = stability_timeout_min * 60

        last_change_time = start_time
        poll_count = 0

        while True:
            poll_count += 1
            elapsed_sec = time.time() - start_time

            if elapsed_sec > max_wait_sec:
                msg = "\n".join(messages)
                msg += f"\n\nERROR: Maximum wait time ({max_wait_min} minutes) exceeded"
                msg += f"\nFinal count: 0, Expected: > 0"
                return False, msg

            try:
                query = {
                    "rataEmissione": rata,
                    "tipoSpesa": "SPT"
                }
                actual_count = collection.count_documents(query)
                poll_msg = f"Poll #{poll_count} ({elapsed_sec:.0f}s): {mongo_db}.anagrafiche query={query} count={actual_count}"
                messages.append(poll_msg)
                if _logger:
                    _logger.info(poll_msg)

                if actual_count > 0:
                    msg = "\n".join(messages)
                    msg += f"\n\nSUCCESS: Document count is > 0 (actual: {actual_count})"
                    msg += f"\nTotal time: {elapsed_sec:.1f}s ({poll_count} polls)"
                    return True, msg

                time_stable = time.time() - last_change_time
                if time_stable >= stability_timeout_sec:
                    msg = "\n".join(messages)
                    msg += f"\n\nERROR: Document count stable at 0 for {stability_timeout_min} minutes"
                    msg += f"\nExpected: > 0, Actual: {actual_count}"
                    return False, msg

            except (OperationFailure, Exception) as e:
                messages.append(f"Poll #{poll_count}: Query error: {e}")

            time.sleep(poll_interval_sec)

    except ConnectionFailure as e:
        return False, f"Failed to connect to MongoDB: {e}"
    except Exception as e:
        return False, f"Unexpected error: {e}"
    # No finally block - connection pool manages lifecycle


def verify_anasti_data_quality_with_polling(
    rata: str,
    expected_count: int,
    mongo_config: dict,
    poll_interval_sec: int = 30,
    stability_timeout_min: int = 5,
    max_wait_min: int = 60
) -> Tuple[bool, str]:
    """
    Post-check for ANASTI: Wait for correct document count, then verify data quality.

    PHASE 1 - Document Count Polling:
    Polls MongoDB periodically until document count matches expected.
    - Count matches expected -> proceed to PHASE 2 (quality checks)
    - Count doesn't change for stability_timeout_min minutes -> FAILURE
    - Total wait time exceeds max_wait_min minutes -> FAILURE

    PHASE 2 - Quality Checks (only if count is correct):
    Executes detailed data quality checks. If count is not correct, this phase is NEVER executed.

    Args:
        rata: RATA value (YYYYMM format)
        expected_count: Expected number of documents
        mongo_config: MongoDB configuration dict
        poll_interval_sec: Seconds between polls (default: 30)
        stability_timeout_min: Minutes of stability before giving up (default: 5)
        max_wait_min: Maximum total wait time in minutes (default: 60)

    Returns:
        Tuple of (success: bool, message: str)
    """
    import time

    mongo_host = mongo_config.get('host', 'localhost')
    mongo_port = mongo_config.get('port', 27017)
    mongo_db = mongo_config.get('database', 'your_database')
    mongo_user = mongo_config.get('user', '')
    mongo_pass = mongo_config.get('password', '')

    # SECURITY: Validate inputs to prevent MongoDB injection
    if not validate_rata_format(rata):
        return False, f"Invalid RATA format: {rata} (expected YYYYMM)"

    try:
        rata = sanitize_query_param(rata, "rata")
    except ValueError as e:
        return False, f"Parameter validation failed: {e}"

    messages = []
    messages.append(f"Expected document count: {expected_count}")
    messages.append(f"MongoDB: {mongo_host}:{mongo_port}/{mongo_db}")
    messages.append(f"Collection: ANASTI-TEL-RIT")
    messages.append(f"Poll interval: {poll_interval_sec}s, Stability timeout: {stability_timeout_min}min, Max wait: {max_wait_min}min")
    messages.append("-" * 60)

    try:
        # Use connection pool with circuit breaker protection
        client = get_mongo_client(mongo_config)
        if client is None:
            return False, "Failed to get MongoDB client (circuit breaker may be OPEN)"

        db = client[mongo_db]
        collection = db['ANASTI-TEL-RIT']

        # ===== PHASE 1: Document Count Polling =====
        messages.append("PHASE 1: Waiting for correct document count...")

        start_time = time.time()
        max_wait_sec = max_wait_min * 60
        stability_timeout_sec = stability_timeout_min * 60

        last_count = -1
        last_change_time = start_time
        poll_count = 0

        while True:
            poll_count += 1
            elapsed_sec = time.time() - start_time

            # Build query
            query = {
                "rataEmissione": rata,
                "tipoSpesa": "SPT"
            }

            # Simple count query
            total_count = collection.count_documents(query)

            # Track count changes for stability detection
            if total_count != last_count:
                last_count = total_count
                last_change_time = time.time()

            # Log poll result
            poll_msg = f"Poll #{poll_count} ({int(elapsed_sec)}s): {total_count} documents"
            messages.append(poll_msg)
            if _logger:
                _logger.info(poll_msg)

            # SUCCESS: count matches expected -> proceed to PHASE 2
            if total_count == expected_count:
                messages.append("")
                messages.append(f"✓ Document count matches expected ({expected_count})")
                messages.append(f"Total polling time: {elapsed_sec:.1f}s ({poll_count} polls)")
                messages.append("")
                break

            # FAILURE: stability timeout (count hasn't changed)
            time_since_change = time.time() - last_change_time
            if time_since_change >= stability_timeout_sec:
                messages.append("")
                messages.append(f"FAILURE: Document count stable at {total_count} for {stability_timeout_min} min")
                messages.append(f"Expected: {expected_count}, Got: {total_count}")
                messages.append("❌ Document count mismatch is a critical error - quality checks not executed")
                return False, "\n".join(messages)

            # FAILURE: max wait timeout
            if elapsed_sec >= max_wait_sec:
                messages.append("")
                messages.append(f"FAILURE: Maximum wait time ({max_wait_min} min) exceeded")
                messages.append(f"Expected: {expected_count}, Got: {total_count}")
                messages.append("❌ Document count mismatch is a critical error - quality checks not executed")
                return False, "\n".join(messages)

            # Continue polling
            time.sleep(poll_interval_sec)

        # ===== PHASE 2: Data Quality Checks =====
        messages.append("PHASE 2: Executing data quality checks...")

        # Build base match query
        base_query = {
            "rataEmissione": rata,
            "tipoSpesa": "SPT"
        }

        # Execute aggregate query with all quality checks
        # NOTE: pg and iban are in the vociAddebito array; ibanAccredito is at document root
        pipeline = [
            {
                "$facet": {
                    "countA": [
                        {"$match": base_query},
                        {"$unwind": "$vociAddebito"},
                        {"$match": {"vociAddebito.pg": ""}},
                        {"$count": "total"}
                    ],
                    "countB": [
                        {"$match": base_query},
                        {"$unwind": "$vociAddebito"},
                        {"$match": {"vociAddebito.pg": {"$exists": False}}},
                        {"$count": "total"}
                    ],
                    "countC": [
                        {"$match": base_query},
                        {"$unwind": "$vociAddebito"},
                        {"$match": {"vociAddebito.iban": ""}},
                        {"$count": "total"}
                    ],
                    "countD": [
                        {"$match": base_query},
                        {"$unwind": "$vociAddebito"},
                        {"$match": {"vociAddebito.iban": {"$exists": False}}},
                        {"$count": "total"}
                    ],
                    "countE": [
                        {"$match": {**base_query, "ibanAccredito": ""}},
                        {"$count": "total"}
                    ],
                    "countF": [
                        {"$match": {**base_query, "ibanAccredito": {"$exists": False}}},
                        {"$count": "total"}
                    ],
                    "countG": [
                        {"$match": base_query},
                        {
                            "$addFields": {
                                "importoAnastiNum": {"$toDecimal": "$importoAnasti"},
                                "vociAddebitoNum": {
                                    "$map": {
                                        "input": "$vociAddebito",
                                        "as": "voce",
                                        "in": {"$toDecimal": "$$voce.importoVoceAddebito"}
                                    }
                                }
                            }
                        },
                        {
                            "$addFields": {
                                "sommaVociAddebito": {"$sum": "$vociAddebitoNum"}
                            }
                        },
                        {
                            "$match": {
                                "$expr": {"$ne": ["$sommaVociAddebito", "$importoAnastiNum"]}
                            }
                        },
                        {"$count": "total"}
                    ],
                    "countH": [
                        {"$match": {**base_query, "imputazioni": {"$exists": True}}},
                        {
                            "$addFields": {
                                "importoAnastiNum": {"$toDecimal": "$importoAnasti"},
                                "imputazioniNum": {
                                    "$map": {
                                        "input": "$imputazioni",
                                        "as": "imp",
                                        "in": {"$toDecimal": "$$imp.importoImputazione"}
                                    }
                                }
                            }
                        },
                        {
                            "$addFields": {
                                "sommaImputazioni": {"$sum": "$imputazioniNum"}
                            }
                        },
                        {
                            "$match": {
                                "$expr": {"$ne": ["$sommaImputazioni", "$importoAnastiNum"]}
                            }
                        },
                        {"$count": "total"}
                    ]
                }
            }
        ]

        result = list(collection.aggregate(pipeline))
        if not result:
            return False, "\n".join(messages) + "\n\nFAILURE: Aggregate query returned no results"

        facets = result[0]

        # Extract quality check counts (handle empty lists from facet queries)
        count_pg_empty = facets.get('countA', [{}])[0].get('total', 0) if facets.get('countA') else 0
        count_pg_not_exists = facets.get('countB', [{}])[0].get('total', 0) if facets.get('countB') else 0
        count_iban_empty = facets.get('countC', [{}])[0].get('total', 0) if facets.get('countC') else 0
        count_iban_not_exists = facets.get('countD', [{}])[0].get('total', 0) if facets.get('countD') else 0
        count_iban_accredito_empty = facets.get('countE', [{}])[0].get('total', 0) if facets.get('countE') else 0
        count_iban_accredito_not_exists = facets.get('countF', [{}])[0].get('total', 0) if facets.get('countF') else 0
        count_importo_mismatch = facets.get('countG', [{}])[0].get('total', 0) if facets.get('countG') else 0
        count_imputazioni_mismatch = facets.get('countH', [{}])[0].get('total', 0) if facets.get('countH') else 0

        messages.append("")
        messages.append("Data Quality Check Results:")
        messages.append(f"  - pg empty (in vociAddebito): {count_pg_empty}")
        messages.append(f"  - pg not exists (in vociAddebito): {count_pg_not_exists}")
        messages.append(f"  - iban empty (in vociAddebito): {count_iban_empty}")
        messages.append(f"  - iban not exists (in vociAddebito): {count_iban_not_exists}")
        messages.append(f"  - ibanAccredito empty (at root): {count_iban_accredito_empty}")
        messages.append(f"  - ibanAccredito not exists (at root): {count_iban_accredito_not_exists}")
        messages.append(f"  - importo mismatch (sum vociAddebito != importoAnasti): {count_importo_mismatch}")
        messages.append(f"  - importo mismatch (sum imputazioni != importoAnasti): {count_imputazioni_mismatch}")

        # Check for quality errors
        errors = []
        if count_pg_empty > 0:
            errors.append(f"{count_pg_empty} voci addebito with empty pg")
        if count_pg_not_exists > 0:
            errors.append(f"{count_pg_not_exists} voci addebito with missing pg")
        if count_iban_empty > 0:
            errors.append(f"{count_iban_empty} voci addebito with empty iban")
        if count_iban_not_exists > 0:
            errors.append(f"{count_iban_not_exists} voci addebito with missing iban")
        if count_iban_accredito_empty > 0:
            errors.append(f"{count_iban_accredito_empty} documents with empty ibanAccredito")
        if count_iban_accredito_not_exists > 0:
            errors.append(f"{count_iban_accredito_not_exists} documents with missing ibanAccredito")
        if count_importo_mismatch > 0:
            errors.append(f"{count_importo_mismatch} documents where sum of vociAddebito != importoAnasti")
        if count_imputazioni_mismatch > 0:
            errors.append(f"{count_imputazioni_mismatch} documents where sum of imputazioni != importoAnasti")

        if errors:
            messages.append("")
            messages.append("FAILURE: Quality check errors found:")
            for error in errors:
                messages.append(f"  ❌ {error}")
            return False, "\n".join(messages)

        messages.append("")
        messages.append("✅ SUCCESS: All quality checks passed")
        return True, "\n".join(messages)

    except ConnectionFailure as e:
        return False, f"Failed to connect to MongoDB: {e}"
    except OperationFailure as e:
        return False, f"MongoDB operation failed: {e}"
    except Exception as e:
        return False, f"Unexpected error: {e}"


def verify_22000x_data_with_polling(
    rata: str,
    expected_count: int,
    mongo_config: dict,
    remote_home: str,
    tipo_flusso: str,
    progressivo_speciale: Optional[int] = None,
    poll_interval_sec: int = 30,
    stability_timeout_min: int = 5,
    max_wait_min: int = 60
) -> Tuple[bool, str]:
    """
    Post-check for FILE_22000X with polling: Wait for correct document count, then verify data quality.

    NOTE: progressivo_speciale parameter is accepted for API consistency but NOT used in queries.
    The OPI-Disposizioni collection does not have a progressivoSpeciale field.
    Differentiation between SPECIALE1/2/3 is handled by filename patterns from parametri_firma_digitale.txt.

    PHASE 1 - Document Count Polling:
    Polls MongoDB periodically until document count matches expected.
    - Count matches expected -> proceed to PHASE 2 (quality checks)
    - Count doesn't change for stability_timeout_min minutes -> FAILURE
    - Total wait time exceeds max_wait_min minutes -> FAILURE

    PHASE 2 - Quality Checks (only if count is correct):
    Executes detailed data quality checks on OPI-Disposizioni:
    - Verifies importoVoceAddebito > 0
    - Verifies importoAddebito > 0
    - Checks IBAN addebito (not empty, exists)
    - Checks IBAN accredito (not empty, exists)
    - Verifies balance: importo = sum of vociAddebito.importoVoceAddebito

    Args:
        rata: RATA value (YYYYMM format)
        expected_count: Expected number of documents
        mongo_config: MongoDB configuration dict
        remote_home: Path to REMOTE_HOME directory
        tipo_flusso: Tipo flusso (ORDINARIA, SPECIALE, RITENUTE)
        poll_interval_sec: Seconds between polls (default: 30)
        stability_timeout_min: Minutes of stability before giving up (default: 5)
        max_wait_min: Maximum total wait time in minutes (default: 60)

    Returns:
        Tuple of (success: bool, message: str)
    """
    import time

    mongo_host = mongo_config.get('host', 'localhost')
    mongo_port = mongo_config.get('port', 27017)
    mongo_db = mongo_config.get('database', 'your_database')
    mongo_user = mongo_config.get('user', '')
    mongo_pass = mongo_config.get('password', '')

    # SECURITY: Validate inputs to prevent MongoDB injection
    if not validate_rata_format(rata):
        return False, f"Invalid RATA format: {rata} (expected YYYYMM)"

    if not validate_tipo_flusso(tipo_flusso):
        return False, f"Invalid tipoFlusso: {tipo_flusso} (expected ORDINARIA, SPECIALE, RITENUTE, or SINIIS)"

    try:
        rata = sanitize_query_param(rata, "rata")
        tipo_flusso = sanitize_query_param(tipo_flusso, "tipo_flusso")
        if progressivo_speciale is not None:
            progressivo_speciale = sanitize_query_param(progressivo_speciale, "progressivo_speciale")
    except ValueError as e:
        return False, f"Parameter validation failed: {e}"

    # Extract month from RATA (already validated above)
    if len(rata) != 6:
        return False, f"Invalid RATA format: {rata} (expected YYYYMM)"

    # month = rata[4:6]  # Not needed - using literal "mese" directory

    # Read parametri_firma_digitale.txt to extract filenames
    files_dir = Path(remote_home) / "elaborazioni" / "files" / "mese"
    param_file = files_dir / "parametri_firma_digitale.txt"

    if not param_file.exists():
        return False, f"File not found: {param_file}"

    # Extract filenames from parametri_firma_digitale.txt
    filenames = []
    try:
        with open(param_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line.startswith('FILE='):
                    file_path_str = line[5:]  # Remove 'FILE='
                    filename = Path(file_path_str).name
                    filenames.append(filename)
    except Exception as e:
        return False, f"Failed to read {param_file}: {e}"

    if not filenames:
        return False, f"No FILE= entries found in {param_file}"

    messages = []
    messages.append(f"Expected document count: {expected_count}")
    messages.append(f"MongoDB: {mongo_host}:{mongo_port}/{mongo_db}")
    messages.append(f"Collection: OPI-Disposizioni")
    messages.append(f"TipoFlusso: {tipo_flusso}")
    messages.append(f"Files to check: {', '.join(filenames)}")
    messages.append(f"Poll interval: {poll_interval_sec}s, Stability timeout: {stability_timeout_min}min, Max wait: {max_wait_min}min")
    messages.append("-" * 60)

    try:
        # Use connection pool with circuit breaker protection
        client = get_mongo_client(mongo_config)
        if client is None:
            return False, "Failed to get MongoDB client (circuit breaker may be OPEN)"

        db = client[mongo_db]
        collection = db['OPI-Disposizioni']

        # ===== PHASE 1: Document Count Polling =====
        messages.append("PHASE 1: Waiting for correct document count...")

        start_time = time.time()
        max_wait_sec = max_wait_min * 60
        stability_timeout_sec = stability_timeout_min * 60

        last_count = -1
        last_change_time = start_time
        poll_count = 0

        while True:
            poll_count += 1
            elapsed_sec = time.time() - start_time

            # Build query with $or for all filenames
            or_conditions = []
            for filename in filenames:
                or_conditions.append({"nomeFile": {"$regex": filename}})

            query = {
                "rataEmissione": rata,
                "tipoFlusso": tipo_flusso,
                "specieTitolo": "07",
                "$or": or_conditions
            }

            # NOTE: progressivoSpeciale field does NOT exist in OPI-Disposizioni collection
            # Differentiation between SPECIALE1/2/3 is handled by filename patterns in $or conditions

            # Simple count query
            total_count = collection.count_documents(query)

            # Track count changes for stability detection
            if total_count != last_count:
                last_count = total_count
                last_change_time = time.time()

            # Log poll result
            poll_msg = f"Poll #{poll_count} ({int(elapsed_sec)}s): {total_count} documents"
            messages.append(poll_msg)
            if _logger:
                _logger.info(poll_msg)

            # SUCCESS: count matches expected -> proceed to PHASE 2
            if total_count == expected_count:
                messages.append("")
                messages.append(f"✓ Document count matches expected ({expected_count})")
                messages.append(f"Total polling time: {elapsed_sec:.1f}s ({poll_count} polls)")
                messages.append("")
                break

            # FAILURE: stability timeout (count hasn't changed)
            time_since_change = time.time() - last_change_time
            if time_since_change >= stability_timeout_sec:
                messages.append("")
                messages.append(f"FAILURE: Document count stable at {total_count} for {stability_timeout_min} min")
                messages.append(f"Expected: {expected_count}, Got: {total_count}")
                messages.append("❌ Document count mismatch")
                return False, "\n".join(messages)

            # FAILURE: max wait timeout
            if elapsed_sec >= max_wait_sec:
                messages.append("")
                messages.append(f"FAILURE: Maximum wait time ({max_wait_min} min) exceeded")
                messages.append(f"Expected: {expected_count}, Got: {total_count}")
                messages.append("❌ Document count mismatch")
                return False, "\n".join(messages)

            # Continue polling
            time.sleep(poll_interval_sec)

        # ===== PHASE 2: Data Quality Checks =====
        messages.append("PHASE 2: Executing data quality checks...")

        # Build base match query with filenames
        or_conditions = []
        for filename in filenames:
            or_conditions.append({"nomeFile": {"$regex": filename}})

        base_query = {
            "rataEmissione": rata,
            "tipoFlusso": tipo_flusso,
            "specieTitolo": "07",
            "$or": or_conditions
        }

        # Execute aggregate query with all quality checks
        pipeline = [
            {
                "$facet": {
                    "countimportoVoceAddebitoNEG": [
                        {"$match": {**base_query, "datiDisposizione.ordinativo.addebito.vociAddebito.importoVoceAddebito": {"$lte": 0}}},
                        {"$count": "total"}
                    ],
                    "countimportoAddebitoNEG": [
                        {"$match": {**base_query, "datiDisposizione.ordinativo.addebito.importoAddebito": {"$lte": 0}}},
                        {"$count": "total"}
                    ],
                    "countIbanAddebitoVuoto": [
                        {"$match": {**base_query, "datiDisposizione.ordinativo.addebito.vociAddebito.contoAddebito.iban": ""}},
                        {"$count": "total"}
                    ],
                    "countIbanAddebitoNotExists": [
                        {"$match": {**base_query, "datiDisposizione.ordinativo.addebito.vociAddebito.contoAddebito.iban": {"$exists": False}}},
                        {"$count": "total"}
                    ],
                    "countIbanAccreditoVuoto": [
                        {"$match": {**base_query, "datiDisposizione.ordinativo.accredito.contoAccredito.contoIban.iban": ""}},
                        {"$count": "total"}
                    ],
                    "countIbanAccreditoNotExists": [
                        {"$match": {**base_query, "datiDisposizione.ordinativo.accredito.contoAccredito.contoIban.iban": {"$exists": False}}},
                        {"$count": "total"}
                    ],
                    "countQuadraturaNonCorretta": [
                        {"$match": {**base_query, "datiDisposizione.ordinativo.addebito.vociAddebito": {"$type": "array", "$ne": []}}},
                        {
                            "$addFields": {
                                "sommaAddebiti": {
                                    "$round": [
                                        {
                                            "$sum": {
                                                "$map": {
                                                    "input": "$datiDisposizione.ordinativo.addebito.vociAddebito",
                                                    "as": "voce",
                                                    "in": {
                                                        "$convert": {
                                                            "input": "$$voce.importoVoceAddebito",
                                                            "to": "decimal",
                                                            "onError": 0,
                                                            "onNull": 0
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        2
                                    ]
                                },
                                "importoDispoRounded": {
                                    "$round": [
                                        {
                                            "$convert": {
                                                "input": "$importo",
                                                "to": "decimal",
                                                "onError": 0,
                                                "onNull": 0
                                            }
                                        },
                                        2
                                    ]
                                }
                            }
                        },
                        {
                            "$match": {
                                "$expr": {
                                    "$ne": ["$importoDispoRounded", "$sommaAddebiti"]
                                }
                            }
                        },
                        {"$count": "total"}
                    ]
                }
            }
        ]

        result = list(collection.aggregate(pipeline))
        if not result:
            return False, "\n".join(messages) + "\n\nFAILURE: Aggregate query returned no results"

        facets = result[0]

        # Extract quality check counts (handle empty lists from facet queries)
        impo_voce_neg = facets.get('countimportoVoceAddebitoNEG', [{}])[0].get('total', 0) if facets.get('countimportoVoceAddebitoNEG') else 0
        impo_add_neg = facets.get('countimportoAddebitoNEG', [{}])[0].get('total', 0) if facets.get('countimportoAddebitoNEG') else 0
        iban_add_empty = facets.get('countIbanAddebitoVuoto', [{}])[0].get('total', 0) if facets.get('countIbanAddebitoVuoto') else 0
        iban_add_not_exists = facets.get('countIbanAddebitoNotExists', [{}])[0].get('total', 0) if facets.get('countIbanAddebitoNotExists') else 0
        iban_acc_empty = facets.get('countIbanAccreditoVuoto', [{}])[0].get('total', 0) if facets.get('countIbanAccreditoVuoto') else 0
        iban_acc_not_exists = facets.get('countIbanAccreditoNotExists', [{}])[0].get('total', 0) if facets.get('countIbanAccreditoNotExists') else 0
        quadratura_err = facets.get('countQuadraturaNonCorretta', [{}])[0].get('total', 0) if facets.get('countQuadraturaNonCorretta') else 0

        messages.append("")
        messages.append("Data Quality Check Results:")
        messages.append(f"  - importoVoceAddebito <= 0: {impo_voce_neg}")
        messages.append(f"  - importoAddebito <= 0: {impo_add_neg}")
        messages.append(f"  - IBAN addebito empty: {iban_add_empty}")
        messages.append(f"  - IBAN addebito not exists: {iban_add_not_exists}")
        messages.append(f"  - IBAN accredito empty: {iban_acc_empty}")
        messages.append(f"  - IBAN accredito not exists: {iban_acc_not_exists}")
        messages.append(f"  - Quadratura non corretta (importo != somma voci): {quadratura_err}")

        # Check for quality errors
        errors = []
        if impo_voce_neg > 0:
            errors.append(f"{impo_voce_neg} documents with importoVoceAddebito <= 0")
        if impo_add_neg > 0:
            errors.append(f"{impo_add_neg} documents with importoAddebito <= 0")
        if iban_add_empty > 0:
            errors.append(f"{iban_add_empty} documents with empty IBAN addebito")
        if iban_add_not_exists > 0:
            errors.append(f"{iban_add_not_exists} documents with missing IBAN addebito")
        if iban_acc_empty > 0:
            errors.append(f"{iban_acc_empty} documents with empty IBAN accredito")
        if iban_acc_not_exists > 0:
            errors.append(f"{iban_acc_not_exists} documents with missing IBAN accredito")
        if quadratura_err > 0:
            errors.append(f"{quadratura_err} documents with balance mismatch (importo != sum of vociAddebito)")

        if errors:
            messages.append("")
            messages.append("FAILURE: Quality check errors found:")
            for error in errors:
                messages.append(f"  ❌ {error}")
            return False, "\n".join(messages)

        messages.append("")
        messages.append("✅ SUCCESS: All quality checks passed")
        return True, "\n".join(messages)

    except ConnectionFailure as e:
        return False, f"Failed to connect to MongoDB: {e}"
    except OperationFailure as e:
        return False, f"MongoDB operation failed: {e}"
    except Exception as e:
        return False, f"Unexpected error: {e}"
