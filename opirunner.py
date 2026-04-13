#!/usr/bin/env python3
import argparse
import atexit
import json
import logging
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Set

# Importa modulo di verifica MongoDB
try:
    from mongodb_checker import (
        check_files_and_count_lines,
        check_emisti_files_and_count_lines,
        check_emisti_speciale_files_and_count_lines,
        check_anasti_file_and_count_lines,
        check_22000x_files_and_count_lines,
        verify_mongodb_count_with_polling,
        verify_mongodb_count_nonzero_with_polling,
        verify_emisti_data_quality,
        verify_emisti_data_quality_with_polling,
        verify_anasti_data_quality_with_polling,
        verify_22000x_data_with_polling,
        close_all_mongo_clients,
        set_logger as set_mongodb_logger
    )
    MONGODB_CHECKER_AVAILABLE = True
except ImportError:
    MONGODB_CHECKER_AVAILABLE = False
    close_all_mongo_clients = None
    set_mongodb_logger = None

# Importa metriche e tracking del progresso (opzionale ma consigliato)
try:
    from metrics import MetricsCollector, get_metrics_collector, reset_metrics_collector
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False
    MetricsCollector = None
    get_metrics_collector = None
    reset_metrics_collector = None

try:
    from progress_tracker import ProgressTracker, get_progress_tracker, reset_progress_tracker
    PROGRESS_TRACKER_AVAILABLE = True
except ImportError:
    PROGRESS_TRACKER_AVAILABLE = False
    ProgressTracker = None
    get_progress_tracker = None
    reset_progress_tracker = None

# Importa validazione JSON Schema (opzionale)
try:
    import jsonschema
    JSONSCHEMA_AVAILABLE = True
except ImportError:
    JSONSCHEMA_AVAILABLE = False

# Importa caricatore dipendenze per esecuzione a layer indipendenti (opzionale)
try:
    from dependency_loader import build_dependencies_from_config
    DEPENDENCY_LOADER_AVAILABLE = True
except ImportError:
    DEPENDENCY_LOADER_AVAILABLE = False
    build_dependencies_from_config = None

# Versione
__version__ = "1.3.0"

# Costanti
MIN_YEAR = 2000
POSTCHECK_TIMEOUT_MIN = 10
TEMP_SCRIPT_PREFIX = 'opirunner-'
KILL_TIMEOUT_SEC = 5
CLEANUP_TIMEOUT_SEC = 1
VALID_SHELLS = {'powershell', 'cmd', 'sh'}
MAX_RETRIES = 3
RETRY_INITIAL_DELAY_SEC = 5
RETRY_BACKOFF_MULTIPLIER = 2
LOG_RETENTION_DAYS = 30  # Keep logs for 30 days

# Cache regex
_regex_cache: Dict[str, re.Pattern] = {}

# Gestore globale di shutdown
_shutdown_handler: Optional['GracefulShutdown'] = None


# ==============================================================================
# GESTORE SHUTDOWN GRACEFUL (Affidabilità: Pulizia risorse sui segnali)
# ==============================================================================

class GracefulShutdown:
    """Handle graceful shutdown on SIGTERM/SIGINT signals"""

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.shutdown_requested = False
        self._cleaned_up = False
        self._sigint_count = 0
        self.running_processes = []  # Track active subprocesses

        # Registra gestori di segnale
        signal.signal(signal.SIGTERM, self.handle_signal)
        signal.signal(signal.SIGINT, self.handle_signal)

        # Registra gestore atexit per uscita normale
        atexit.register(self.cleanup)

    def register_process(self, proc):
        """Register a running subprocess for cleanup"""
        self.running_processes.append(proc)

    def unregister_process(self, proc):
        """Unregister a subprocess when it completes"""
        if proc in self.running_processes:
            self.running_processes.remove(proc)

    def handle_signal(self, signum, frame):
        """Handle shutdown signals"""
        try:
            sig_name = signal.Signals(signum).name
        except (ValueError, AttributeError):
            sig_name = f"Signal {signum}"

        # Gestisce doppio Ctrl+C per uscita immediata
        if signum == signal.SIGINT:
            self._sigint_count += 1
            if self._sigint_count >= 2:
                self.logger.error("Received second SIGINT, forcing immediate exit!")
                # Termina immediatamente tutti i processi in esecuzione
                for proc in self.running_processes:
                    try:
                        proc.kill()
                    except Exception:
                        pass
                sys.exit(130)  # 128 + SIGINT(2)

        if self.shutdown_requested:
            return  # Already handling shutdown

        self.logger.warning(f"Received {sig_name}, terminating processes... (Press Ctrl+C again to force exit)")
        self.shutdown_requested = True

        # Termina immediatamente tutti i processi in esecuzione
        for proc in self.running_processes:
            try:
                self.logger.info(f"Terminating process PID {proc.pid}...")
                proc.terminate()
            except Exception as e:
                self.logger.debug(f"Error terminating process: {e}")

        self.cleanup()

        # Esci con codice specifico del segnale
        sys.exit(128 + signum)

    def cleanup(self):
        """Cleanup resources"""
        if self._cleaned_up:
            return  # Already cleaned up

        self._cleaned_up = True
        self.logger.info("Cleaning up resources...")

        # Chiudi connessioni MongoDB
        if MONGODB_CHECKER_AVAILABLE and close_all_mongo_clients:
            try:
                close_all_mongo_clients()
                self.logger.info("MongoDB connections closed")
            except Exception as e:
                self.logger.error(f"Error closing MongoDB: {e}")

        # Pulisci file temporanei
        try:
            import glob
            temp_pattern = os.path.join(tempfile.gettempdir(), f'{TEMP_SCRIPT_PREFIX}*')
            temp_dirs = glob.glob(temp_pattern)
            for temp_dir in temp_dirs:
                try:
                    if os.path.isdir(temp_dir):
                        shutil.rmtree(temp_dir, ignore_errors=True)
                except Exception as e:
                    self.logger.debug(f"Could not remove temp dir {temp_dir}: {e}")
            if temp_dirs:
                self.logger.info(f"Cleaned {len(temp_dirs)} temp directories")
        except Exception as e:
            self.logger.error(f"Error cleaning temp files: {e}")


# ==============================================================================
# SANITIZZAZIONE LOG (Sicurezza: Previene perdita credenziali nei log)
# ==============================================================================

def sanitize_for_logging(text: str) -> str:
    """Remove sensitive data from log messages.

    Args:
        text: Text to sanitize

    Returns:
        Sanitized text with credentials redacted
    """
    if not isinstance(text, str):
        return str(text)

    patterns = [
        # Flag password: -p, --password, password=
        (r'(-p|--password|password=)\s*\S+', r'\1 ***REDACTED***'),
        # URI MongoDB con credenziali
        (r'(mongodb://[^:]+:)[^@]+(@)', r'\1***REDACTED***\2'),
        # Chiavi API (case insensitive)
        (r'(api[_-]?key[=:\s]+)\S+', r'\1***REDACTED***'),
        # Token (case insensitive)
        (r'(token[=:\s]+)\S+', r'\1***REDACTED***'),
        # Pattern password generici
        (r'(pwd|pass|secret)[=:\s]+\S+', r'\1=***REDACTED***'),
    ]

    sanitized = text
    for pattern, replacement in patterns:
        sanitized = re.sub(pattern, replacement, sanitized, flags=re.IGNORECASE)

    return sanitized


def retry_with_backoff(func, max_retries: int = MAX_RETRIES, initial_delay: float = RETRY_INITIAL_DELAY_SEC,
                       logger: Optional[logging.Logger] = None, operation_name: str = "operation"):
    """Execute function with exponential backoff retry logic.

    Args:
        func: Function to execute (should return Tuple[bool, Any])
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds between retries
        logger: Logger for retry messages
        operation_name: Name of operation for logging

    Returns:
        Result from func (success, value)
    """
    delay = initial_delay
    for attempt in range(1, max_retries + 1):
        try:
            success, result = func()
            if success:
                if attempt > 1 and logger:
                    logger.info(f"{operation_name} succeeded on attempt {attempt}/{max_retries}")
                return True, result

            # Operation failed but didn't raise exception
            if attempt < max_retries:
                if logger:
                    logger.warning(f"{operation_name} failed (attempt {attempt}/{max_retries}), retrying in {delay:.1f}s...")
                time.sleep(delay)
                delay *= RETRY_BACKOFF_MULTIPLIER
            else:
                if logger:
                    logger.error(f"{operation_name} failed after {max_retries} attempts")
                return False, result

        except Exception as e:
            if attempt < max_retries:
                if logger:
                    logger.warning(f"{operation_name} exception (attempt {attempt}/{max_retries}): {e}, retrying in {delay:.1f}s...")
                time.sleep(delay)
                delay *= RETRY_BACKOFF_MULTIPLIER
            else:
                if logger:
                    logger.error(f"{operation_name} exception after {max_retries} attempts: {e}")
                return False, str(e)

    return False, "Max retries exceeded"


@dataclass
class StepResult:
    """Result of executing a single step."""
    index: int
    name: str
    passed: bool
    exit_code: int
    timed_out: bool
    duration_sec: float
    reasons: List[str]
    stdout: str = ""
    stderr: str = ""
    precheck_details: str = ""
    postcheck_details: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for JSON serialization."""
        d = asdict(self)
        d['pass'] = d.pop('passed')  # Keep backwards compatibility
        d['exitCode'] = d.pop('exit_code')
        d['timedOut'] = d.pop('timed_out')
        d['durationSec'] = d.pop('duration_sec')
        d['precheckDetails'] = d.pop('precheck_details')
        d['postcheckDetails'] = d.pop('postcheck_details')
        return d


class ProgressiveReportWriter:
    """Writes detailed report progressively as steps complete."""

    def __init__(self, log_dir: Path, rata: str = None, logger: logging.Logger = None):
        self.logger = logger
        timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
        rata_suffix = f"_rata{rata}" if rata else ""
        self.report_file = log_dir / f"report{rata_suffix}_{timestamp}.txt"
        self.file_handle = None
        self.flows = {}
        self.results_count = {'total': 0, 'passed': 0, 'failed': 0, 'blocked': 0}

    def __enter__(self):
        """Open report file and write header."""
        try:
            self.file_handle = open(self.report_file, 'w', encoding='utf-8')
            self._write_header()
            if self.logger:
                self.logger.info(f"Progressive report initialized: {self.report_file}")
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Failed to open report file: {e}")
            self.file_handle = None
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Write summary and close file."""
        if self.file_handle:
            self._write_summary()
            self.file_handle.close()
            if self.logger:
                self.logger.info(f"Report saved: {self.report_file}")

    def _write_header(self):
        """Write report header."""
        if not self.file_handle:
            return
        self.file_handle.write("=" * 100 + "\n")
        self.file_handle.write("RESOCONTO ESECUZIONE PIPELINE (PROGRESSIVO)\n")
        self.file_handle.write(f"Inizio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        self.file_handle.write("=" * 100 + "\n\n")
        self.file_handle.flush()

    def write_step_result(self, result: StepResult):
        """Write a single step result immediately."""
        if not self.file_handle:
            return

        # Update counters
        self.results_count['total'] += 1
        if result.passed:
            self.results_count['passed'] += 1
        elif "Blocked" in str(result.reasons):
            self.results_count['blocked'] += 1
        else:
            self.results_count['failed'] += 1

        # Extract flow name
        parts = result.name.split(' - ')
        if len(parts) >= 2:
            flow = parts[0].strip()
            step_type = parts[1].strip()
        else:
            flow = "ALTRI"
            step_type = result.name

        # Write flow header if first step of this flow
        if flow not in self.flows:
            self.flows[flow] = True
            self.file_handle.write("─" * 100 + "\n")
            self.file_handle.write(f"FLUSSO: {flow}\n")
            self.file_handle.write("─" * 100 + "\n")

        # Write step details
        if result.passed:
            self.file_handle.write(f"\n  ✓ {step_type}\n")
            self.file_handle.write(f"    Stato: SUCCESSO\n")
            self.file_handle.write(f"    Durata: {result.duration_sec:.2f}s\n")

            if result.precheck_details:
                self.file_handle.write(f"\n    === DETTAGLI PRECHECK ===\n")
                for line in result.precheck_details.split('\n'):
                    self.file_handle.write(f"    {line}\n")

            if result.postcheck_details:
                self.file_handle.write(f"\n    === DETTAGLI POSTCHECK ===\n")
                for line in result.postcheck_details.split('\n'):
                    self.file_handle.write(f"    {line}\n")
        else:
            if "Blocked: dependency failed" in result.reasons:
                self.file_handle.write(f"\n  ⊘ {step_type}\n")
                self.file_handle.write(f"    Stato: NON ESEGUITO (dipendenza fallita)\n")
                self.file_handle.write(f"    Motivo: Uno step precedente necessario è fallito\n")
            else:
                self.file_handle.write(f"\n  ✗ {step_type}\n")
                self.file_handle.write(f"    Stato: FALLITO\n")
                self.file_handle.write(f"    Durata: {result.duration_sec:.2f}s\n")

                if result.reasons:
                    self.file_handle.write(f"    Controlli falliti:\n")
                    for reason in result.reasons:
                        if "preCheck failed" in reason:
                            self.file_handle.write(f"      - PreCheck: {reason.replace('preCheck failed: ', '')}\n")
                        elif "postCheck failed" in reason:
                            self.file_handle.write(f"      - PostCheck: {reason.replace('postCheck failed: ', '')}\n")
                        elif "exit code" in reason:
                            self.file_handle.write(f"      - Comando: {reason}\n")
                        else:
                            self.file_handle.write(f"      - {reason}\n")

                if result.precheck_details:
                    self.file_handle.write(f"\n    === DETTAGLI PRECHECK ===\n")
                    for line in result.precheck_details.split('\n'):
                        self.file_handle.write(f"    {line}\n")

                if result.postcheck_details:
                    self.file_handle.write(f"\n    === DETTAGLI POSTCHECK ===\n")
                    for line in result.postcheck_details.split('\n'):
                        self.file_handle.write(f"    {line}\n")

        self.file_handle.write("\n")
        self.file_handle.flush()  # Ensure immediate write to disk

    def _write_summary(self):
        """Write final summary."""
        if not self.file_handle:
            return
        self.file_handle.write("\n" + "=" * 100 + "\n")
        self.file_handle.write("RIEPILOGO GENERALE\n")
        self.file_handle.write("=" * 100 + "\n")
        self.file_handle.write(f"  Totale step: {self.results_count['total']}\n")
        self.file_handle.write(f"  ✓ Successo: {self.results_count['passed']}\n")
        self.file_handle.write(f"  ✗ Falliti: {self.results_count['failed']}\n")
        self.file_handle.write(f"  ⊘ Bloccati (dipendenze): {self.results_count['blocked']}\n")
        self.file_handle.write("=" * 100 + "\n")
        self.file_handle.write(f"Fine: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        self.file_handle.flush()


def ts() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def cleanup_old_logs(log_dir: Path, retention_days: int = LOG_RETENTION_DAYS) -> int:
    """Remove log files older than retention period.

    Args:
        log_dir: Directory containing log files
        retention_days: Number of days to retain logs

    Returns:
        Number of files deleted
    """
    if not log_dir.exists():
        return 0

    deleted_count = 0
    cutoff_time = time.time() - (retention_days * 24 * 3600)

    try:
        for log_file in log_dir.glob('run-*.log'):
            if log_file.is_file():
                # Check file modification time
                if log_file.stat().st_mtime < cutoff_time:
                    try:
                        log_file.unlink()
                        deleted_count += 1
                    except OSError:
                        pass  # Ignore errors deleting individual files
    except Exception:
        pass  # Ignore errors during cleanup

    return deleted_count


def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except (json.JSONDecodeError, IOError, OSError) as e:
        raise SystemExit(f"Error reading/parsing JSON '{path}': {e}")


def to_list(val: Any) -> List[Any]:
    if val is None:
        return []
    if isinstance(val, list):
        return val
    return [val]


VAR_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def expand_template(text: Optional[str], vars_: Dict[str, str], shell_escape: bool = True) -> Optional[str]:
    """Expand ${VAR} templates with optional shell escaping for security.

    Args:
        text: Template string to expand
        vars_: Variable dictionary
        shell_escape: If True, apply shlex.quote to expanded values (default: True)

    Returns:
        Expanded string with variables replaced
    """
    if text is None:
        return None
    def repl(m: re.Match) -> str:
        key = m.group(1)
        val = vars_.get(key)
        if val is None:
            return m.group(0)  # Keep placeholder if variable not found
        val_str = str(val)
        # Apply shell escaping to prevent command injection
        if shell_escape and val_str != m.group(0):
            return shlex.quote(val_str)
        return val_str
    return VAR_PATTERN.sub(repl, text)


def expand_list(values: Any, vars_: Dict[str, str], shell_escape: bool = False) -> List[str]:
    """Expand list of template strings. Note: shell_escape=False for expect patterns by default."""
    return [expand_template(str(v), vars_, shell_escape=shell_escape) for v in to_list(values)]


def build_scope(cfg_vars: Dict[str, Any], step_env: Dict[str, Any], index: int, name: str, config_dir: Path) -> Dict[str, str]:
    scope: Dict[str, str] = {}
    # eredita ambiente corrente
    scope.update({k: str(v) for k, v in os.environ.items()})
    # built-in (aggiunti prima per essere disponibili nelle variabili)
    scope['STEP_INDEX'] = str(index)
    scope['STEP_NAME'] = str(name)
    scope['CONFIG_DIR'] = str(config_dir)
    scope['WORKSPACE'] = str(Path.cwd())
    # variabili di configurazione (con espansione ricorsiva)
    if cfg_vars:
        # Espandi le variabili ricorsivamente fino a 10 iterazioni per gestire dipendenze circolari
        max_iterations = 10
        for iteration in range(max_iterations):
            changed = False
            for k, v in cfg_vars.items():
                v_str = str(v)
                expanded = expand_template(v_str, scope, shell_escape=False)
                if expanded != scope.get(k):
                    scope[k] = expanded
                    changed = True
            if not changed:
                break
    # override env dello step (come variabili per interpolazione)
    if step_env:
        for k, v in step_env.items():
            v_str = str(v)
            scope[k] = expand_template(v_str, scope, shell_escape=False)
    return scope


def construct_remote_home(run_command: str, scope: Dict[str, str]) -> Optional[str]:
    """Construct REMOTE_HOME path dynamically based on tipoFlusso and progressivoSpeciale.

    Parses the run command to extract -t (tipoFlusso) and -s (progressivoSpeciale) flags
    and constructs the appropriate path:
    - ORDINARIA: $HOME/ORDINARIA
    - RITENUTE: $HOME/RITENUTE
    - SPECIALE with -s SPECIALE1: $HOME/SPECIALE/SPECIALE1
    - SPECIALE with -s SPECIALE2: $HOME/SPECIALE/SPECIALE2

    Args:
        run_command: The command string to parse
        scope: Variable scope containing HOME

    Returns:
        Constructed REMOTE_HOME path or None if cannot be determined
    """
    if not run_command:
        return None

    # Estrai tipoFlusso (flag -t)
    tipo_flusso_match = re.search(r'-t\s+(\w+)', run_command)
    if not tipo_flusso_match:
        return None

    tipo_flusso = tipo_flusso_match.group(1)
    home = scope.get('HOME', os.environ.get('HOME', ''))

    if not home:
        return None

    # Gestisci caso SPECIALE con progressivoSpeciale (flag -s)
    if tipo_flusso == 'SPECIALE':
        progressivo_match = re.search(r'-s\s+(\w+)', run_command)
        if progressivo_match:
            progressivo = progressivo_match.group(1)
            return f"{home}/SPECIALE/{progressivo}"
        else:
            # SPECIALE senza flag -s, usa solo SPECIALE
            return f"{home}/SPECIALE"

    # Gestisci ORDINARIA e RITENUTE
    elif tipo_flusso in ['ORDINARIA', 'RITENUTE']:
        return f"{home}/{tipo_flusso}"

    return None


def parse_step_info(step_name: str) -> Optional[Dict[str, str]]:
    """Parse step name to extract flow type, file type, and optional rata.

    Expected formats:
    - "FLUSSO[-RATA] - Transfer FILE_TYPE"
    - "SINIIS[-RATA] - Caricamento SINIIS" (special case)

    Examples:
    - "ORDINARIA - Transfer FILE_UTILITY"
    - "SPECIALE1 - Transfer FILE_EMISTI"
    - "RITENUTE - Transfer FILE_22000X"
    - "ORDINARIA-202502 - Transfer FILE_UTILITY"
    - "SPECIALE1-202502 - Transfer FILE_EMISTI"
    - "SINIIS - Caricamento SINIIS"
    - "SINIIS-202503 - Caricamento SINIIS"

    Args:
        step_name: Name of the step

    Returns:
        Dict with 'flow', 'file_type', and 'rata' keys, or None if parsing fails
        'rata' will be None if not specified in the name
    """
    # Pattern per step Transfer: FLOW[-RATA] - Transfer FILE_TYPE
    match = re.match(r'^(ORDINARIA|SPECIALE\d+|RITENUTE)(?:-(\d+))?\s*-\s*Transfer\s+(.+)$', step_name)
    if match:
        return {
            'flow': match.group(1),
            'rata': match.group(2),  # Can be None
            'file_type': match.group(3)
        }

    # Pattern per step SINIIS: SINIIS[-RATA] - Caricamento SINIIS or RITENUTE[-RATA] - Caricamento SINIIS
    match_siniis = re.match(r'^(SINIIS|RITENUTE)(?:-(\d+))?\s*-\s*Caricamento\s+SINIIS$', step_name)
    if match_siniis:
        return {
            'flow': 'SINIIS',  # Always treat as SINIIS flow for dependency purposes
            'rata': match_siniis.group(2),  # Can be None
            'file_type': 'SINIIS'
        }

    return None


def build_dependencies(steps: List[Dict]) -> Dict[int, Set[int]]:
    """Build dependency graph for steps based on naming convention.

    Dependency rules:
    1. FILE_EMISTI depends on FILE_UTILITY of the same flow and rata
    2. FILE_22000X depends on FILE_EMISTI of the same flow and rata
    3. SPECIALE* FILE_UTILITY depends on ORDINARIA FILE_UTILITY (same rata)
    4. All RITENUTE steps depend on completion of ORDINARIA and all SPECIALE flows (same rata)

    Note: Steps with different ratas are independent and can run in parallel.

    Args:
        steps: List of step configurations

    Returns:
        Dictionary mapping step index to set of step indices it depends on
    """
    dependencies: Dict[int, Set[int]] = {}
    step_map: Dict[str, int] = {}  # Map "FLOW:RATA:FILE_TYPE" to step index

    # Primo passaggio: costruisci mappa step
    for idx, step in enumerate(steps):
        name = step.get('name', '')
        info = parse_step_info(name)
        if info:
            rata = info['rata'] or 'default'  # Use 'default' for steps without explicit rata
            key = f"{info['flow']}:{rata}:{info['file_type']}"
            step_map[key] = idx

    # Secondo passaggio: costruisci dipendenze
    for idx, step in enumerate(steps):
        dependencies[idx] = set()
        name = step.get('name', '')
        info = parse_step_info(name)

        if not info:
            continue

        flow = info['flow']
        file_type = info['file_type']
        rata = info['rata'] or 'default'

        # Regola 0: SINIIS dipende da tutti i FILE_EMISTI di ORDINARIA e SPECIALE (stessa rata)
        if file_type == 'SINIIS':
            # Trova FILE_EMISTI di ogni flusso ORDINARIA e SPECIALE (stessa rata)
            for other_idx, other_step in enumerate(steps):
                other_name = other_step.get('name', '')
                other_info = parse_step_info(other_name)
                if other_info:
                    other_flow = other_info['flow']
                    other_file = other_info['file_type']
                    other_rata = other_info['rata'] or 'default'
                    # Crea dipendenza solo se stessa rata
                    if other_rata == rata:
                        # Dipende da FILE_EMISTI di ORDINARIA e tutti i SPECIALE*
                        if other_flow in ['ORDINARIA'] or other_flow.startswith('SPECIALE'):
                            if other_file == 'FILE_EMISTI':
                                dependencies[idx].add(other_idx)

        # Regola 1: FILE_EMISTI dipende da FILE_UTILITY dello stesso flusso e rata
        elif file_type == 'FILE_EMISTI':
            dep_key = f"{flow}:{rata}:FILE_UTILITY"
            if dep_key in step_map:
                dependencies[idx].add(step_map[dep_key])

        # Regola 2: FILE_22000X e ANASTI dipendono dallo step precedente (stessa rata)
        elif file_type in ['FILE_22000X', 'ANASTI']:
            if file_type == 'ANASTI':
                # ANASTI dipende da FILE_UTILITY dello stesso flusso e rata
                dep_key = f"{flow}:{rata}:FILE_UTILITY"
                if dep_key in step_map:
                    dependencies[idx].add(step_map[dep_key])
            elif file_type == 'FILE_22000X':
                if flow == 'RITENUTE':
                    # RITENUTE FILE_22000X dipende da ANASTI (stessa rata)
                    dep_key = f"{flow}:{rata}:ANASTI"
                else:
                    # Altri flussi: FILE_22000X dipende da FILE_EMISTI (stessa rata)
                    dep_key = f"{flow}:{rata}:FILE_EMISTI"
                if dep_key in step_map:
                    dependencies[idx].add(step_map[dep_key])

        # Regola 3: SPECIALE* FILE_UTILITY dipende da ORDINARIA FILE_UTILITY (stessa rata)
        elif flow.startswith('SPECIALE') and file_type == 'FILE_UTILITY':
            dep_key = f"ORDINARIA:{rata}:FILE_UTILITY"
            if dep_key in step_map:
                dependencies[idx].add(step_map[dep_key])

        # Regola 4: gli step transfer RITENUTE dipendono da SINIIS (stessa rata)
        # NOTA: questa regola si AGGIUNGE alle dipendenze interne definite sopra
        if flow == 'RITENUTE' and file_type != 'SINIIS':
            # Tutti gli step RITENUTE dipendono da SINIIS
            siniis_key = f"SINIIS:{rata}:SINIIS"
            if siniis_key in step_map:
                dependencies[idx].add(step_map[siniis_key])

    return dependencies


def resolve_path(base: Path, p: Optional[str]) -> Path:
    if not p:
        return base
    path = Path(p)
    if path.is_absolute():
        return path
    return (base / path).resolve()


def is_path_safe(base: Path, target: Path) -> bool:
    """Check if target path is within base directory (prevent path traversal).

    Args:
        base: Base directory path
        target: Target path to validate

    Returns:
        True if target is within base, False otherwise
    """
    try:
        target.resolve().relative_to(base.resolve())
        return True
    except ValueError:
        return False


def write_temp_script(shell: str, content: str) -> Path:
    """Write temporary script file with restricted permissions.

    Args:
        shell: Shell type (powershell, cmd, sh)
        content: Script content

    Returns:
        Path to created script file
    """
    suffix = {
        'powershell': '.ps1',
        'cmd': '.cmd',
        'sh': '.sh',
    }.get(shell, '.sh')
    temp_dir = Path(tempfile.mkdtemp(prefix=TEMP_SCRIPT_PREFIX))
    script_path = temp_dir / f"step{int(time.time() * 1000)}{suffix}"
    if shell == 'cmd':
        # assicura newline windows e echo off
        content = "@echo off\r\n" + content.replace("\n", "\r\n") + "\r\n"
        script_path.write_text(content, encoding='utf-8')
    else:
        script_path.write_text(content, encoding='utf-8')

    # Limita permessi solo al proprietario (previene lettura segreti da altri utenti)
    try:
        script_path.chmod(0o600)  # rw------- (owner read/write only)
    except (OSError, NotImplementedError):
        # Windows potrebbe non supportare chmod, ignora
        pass

    return script_path


def build_spawn(shell: str, script_path: Path) -> Tuple[str, List[str]]:
    if shell == 'powershell':
        exe = shutil.which('powershell.exe') or shutil.which('powershell') or 'powershell'
        return exe, ['-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', '-File', str(script_path)]
    if shell == 'cmd':
        exe = shutil.which('cmd.exe') or 'cmd.exe'
        return exe, ['/d', '/s', '/c', str(script_path)]
    # default a sh
    exe = shutil.which('sh') or shutil.which('bash') or 'sh'
    return exe, [str(script_path)]


def run_command(script: str, shell: str, cwd: Path, env: Dict[str, str], timeout: int) -> Dict[str, Any]:
    """Execute a script in the specified shell.

    Args:
        script: Script content to execute
        shell: Shell type (powershell, cmd, sh)
        cwd: Working directory
        env: Environment variables
        timeout: Timeout in seconds (0 = no timeout)

    Returns:
        Dict with exit_code, timed_out, stdout, stderr, duration
    """
    script_path = write_temp_script(shell, script)
    exe, args = build_spawn(shell, script_path)
    full_env = os.environ.copy()
    full_env.update({k: str(v) for k, v in env.items() if v is not None})

    # Forza output senza buffer per sottoprocessi Python
    full_env['PYTHONUNBUFFERED'] = '1'

    started = time.time()
    timed_out = False
    try:
        proc = subprocess.Popen(
            [exe, *args],
            cwd=str(cwd),
            env=full_env,
            stdin=subprocess.DEVNULL,  # Close stdin to prevent hanging on input
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8',
            bufsize=0,  # Unbuffered
        )

        # Registra processo con gestore shutdown per pulizia Ctrl+C
        global _shutdown_handler
        if _shutdown_handler:
            _shutdown_handler.register_process(proc)

        # Raccogli output mostrando progresso
        out_lines = []
        err_lines = []
        start_time = time.time()

        try:
            import select
            import sys

            # Usa select per I/O non-bloccante su sistemi Unix-like
            if hasattr(select, 'poll'):
                # Stream output in tempo reale usando select per I/O veramente non-bloccante
                last_heartbeat = time.time()
                heartbeat_interval = 30  # Show "still running" message every 30 seconds

                # Crea poller
                poller = select.poll()
                poller.register(proc.stdout, select.POLLIN)
                poller.register(proc.stderr, select.POLLIN)

                # Mappa file descriptor a stream
                fd_to_stream = {
                    proc.stdout.fileno(): ('stdout', proc.stdout, out_lines),
                    proc.stderr.fileno(): ('stderr', proc.stderr, err_lines)
                }

                while True:
                    # Controlla timeout
                    if timeout and timeout > 0:
                        elapsed = time.time() - start_time
                        if elapsed > timeout:
                            raise subprocess.TimeoutExpired(proc.args, timeout)

                    # Controlla se processo terminato
                    poll_result = proc.poll()
                    if poll_result is not None:
                        # Processo terminato, leggi TUTTO loutput rimanente
                        for stream_name, stream, lines in fd_to_stream.values():
                            remaining = stream.read()
                            if remaining:
                                lines.append(remaining)
                                if stream_name == 'stdout':
                                    print(remaining, end='', flush=True)
                                else:
                                    print(remaining, end='', file=sys.stderr, flush=True)
                        break

                    # Usa select con timeout per evitare busy-wait
                    ready = poller.poll(100)  # 100ms timeout

                    has_output = False
                    for fd, event in ready:
                        if event & select.POLLIN:
                            stream_name, stream, lines = fd_to_stream[fd]
                            # Leggi una riga se disponibile
                            line = stream.readline()
                            if line:
                                lines.append(line)
                                if stream_name == 'stdout':
                                    print(line, end='', flush=True)
                                else:
                                    print(line, end='', file=sys.stderr, flush=True)
                                has_output = True

                    # Mostra heartbeat se nessun output per un po
                    current_time = time.time()
                    if not has_output and (current_time - last_heartbeat) >= heartbeat_interval:
                        elapsed = int(current_time - start_time)
                        print(f"\r[Command still running... {elapsed}s elapsed]", end='', flush=True)
                        last_heartbeat = current_time

                out = ''.join(out_lines)
                err = ''.join(err_lines)
            else:
                # Fallback per Windows - usa communicate
                out, err = proc.communicate(timeout=timeout if timeout and timeout > 0 else None)

        except subprocess.TimeoutExpired:
            timed_out = True
            proc.kill()
            # Dai tempo al processo di pulire dopo kill
            try:
                out, err = proc.communicate(timeout=KILL_TIMEOUT_SEC)
            except subprocess.TimeoutExpired:
                # Forza terminazione se ancora appeso
                proc.terminate()
                try:
                    out, err = proc.communicate(timeout=CLEANUP_TIMEOUT_SEC)
                except subprocess.TimeoutExpired:
                    # Ultima risorsa - processo è zombie
                    out, err = '', 'Process did not respond to kill/terminate signals'
        code = proc.returncode

        # Deregistra processo da gestore shutdown
        if _shutdown_handler:
            _shutdown_handler.unregister_process(proc)
    finally:
        # pulisci directory script
        try:
            shutil.rmtree(script_path.parent, ignore_errors=True)
        except (OSError, PermissionError) as e:
            logging.warning(f"Failed to cleanup temp directory {script_path.parent}: {e}")
    duration = time.time() - started
    return {
        'exit_code': code,
        'timed_out': timed_out,
        'stdout': out or '',
        'stderr': err or '',
        'duration': duration,
    }


def check_contains(hay: str, needles: List[str], case_sensitive: bool, must_contain: bool) -> bool:
    """Unified function to check if haystack contains/doesn't contain needles.

    Args:
        hay: String to search in
        needles: List of strings to search for
        case_sensitive: Whether to perform case-sensitive search
        must_contain: If True, all needles must be present; if False, none must be present

    Returns:
        True if condition is satisfied, False otherwise
    """
    # Ottimizza ricerca case-insensitive convertendo una volta
    if not case_sensitive:
        hay = hay.lower()
        needles = [n.lower() for n in needles if n]
    else:
        needles = [n for n in needles if n]

    for n in needles:
        found = n in hay
        if must_contain and not found:
            return False
        if not must_contain and found:
            return False
    return True


def regex_all(hay: str, patterns: List[str]) -> bool:
    """Check if all regex patterns match in haystack (with caching).

    Args:
        hay: String to search in
        patterns: List of regex patterns

    Returns:
        True if all patterns match, False otherwise

    Raises:
        ValueError: If a pattern is invalid regex
    """
    for p in patterns:
        if not p:
            continue
        try:
            # Usa regex compilata in cache per performance
            if p not in _regex_cache:
                _regex_cache[p] = re.compile(p, flags=re.MULTILINE)
            if _regex_cache[p].search(hay) is None:
                return False
        except re.error as e:
            raise ValueError(f"Invalid regex pattern '{p}': {e}") from e
    return True


def pre_check(script: Optional[str], cwd: Path, timeout: int, default_shell: str) -> Tuple[bool, str]:
    """Execute pre-check validation script.

    Args:
        script: Script to execute for validation
        cwd: Working directory
        timeout: Timeout from main step
        default_shell: Shell to use

    Returns:
        Tuple of (success: bool, message: str)
    """
    if not script:
        return True, ''
    res = run_command(script, default_shell, cwd, env={}, timeout=max(POSTCHECK_TIMEOUT_MIN, timeout or 0))
    if res['timed_out']:
        return False, 'preCheck timeout'
    if res['exit_code'] != 0:
        return False, f"preCheck exit={res['exit_code']} stderr={res['stderr'].strip()}"
    return True, res['stdout']


def post_check(script: Optional[str], cwd: Path, timeout: int, default_shell: str) -> Tuple[bool, str]:
    """Execute post-check validation script.

    Args:
        script: Script to execute for validation
        cwd: Working directory
        timeout: Timeout from main step
        default_shell: Shell to use

    Returns:
        Tuple of (success: bool, message: str)
    """
    if not script:
        return True, ''
    res = run_command(script, default_shell, cwd, env={}, timeout=max(POSTCHECK_TIMEOUT_MIN, timeout or 0))
    if res['timed_out']:
        return False, 'postCheck timeout'
    if res['exit_code'] != 0:
        return False, f"postCheck exit={res['exit_code']} stderr={res['stderr'].strip()}"
    return True, res['stdout']


def validate_shell(shell: str) -> None:
    """Validate shell parameter.

    Args:
        shell: Shell name to validate

    Raises:
        ValueError: If shell is not valid
    """
    if shell not in VALID_SHELLS:
        raise ValueError(f"Invalid shell '{shell}'. Must be one of: {', '.join(VALID_SHELLS)}")


def validate_step_structure(step: Dict, index: int) -> List[str]:
    """Validate structure of a single step.

    Args:
        step: Step configuration dictionary
        index: Step index (1-based)

    Returns:
        List of error messages (empty if valid)
    """
    errors = []

    if not isinstance(step, dict):
        errors.append(f"Step {index}: must be a dictionary")
        return errors

    if 'run' not in step:
        errors.append(f"Step {index}: missing required 'run' field")

    if 'shell' in step:
        shell = step['shell']
        if shell not in VALID_SHELLS:
            errors.append(f"Step {index}: invalid shell '{shell}'. Must be one of: {', '.join(VALID_SHELLS)}")

    if 'timeout' in step:
        try:
            timeout = int(step['timeout'])
            if timeout < 0:
                errors.append(f"Step {index}: timeout must be >= 0")
        except (ValueError, TypeError):
            errors.append(f"Step {index}: timeout must be an integer")

    if 'expect' in step:
        expect = step['expect']
        if not isinstance(expect, dict):
            errors.append(f"Step {index}: 'expect' must be a dictionary")
        else:
            if 'exitCode' in expect:
                exit_code = expect['exitCode']
                if not isinstance(exit_code, (int, list)):
                    errors.append(f"Step {index}: exitCode must be int or list of ints")
                elif isinstance(exit_code, list):
                    if not all(isinstance(ec, int) for ec in exit_code):
                        errors.append(f"Step {index}: exitCode list must contain only integers")

    return errors


def validate_config(cfg: Dict) -> List[str]:
    """Validate configuration structure.

    Args:
        cfg: Configuration dictionary

    Returns:
        List of error messages (empty if valid)
    """
    errors = []

    if 'steps' not in cfg:
        errors.append("Configuration missing required 'steps' field")
        return errors

    if not isinstance(cfg['steps'], list):
        errors.append("'steps' must be a list")
        return errors

    if len(cfg['steps']) == 0:
        errors.append("'steps' list is empty")

    # Valida shell di default se presente
    if 'defaultShell' in cfg:
        shell = cfg['defaultShell']
        if shell not in VALID_SHELLS:
            errors.append(f"Invalid defaultShell '{shell}'. Must be one of: {', '.join(VALID_SHELLS)}")

    # SICUREZZA: Controlla credenziali hardcoded nel config
    variables = cfg.get('variables', {})
    dangerous_keys = ['MONGO_PASS', 'MONGO_USER', 'MONGODB_URI', 'PASSWORD', 'SECRET', 'API_KEY', 'TOKEN']
    for key in variables:
        if any(danger in key.upper() for danger in dangerous_keys):
            # Solo warning, non errore - per non rompere config esistenti
            errors.append(f"SECURITY WARNING: Credentials should not be stored in config file: {key}. "
                         f"Use environment variables instead (export {key}=...)")

    # Valida ogni step
    for i, step in enumerate(cfg['steps'], start=1):
        errors.extend(validate_step_structure(step, i))

    return errors


def setup_logging(log_file: Path, verbose: bool = False) -> logging.Logger:
    """Setup logging configuration with thread-safety.

    Args:
        log_file: Path to log file
        verbose: Enable verbose logging

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger('opirunner')
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    # Aggiungi lock per logging thread-safe in modalità parallela
    logger._lock = threading.Lock()

    # File handler (dettagliato) - thread-safe di default
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter('[%(asctime)s] [%(threadName)s] %(levelname)s: %(message)s')
    file_handler.setFormatter(file_formatter)

    # Console handler (meno verboso)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG if verbose else logging.INFO)
    console_formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(console_formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def validate_config_with_schema(cfg: Dict, config_path: Path, logger: Optional[logging.Logger] = None) -> bool:
    """Validate configuration against JSON Schema if available.

    Args:
        cfg: Configuration dictionary
        config_path: Path to config file (used to locate schema)
        logger: Optional logger for messages

    Returns:
        True if validation passed or schema not available, False if validation failed
    """
    if not JSONSCHEMA_AVAILABLE:
        if logger:
            logger.debug("jsonschema module not available, skipping schema validation")
        return True

    # Cerca file schema
    schema_path = config_path.parent / 'pipeline_schema.json'
    if not schema_path.exists():
        if logger:
            logger.debug(f"Schema file not found at {schema_path}, skipping schema validation")
        return True

    try:
        # Carica schema
        with open(schema_path, 'r', encoding='utf-8') as f:
            schema = json.load(f)

        # Valida
        jsonschema.validate(instance=cfg, schema=schema)

        if logger:
            logger.info(f"Configuration validated against schema: {schema_path.name}")
        return True

    except jsonschema.ValidationError as e:
        if logger:
            logger.error(f"Configuration schema validation failed:")
            logger.error(f"  Path: {' -> '.join(str(p) for p in e.path)}")
            logger.error(f"  Error: {e.message}")
        return False
    except Exception as e:
        if logger:
            logger.warning(f"Schema validation error: {e}")
        return True  # Don't fail on schema errors


def execute_step(
    step_index: int,
    step: Dict[str, Any],
    cfg: Dict[str, Any],
    config_dir: Path,
    default_shell: str,
    default_timeout: int,
    default_exit_code: int,
    args: Any,
    logger: logging.Logger,
    metrics_collector: Optional[Any] = None,
    progress_tracker: Optional[Any] = None
) -> StepResult:
    """Execute a single step and return the result.

    This function extracts the step execution logic to enable parallel execution.

    Note: metrics_collector and progress_tracker are passed but tracking is done
    by the caller (main sequential loop or run_parallel) to avoid duplicate tracking.
    """
    i = step_index + 1  # 1-indexed for display
    name = str(step.get('name') or f'Step {i}')
    cfg_vars = cfg.get('variables') or {}
    env_cfg = step.get('env') or {}
    scope = build_scope(cfg_vars, env_cfg, i, name, config_dir)
    shell = step.get('shell') or default_shell

    # Valida shell dello step
    try:
        validate_shell(shell)
    except ValueError as e:
        logger.error(f"Step {i} ({name}): {e}")
        return StepResult(
            index=i, name=name, passed=False, exit_code=-1,
            timed_out=False, duration_sec=0.0,
            reasons=[f"Invalid shell: {e}"],
            stdout='', stderr=''
        )

    cwd_tpl = step.get('cwd')
    cwd_expanded = expand_template(cwd_tpl, scope, shell_escape=False) if cwd_tpl else str(config_dir)
    cwd = resolve_path(config_dir, cwd_expanded)

    # Valida che CWD esista e sia una directory
    if not cwd.exists():
        logger.error(f"Step {i} ({name}): Working directory does not exist: {cwd}")
        return StepResult(
            index=i, name=name, passed=False, exit_code=-1,
            timed_out=False, duration_sec=0.0,
            reasons=[f"Working directory does not exist: {cwd}"],
            stdout='', stderr=''
        )
    if not cwd.is_dir():
        logger.error(f"Step {i} ({name}): CWD path is not a directory: {cwd}")
        return StepResult(
            index=i, name=name, passed=False, exit_code=-1,
            timed_out=False, duration_sec=0.0,
            reasons=[f"CWD path is not a directory: {cwd}"],
            stdout='', stderr=''
        )

    run_tpl = step.get('run')
    run_script = expand_template(run_tpl, scope, shell_escape=True) if run_tpl else ''

    timeout = int(step.get('timeout') or default_timeout or 0)

    # Inizia con ambiente corrente completo, poi sovrascrivi con variabili specifiche dello step
    env_final: Dict[str, str] = {k: str(v) for k, v in os.environ.items()}

    # Sovrascrivi con variabili env specifiche dello step da JSON
    for k, v in env_cfg.items():
        env_final[k] = expand_template(str(v), scope, shell_escape=False) or ''

    # Costruisci e imposta dinamicamente REMOTE_HOME solo se non già fornito
    if run_tpl and not env_final.get('REMOTE_HOME'):
        remote_home = construct_remote_home(run_tpl, scope)
        if remote_home:
            env_final['REMOTE_HOME'] = remote_home
            scope['REMOTE_HOME'] = remote_home
            logger.debug(f"Dynamically set REMOTE_HOME={remote_home}")

    # Log inizio step con timestamp
    start_time = datetime.now()
    logger.info(f"\n{'='*80}")
    logger.info(f"STEP [{i}/{cfg.get('__total_steps', '?')}]: {name}")
    logger.info(f"Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Shell: {shell} | CWD: {cwd} | Timeout: {timeout}s")
    logger.info(f"Command: {sanitize_for_logging(run_script)}")
    logger.info(f"{'='*80}")

    # Ottieni configurazione expect in anticipo
    expect = step.get('expect') or {}

    # Initialize check details accumulators
    precheck_details_list = []
    postcheck_details_list = []

    # Esegui preCheck se presente
    expected_lines_count = None
    if 'preCheck' in expect:
        precheck_cfg = expect.get('preCheck')

        # Check if it's file existence preCheck
        if isinstance(precheck_cfg, dict) and 'fileExists' in precheck_cfg:
            file_path_tpl = precheck_cfg.get('fileExists')
            file_path_str = expand_template(file_path_tpl, scope, shell_escape=False)
            file_path = Path(file_path_str) if file_path_str else None

            if not file_path:
                logger.error("preCheck failed: fileExists path not specified")
                logger.info(f"PreCheck Result: FAILED")
                logger.info(f"PreCheck Details: fileExists path not specified")
                return StepResult(
                    index=i, name=name, passed=False, exit_code=-1,
                    timed_out=False, duration_sec=0.0,
                    reasons=["preCheck failed: fileExists path not specified"],
                    stdout='', stderr=''
                )

            logger.info(f"Executing fileExists preCheck for: {file_path}")

            if not file_path.exists():
                logger.error(f"PreCheck FAILED: File does not exist: {file_path}")
                logger.info(f"PreCheck Result: FAILED")
                logger.info(f"PreCheck Details: File not found: {file_path}")
                return StepResult(
                    index=i, name=name, passed=False, exit_code=-1,
                    timed_out=False, duration_sec=0.0,
                    reasons=[f"preCheck failed: File not found: {file_path}"],
                    stdout='', stderr=''
                )

            if not file_path.is_file():
                logger.error(f"PreCheck FAILED: Path is not a file: {file_path}")
                logger.info(f"PreCheck Result: FAILED")
                logger.info(f"PreCheck Details: Path is not a file: {file_path}")
                return StepResult(
                    index=i, name=name, passed=False, exit_code=-1,
                    timed_out=False, duration_sec=0.0,
                    reasons=[f"preCheck failed: Path is not a file: {file_path}"],
                    stdout='', stderr=''
                )

            logger.info(f"PreCheck Result: PASSED")
            logger.info(f"PreCheck Details: File exists: {file_path}")

        # Check if it's MongoDB-based preCheck
        elif isinstance(precheck_cfg, dict) and precheck_cfg.get('type') in ['mongodb_files', 'mongodb_emisti', 'mongodb_emisti_speciale', 'mongodb_anasti', 'mongodb_22000x']:
            if not MONGODB_CHECKER_AVAILABLE:
                logger.error("MongoDB checker module not available (missing pymongo?)")
                return StepResult(
                    index=i, name=name, passed=False, exit_code=-1,
                    timed_out=False, duration_sec=0.0,
                    reasons=["MongoDB checker module not available"],
                    stdout='', stderr=''
                )

            precheck_type = precheck_cfg.get('type')
            rata = expand_template(precheck_cfg.get('rata'), scope, shell_escape=False)
            remote_home = expand_template(precheck_cfg.get('remoteHome'), scope, shell_escape=False) or os.environ.get('REMOTE_HOME', '')

            logger.info("=" * 80)
            logger.info(f"PRECHECK: MongoDB file verification")
            logger.info(f"  Type: {precheck_type}")
            logger.info(f"  Rata: {rata}")
            logger.info(f"  Remote Home: {remote_home}")
            logger.info("=" * 80)

            if not remote_home:
                logger.error("preCheck failed: REMOTE_HOME not configured")
                return StepResult(
                    index=i, name=name, passed=False, exit_code=-1,
                    timed_out=False, duration_sec=0.0,
                    reasons=["preCheck failed: REMOTE_HOME not configured"],
                    stdout='', stderr=''
                )

            # Build MongoDB config if needed for ANASTI execution state check
            mongo_config = None
            if precheck_type == 'mongodb_anasti':
                # Check if MongoDB config is provided in preCheck
                if precheck_cfg.get('mongoHost') or precheck_cfg.get('mongoUri'):
                    mongo_config = {
                        'host': expand_template(precheck_cfg.get('mongoHost'), scope, shell_escape=False) or os.environ.get('MONGO_HOST', 'localhost'),
                        'port': int(expand_template(precheck_cfg.get('mongoPort'), scope, shell_escape=False) or os.environ.get('MONGO_PORT', '27017')),
                        'database': expand_template(precheck_cfg.get('mongoDb'), scope, shell_escape=False) or os.environ.get('MONGO_DB', 'your_database'),
                        'user': expand_template(precheck_cfg.get('mongoUser'), scope, shell_escape=False) or os.environ.get('MONGO_USER', ''),
                        'password': expand_template(precheck_cfg.get('mongoPass'), scope, shell_escape=False) or os.environ.get('MONGO_PASS', ''),
                    }
                    mongo_uri = expand_template(precheck_cfg.get('mongoUri'), scope, shell_escape=False) or os.environ.get('MONGODB_URI', '')
                    if mongo_uri:
                        mongo_config['uri'] = mongo_uri

            # Call appropriate check function based on type (with retry logic)
            def precheck_operation():
                # Wrap Mongo precheck results into (ok, (count, message)) for retry_with_backoff
                if precheck_type == 'mongodb_emisti':
                    ok, count, msg = check_emisti_files_and_count_lines(rata, remote_home)
                elif precheck_type == 'mongodb_emisti_speciale':
                    ok, count, msg = check_emisti_speciale_files_and_count_lines(rata, remote_home)
                elif precheck_type == 'mongodb_anasti':
                    poll_interval_sec = int(precheck_cfg.get('pollIntervalSec', 10))
                    max_wait_min = int(precheck_cfg.get('maxWaitMin', 60))
                    ok, count, msg = check_anasti_file_and_count_lines(
                        rata, remote_home, mongo_config, poll_interval_sec, max_wait_min
                    )
                elif precheck_type == 'mongodb_22000x':
                    ok, count, msg = check_22000x_files_and_count_lines(rata, remote_home)
                else:  # mongodb_files
                    ok, count, msg = check_files_and_count_lines(rata, remote_home)
                return ok, (count, msg)

            pc_ok, (expected_lines_count, pc_msg) = retry_with_backoff(
                precheck_operation,
                max_retries=MAX_RETRIES,
                logger=logger,
                operation_name=f"MongoDB preCheck ({precheck_type})"
            )
            if not pc_ok:
                expected_lines_count = 0

            # Salva dettagli precheck
            precheck_details_list.append(f"Tipo: {precheck_type}")
            precheck_details_list.append(f"Rata: {rata}")
            precheck_details_list.append(f"Remote Home: {remote_home}")
            precheck_details_list.append("-" * 60)
            precheck_details_list.append(pc_msg)

            if not pc_ok:
                logger.error("=" * 80)
                logger.error(f"❌ PRECHECK FAILED")
                logger.error(f"  Reason: {pc_msg}")
                logger.error("=" * 80)
                return StepResult(
                    index=i, name=name, passed=False, exit_code=-1,
                    timed_out=False, duration_sec=0.0,
                    reasons=[f"preCheck failed: {pc_msg}"],
                    stdout='', stderr='',
                    precheck_details="\n".join(precheck_details_list)
                )
            else:
                logger.info("=" * 80)
                logger.info(f"✓ PRECHECK PASSED")
                logger.info(f"  {pc_msg}")
                logger.info("=" * 80)

    # Execute the command (unless dry-run)
    logger.info("Executing command...")
    if args.dry_run:
        logger.info("DRY RUN - Command not executed")
        res = {'exit_code': 0, 'stdout': '', 'stderr': '', 'timed_out': False, 'duration': 0.0}
    else:
        cmd_start = datetime.now()
        res = run_command(run_script, shell=shell, cwd=cwd, timeout=timeout, env=env_final)
        cmd_end = datetime.now()
        logger.info(f"Command completed in {res['duration']:.2f}s")
        logger.info(f"Exit Code: {res['exit_code']}")
        if res['timed_out']:
            logger.warning(f"Command TIMED OUT after {timeout}s")

    # Check result
    reasons: List[str] = []
    ok = True

    # Check exit code
    expected_code = expect.get('exitCode', default_exit_code)
    if res['exit_code'] != expected_code:
        ok = False
        reasons.append(f"exit code {res['exit_code']} != expected {expected_code}")

    # Check timeout
    if res['timed_out']:
        ok = False
        reasons.append(f"timed out (limit={timeout}s)")

    # Check stdout/stderr patterns
    for k in ['stdout', 'stderr']:
        for match_type in ['contains', 'notContains', 'regex', 'notRegex']:
            patterns = expect.get(k, {}).get(match_type, [])
            if isinstance(patterns, str):
                patterns = [patterns]
            for pat in patterns:
                text = res[k]
                if match_type == 'contains':
                    if pat not in text:
                        ok = False
                        reasons.append(f"{k}: missing '{pat}'")
                elif match_type == 'notContains':
                    if pat in text:
                        ok = False
                        reasons.append(f"{k}: contains unwanted '{pat}'")
                elif match_type == 'regex':
                    if not re.search(pat, text):
                        ok = False
                        reasons.append(f"{k}: does not match regex '{pat}'")
                elif match_type == 'notRegex':
                    if re.search(pat, text):
                        ok = False
                        reasons.append(f"{k}: matches unwanted regex '{pat}'")

    # Check file existence/content
    for fcheck in expect.get('files', []):
        p_tpl = fcheck.get('path')
        p_str = expand_template(p_tpl, scope, shell_escape=False)
        p = resolve_path(cwd, p_str) if p_str else cwd
        try:
            if 'exists' in fcheck:
                should_exist = fcheck['exists']
                if should_exist and not p.exists():
                    ok = False
                    reasons.append(f"file does not exist: {p}")
                elif not should_exist and p.exists():
                    ok = False
                    reasons.append(f"file exists (should not): {p}")
            if 'contains' in fcheck and p.is_file():
                needle = fcheck['contains']
                content = p.read_text(encoding='utf-8', errors='ignore')
                if needle not in content:
                    ok = False
                    reasons.append(f"file {p} does not contain '{needle}'")
        except Exception as e:
            ok = False
            reasons.append(f"cannot check file {p}: {e}")

    # Execute postCheck if present
    if 'postCheck' in expect:
        postcheck_cfg = expect.get('postCheck')
        postcheck_type = postcheck_cfg.get('type') if isinstance(postcheck_cfg, dict) else None

        if postcheck_type == 'mongodb_emisti_quality':
            if not MONGODB_CHECKER_AVAILABLE:
                ok = False
                reasons.append("postCheck failed: MongoDB checker module not available")
            elif expected_lines_count is None:
                ok = False
                reasons.append("postCheck failed: preCheck did not provide expected count")
            else:
                rata = expand_template(postcheck_cfg.get('rata'), scope, shell_escape=False)
                tipo_flusso = expand_template(postcheck_cfg.get('tipoFlusso'), scope, shell_escape=False) or 'ORDINARIA'
                progressivo_speciale = postcheck_cfg.get('progressivoSpeciale')

                logger.info("=" * 80)
                logger.info(f"POSTCHECK: MongoDB EMISTI data quality verification")
                logger.info(f"  Rata: {rata}")
                logger.info(f"  Tipo Flusso: {tipo_flusso}")
                if progressivo_speciale:
                    logger.info(f"  Progressivo Speciale: {progressivo_speciale}")
                logger.info(f"  Expected Count: {expected_lines_count}")
                logger.info("=" * 80)

                mongo_config = {
                    'host': expand_template(postcheck_cfg.get('mongoHost'), scope, shell_escape=False) or os.environ.get('MONGO_HOST', 'localhost'),
                    'port': int(expand_template(postcheck_cfg.get('mongoPort'), scope, shell_escape=False) or os.environ.get('MONGO_PORT', '27017')),
                    'database': expand_template(postcheck_cfg.get('mongoDb'), scope, shell_escape=False) or os.environ.get('MONGO_DB', 'your_database'),
                    'user': expand_template(postcheck_cfg.get('mongoUser'), scope, shell_escape=False) or os.environ.get('MONGO_USER', ''),
                    'password': expand_template(postcheck_cfg.get('mongoPass'), scope, shell_escape=False) or os.environ.get('MONGO_PASS', ''),
                }
                mongo_uri = expand_template(postcheck_cfg.get('mongoUri'), scope, shell_escape=False) or os.environ.get('MONGODB_URI', '')
                if mongo_uri:
                    mongo_config['uri'] = mongo_uri

                poll_interval_sec = int(postcheck_cfg.get('pollIntervalSec', 30))
                stability_timeout_min = int(postcheck_cfg.get('stabilityTimeoutMin', 5))
                max_wait_min = int(postcheck_cfg.get('maxWaitMin', 60))

                pc_ok, pc_msg = verify_emisti_data_quality_with_polling(
                    rata=rata,
                    expected_count=expected_lines_count,
                    mongo_config=mongo_config,
                    tipo_flusso=tipo_flusso,
                    progressivo_speciale=progressivo_speciale,
                    poll_interval_sec=poll_interval_sec,
                    stability_timeout_min=stability_timeout_min,
                    max_wait_min=max_wait_min
                )

                # Salva dettagli postcheck
                postcheck_details_list.append(f"Tipo: mongodb_emisti_quality")
                postcheck_details_list.append(f"Rata: {rata}")
                postcheck_details_list.append(f"Tipo Flusso: {tipo_flusso}")
                if progressivo_speciale:
                    postcheck_details_list.append(f"Progressivo Speciale: {progressivo_speciale}")
                postcheck_details_list.append(f"Conteggio Atteso: {expected_lines_count}")
                postcheck_details_list.append(f"MongoDB: {mongo_config['host']}:{mongo_config['port']}/{mongo_config['database']}")
                postcheck_details_list.append("-" * 60)
                postcheck_details_list.append(pc_msg)

                if not pc_ok:
                    ok = False
                    reasons.append(f"postCheck failed: {pc_msg}")
                    logger.error("=" * 80)
                    logger.error(f"❌ POSTCHECK FAILED: EMISTI Quality")
                    logger.error(f"  {pc_msg}")
                    logger.error("=" * 80)
                else:
                    logger.info("=" * 80)
                    logger.info(f"✓ POSTCHECK PASSED: EMISTI Quality")
                    logger.info(f"  All data quality checks successful")
                    logger.info("=" * 80)

        elif postcheck_type == 'mongodb_anasti_quality':
            if not MONGODB_CHECKER_AVAILABLE:
                ok = False
                reasons.append("postCheck failed: MongoDB checker module not available")
            elif expected_lines_count is None:
                ok = False
                reasons.append("postCheck failed: preCheck did not provide expected count")
            else:
                rata = expand_template(postcheck_cfg.get('rata'), scope, shell_escape=False)

                logger.info("=" * 80)
                logger.info(f"POSTCHECK: MongoDB ANASTI data quality verification")
                logger.info(f"  Rata: {rata}")
                logger.info(f"  Expected Count: {expected_lines_count}")
                logger.info("=" * 80)

                mongo_config = {
                    'host': expand_template(postcheck_cfg.get('mongoHost'), scope, shell_escape=False) or os.environ.get('MONGO_HOST', 'localhost'),
                    'port': int(expand_template(postcheck_cfg.get('mongoPort'), scope, shell_escape=False) or os.environ.get('MONGO_PORT', '27017')),
                    'database': expand_template(postcheck_cfg.get('mongoDb'), scope, shell_escape=False) or os.environ.get('MONGO_DB', 'your_database'),
                    'user': expand_template(postcheck_cfg.get('mongoUser'), scope, shell_escape=False) or os.environ.get('MONGO_USER', ''),
                    'password': expand_template(postcheck_cfg.get('mongoPass'), scope, shell_escape=False) or os.environ.get('MONGO_PASS', ''),
                }
                mongo_uri = expand_template(postcheck_cfg.get('mongoUri'), scope, shell_escape=False) or os.environ.get('MONGODB_URI', '')
                if mongo_uri:
                    mongo_config['uri'] = mongo_uri

                poll_interval_sec = int(postcheck_cfg.get('pollIntervalSec', 30))
                stability_timeout_min = int(postcheck_cfg.get('stabilityTimeoutMin', 5))
                max_wait_min = int(postcheck_cfg.get('maxWaitMin', 60))

                pc_ok, pc_msg = verify_anasti_data_quality_with_polling(
                    rata=rata,
                    expected_count=expected_lines_count,
                    mongo_config=mongo_config,
                    poll_interval_sec=poll_interval_sec,
                    stability_timeout_min=stability_timeout_min,
                    max_wait_min=max_wait_min
                )

                # Salva dettagli postcheck
                postcheck_details_list.append(f"Tipo: mongodb_anasti_quality")
                postcheck_details_list.append(f"Rata: {rata}")
                postcheck_details_list.append(f"Conteggio Atteso: {expected_lines_count}")
                postcheck_details_list.append(f"MongoDB: {mongo_config['host']}:{mongo_config['port']}/{mongo_config['database']}")
                postcheck_details_list.append("-" * 60)
                postcheck_details_list.append(pc_msg)

                if not pc_ok:
                    ok = False
                    reasons.append(f"postCheck failed: {pc_msg}")
                    logger.error("=" * 80)
                    logger.error(f"❌ POSTCHECK FAILED: ANASTI Quality")
                    logger.error(f"  {pc_msg}")
                    logger.error("=" * 80)
                else:
                    logger.info("=" * 80)
                    logger.info(f"✓ POSTCHECK PASSED: ANASTI Quality")
                    logger.info(f"  All data quality checks successful")
                    logger.info("=" * 80)

        elif postcheck_type in ['mongodb_22000x_polling', 'mongodb_22000x_quality']:
            if not MONGODB_CHECKER_AVAILABLE:
                ok = False
                reasons.append("postCheck failed: MongoDB checker module not available")
            elif expected_lines_count is None:
                ok = False
                reasons.append("postCheck failed: preCheck did not provide expected count")
            else:
                rata = expand_template(postcheck_cfg.get('rata'), scope, shell_escape=False)
                remote_home = expand_template(postcheck_cfg.get('remoteHome'), scope, shell_escape=False) or os.environ.get('REMOTE_HOME', '')
                tipo_flusso = expand_template(postcheck_cfg.get('tipoFlusso'), scope, shell_escape=False) or 'ORDINARIA'
                progressivo_speciale = postcheck_cfg.get('progressivoSpeciale')

                logger.info("=" * 80)
                logger.info(f"POSTCHECK: MongoDB 22000X data quality verification")
                logger.info(f"  Rata: {rata}")
                logger.info(f"  Tipo Flusso: {tipo_flusso}")
                if progressivo_speciale:
                    logger.info(f"  Progressivo Speciale: {progressivo_speciale}")
                logger.info(f"  Remote Home: {remote_home}")
                logger.info(f"  Expected Count: {expected_lines_count}")
                logger.info("=" * 80)

                mongo_config = {
                    'host': expand_template(postcheck_cfg.get('mongoHost'), scope, shell_escape=False) or os.environ.get('MONGO_HOST', 'localhost'),
                    'port': int(expand_template(postcheck_cfg.get('mongoPort'), scope, shell_escape=False) or os.environ.get('MONGO_PORT', '27017')),
                    'database': expand_template(postcheck_cfg.get('mongoDb'), scope, shell_escape=False) or os.environ.get('MONGO_DB', 'your_database'),
                    'user': expand_template(postcheck_cfg.get('mongoUser'), scope, shell_escape=False) or os.environ.get('MONGO_USER', ''),
                    'password': expand_template(postcheck_cfg.get('mongoPass'), scope, shell_escape=False) or os.environ.get('MONGO_PASS', ''),
                }
                mongo_uri = expand_template(postcheck_cfg.get('mongoUri'), scope, shell_escape=False) or os.environ.get('MONGODB_URI', '')
                if mongo_uri:
                    mongo_config['uri'] = mongo_uri

                poll_interval_sec = int(postcheck_cfg.get('pollIntervalSec', 30))
                stability_timeout_min = int(postcheck_cfg.get('stabilityTimeoutMin', 5))
                max_wait_min = int(postcheck_cfg.get('maxWaitMin', 60))

                pc_ok, pc_msg = verify_22000x_data_with_polling(
                    rata=rata,
                    remote_home=remote_home,
                    expected_count=expected_lines_count,
                    mongo_config=mongo_config,
                    tipo_flusso=tipo_flusso,
                    progressivo_speciale=progressivo_speciale,
                    poll_interval_sec=poll_interval_sec,
                    stability_timeout_min=stability_timeout_min,
                    max_wait_min=max_wait_min
                )

                # Salva dettagli postcheck
                postcheck_details_list.append(f"Tipo: mongodb_22000x_quality")
                postcheck_details_list.append(f"Rata: {rata}")
                postcheck_details_list.append(f"Tipo Flusso: {tipo_flusso}")
                if progressivo_speciale:
                    postcheck_details_list.append(f"Progressivo Speciale: {progressivo_speciale}")
                postcheck_details_list.append(f"Remote Home: {remote_home}")
                postcheck_details_list.append(f"Conteggio Atteso: {expected_lines_count}")
                postcheck_details_list.append(f"MongoDB: {mongo_config['host']}:{mongo_config['port']}/{mongo_config['database']}")
                postcheck_details_list.append("-" * 60)
                postcheck_details_list.append(pc_msg)

                if not pc_ok:
                    ok = False
                    reasons.append(f"postCheck failed: {pc_msg}")
                    logger.error("=" * 80)
                    logger.error(f"❌ POSTCHECK FAILED: 22000X Quality")
                    logger.error(f"  {pc_msg}")
                    logger.error("=" * 80)
                else:
                    logger.info("=" * 80)
                    logger.info(f"✓ POSTCHECK PASSED: 22000X Quality")
                    logger.info(f"  All data quality checks successful")
                    logger.info("=" * 80)

        elif postcheck_type == 'mongodb_polling':
            if not MONGODB_CHECKER_AVAILABLE:
                ok = False
                reasons.append("postCheck failed: MongoDB checker module not available")
            elif expected_lines_count is None:
                ok = False
                reasons.append("postCheck failed: preCheck did not provide expected count")
            else:
                logger.info("Executing MongoDB postCheck (polling for expected count)...")
                rata = expand_template(postcheck_cfg.get('rata'), scope, shell_escape=False)

                mongo_config = {
                    'host': expand_template(postcheck_cfg.get('mongoHost'), scope, shell_escape=False) or os.environ.get('MONGO_HOST', 'localhost'),
                    'port': int(expand_template(postcheck_cfg.get('mongoPort'), scope, shell_escape=False) or os.environ.get('MONGO_PORT', '27017')),
                    'database': expand_template(postcheck_cfg.get('mongoDb'), scope, shell_escape=False) or os.environ.get('MONGO_DB', 'your_database'),
                    'user': expand_template(postcheck_cfg.get('mongoUser'), scope, shell_escape=False) or os.environ.get('MONGO_USER', ''),
                    'password': expand_template(postcheck_cfg.get('mongoPass'), scope, shell_escape=False) or os.environ.get('MONGO_PASS', ''),
                }
                mongo_uri = expand_template(postcheck_cfg.get('mongoUri'), scope, shell_escape=False) or os.environ.get('MONGODB_URI', '')
                if mongo_uri:
                    mongo_config['uri'] = mongo_uri

                poll_interval_sec = int(postcheck_cfg.get('pollIntervalSec', 30))
                stability_timeout_min = int(postcheck_cfg.get('stabilityTimeoutMin', 5))
                max_wait_min = int(postcheck_cfg.get('maxWaitMin', 60))

                pc_ok, pc_msg = verify_mongodb_count_with_polling(
                    rata=rata,
                    expected_count=expected_lines_count,
                    mongo_config=mongo_config,
                    poll_interval_sec=poll_interval_sec,
                    stability_timeout_min=stability_timeout_min,
                    max_wait_min=max_wait_min
                )

                if not pc_ok:
                    ok = False
                    reasons.append(f"postCheck failed: {pc_msg}")
                    logger.error(f"PostCheck FAILED: {pc_msg}")
                    logger.info(f"PostCheck Result: FAILED")
                    logger.info(f"PostCheck Details: {pc_msg}")
                else:
                    logger.info(f"PostCheck Result: PASSED")
                    logger.info(f"PostCheck Details: {pc_msg}")

        elif postcheck_type == 'mongodb_polling_nonzero':
            # Success if count becomes >0; fail only if remains 0 for stability/max wait
            if not MONGODB_CHECKER_AVAILABLE:
                ok = False
                reasons.append("postCheck failed: MongoDB checker module not available")
            else:
                rata = expand_template(postcheck_cfg.get('rata'), scope, shell_escape=False)

                logger.info("=" * 80)
                logger.info(f"POSTCHECK: MongoDB count verification (non-zero)")
                logger.info(f"  Rata: {rata}")
                logger.info(f"  Expected: count > 0")
                logger.info("=" * 80)

                mongo_config = {
                    'host': expand_template(postcheck_cfg.get('mongoHost'), scope, shell_escape=False) or os.environ.get('MONGO_HOST', 'localhost'),
                    'port': int(expand_template(postcheck_cfg.get('mongoPort'), scope, shell_escape=False) or os.environ.get('MONGO_PORT', '27017')),
                    'database': expand_template(postcheck_cfg.get('mongoDb'), scope, shell_escape=False) or os.environ.get('MONGO_DB', 'your_database'),
                    'user': expand_template(postcheck_cfg.get('mongoUser'), scope, shell_escape=False) or os.environ.get('MONGO_USER', ''),
                    'password': expand_template(postcheck_cfg.get('mongoPass'), scope, shell_escape=False) or os.environ.get('MONGO_PASS', ''),
                }
                mongo_uri = expand_template(postcheck_cfg.get('mongoUri'), scope, shell_escape=False) or os.environ.get('MONGODB_URI', '')
                if mongo_uri:
                    mongo_config['uri'] = mongo_uri

                poll_interval_sec = int(postcheck_cfg.get('pollIntervalSec', 30))
                stability_timeout_min = int(postcheck_cfg.get('stabilityTimeoutMin', 5))
                max_wait_min = int(postcheck_cfg.get('maxWaitMin', 60))

                pc_ok, pc_msg = verify_mongodb_count_nonzero_with_polling(
                    rata=rata,
                    mongo_config=mongo_config,
                    poll_interval_sec=poll_interval_sec,
                    stability_timeout_min=stability_timeout_min,
                    max_wait_min=max_wait_min
                )

                # Salva dettagli postcheck
                postcheck_details_list.append(f"Tipo: mongodb_polling_nonzero")
                postcheck_details_list.append(f"Rata: {rata}")
                postcheck_details_list.append(f"Atteso: conteggio > 0")
                postcheck_details_list.append(f"MongoDB: {mongo_config['host']}:{mongo_config['port']}/{mongo_config['database']}")
                postcheck_details_list.append("-" * 60)
                postcheck_details_list.append(pc_msg)

                if not pc_ok:
                    ok = False
                    reasons.append(f"postCheck failed: {pc_msg}")
                    logger.error("=" * 80)
                    logger.error(f"❌ POSTCHECK FAILED: Count verification")
                    logger.error(f"  {pc_msg}")
                    logger.error("=" * 80)
                else:
                    logger.info("=" * 80)
                    logger.info(f"✓ POSTCHECK PASSED: Count > 0")
                    logger.info(f"  MongoDB contains data")
                    logger.info("=" * 80)

    # Log final result with timestamp
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logger.info(f"{'-'*80}")
    if ok:
        logger.info(f"STEP RESULT: PASSED")
    else:
        logger.error(f"STEP RESULT: FAILED")
        logger.error(f"Failure Reasons: {' | '.join(reasons)}")
    logger.info(f"End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Total Duration: {duration:.2f}s")
    logger.info(f"{'='*80}\n")

    return StepResult(
        index=i,
        name=name,
        passed=ok,
        exit_code=res['exit_code'],
        timed_out=res['timed_out'],
        duration_sec=round(res['duration'], 3),
        reasons=reasons,
        stdout=res['stdout'],
        stderr=res['stderr'],
        precheck_details="\n".join(precheck_details_list),
        postcheck_details="\n".join(postcheck_details_list)
    )


def run_parallel(
    cfg: Dict[str, Any],
    config_dir: Path,
    default_shell: str,
    default_timeout: int,
    default_exit_code: int,
    stop_on_failure: bool,
    args: Any,
    logger: logging.Logger,
    max_workers: Optional[int] = None,
    start_step_index: int = 0,
    metrics_collector: Optional[Any] = None,
    progress_tracker: Optional[Any] = None
) -> Tuple[int, int, int, List[StepResult]]:
    """Execute pipeline steps in parallel waves based on dependencies.

    Args:
        max_workers: Maximum number of parallel workers (default: min(8, cpu_count+4))
        start_step_index: 0-indexed step to start from (steps before this are marked as skipped)

    Returns:
        Tuple of (total, passed, failed, results)
    """
    steps = cfg['steps']

    # Build dependency graph (prefer external config for independent-layer mode)
    dependencies = None

    if DEPENDENCY_LOADER_AVAILABLE:
        dep_candidates = []
        if config_dir:
            dep_candidates.append(config_dir / 'dependencies.json')
        dep_candidates.append(Path(__file__).parent / 'dependencies.json')

        seen_paths = set()
        for dep_path in dep_candidates:
            if dep_path is None:
                continue
            if dep_path in seen_paths:
                continue
            seen_paths.add(dep_path)

            if not dep_path.exists():
                continue

            try:
                dependencies = build_dependencies_from_config(steps, dep_path, logger=logger)
                logger.info(f"Using dependency graph from {dep_path} (independent-layers mode)")
                break
            except Exception as e:
                logger.warning(f"Failed to load dependency config {dep_path}: {e}")

    if dependencies is None:
        logger.info("Dependency config not available; falling back to sequential dependencies (each step waits for previous step)")
        dependencies = {}
        for i in range(len(steps)):
            if i == 0 or i <= start_step_index:
                # First step or resume point: no dependencies
                dependencies[i] = set()
            else:
                # Sequential fallback: each step waits for the previous one
                dependencies[i] = {i - 1}

    # Calculate max_workers if not specified
    if max_workers is None:
        import os as os_module
        max_workers = min(8, (os_module.cpu_count() or 4) + 4)

    logger.info(f"Parallel execution using max {max_workers} workers")

    total = 0
    passed = 0
    failed = 0
    results: List[StepResult] = []

    # Track completion state
    completed: Set[int] = set()
    failed_steps: Set[int] = set()
    blocked_steps: Set[int] = set()

    # Mark steps before start_step_index as completed (skipped)
    if start_step_index > 0:
        logger.info(f"\n=== Skipping steps 1-{start_step_index} (resume from step {start_step_index + 1}) ===\n")
        for idx in range(start_step_index):
            completed.add(idx)
            step_name = steps[idx].get('name', f'Step {idx+1}')

            # Track skipped step in metrics
            if metrics_collector:
                metrics_collector.skip_step(name=step_name, index=idx + 1)

            # Track skipped step in progress
            if progress_tracker:
                progress_tracker.skip_step()

            results.append(StepResult(
                index=idx + 1,
                name=step_name,
                passed=True,
                exit_code=0,
                timed_out=False,
                duration_sec=0.0,
                reasons=["Skipped (resume mode)"],
                stdout='',
                stderr=''
            ))
            total += 1
            passed += 1

    # Store total for display in execute_step
    cfg['__total_steps'] = len(steps)

    wave = 0
    steps_to_run = len(steps) - start_step_index
    logger.info(f"\n=== Starting Parallel Execution ({steps_to_run} steps) ===\n")

    while len(completed) + len(blocked_steps) < len(steps):
        wave += 1

        # Identify ready steps (all dependencies satisfied)
        ready = []
        for idx in range(len(steps)):
            if idx in completed or idx in blocked_steps:
                continue

            deps = dependencies.get(idx, set())

            # Check if any dependency failed
            if deps & failed_steps:
                # Block this step because a dependency failed
                blocked_steps.add(idx)
                step_name = steps[idx].get('name', f'Step {idx+1}')
                logger.warning(f"[{idx}] {step_name} - BLOCKED (dependency failed)")
                results.append(StepResult(
                    index=idx + 1,
                    name=step_name,
                    passed=False,
                    exit_code=-1,
                    timed_out=False,
                    duration_sec=0.0,
                    reasons=["Blocked: dependency failed"],
                    stdout='',
                    stderr=''
                ))
                total += 1
                failed += 1
                continue

            # Check if all dependencies completed successfully
            if deps.issubset(completed):
                ready.append(idx)

        if not ready:
            if len(completed) + len(blocked_steps) < len(steps):
                logger.error("ERROR: Circular dependency or unreachable steps!")
                break
            else:
                break

        # Track wave in metrics
        if metrics_collector:
            metrics_collector.increment_wave_count()

        logger.info(f"\n{'='*60}")
        logger.info(f"Wave {wave}: {len(ready)} step(s) executing in parallel")
        logger.info(f"{'='*60}")
        for idx in ready:
            step_name = steps[idx].get('name', f'Step {idx+1}')
            logger.info(f"  [{idx}] {step_name}")
        logger.info("")

        # Execute ready steps in parallel (limit to configured max_workers)
        actual_workers = min(len(ready), max_workers)
        with ThreadPoolExecutor(max_workers=actual_workers) as executor:
            # Track step start for each ready step
            for idx in ready:
                step_name = steps[idx].get('name', f'Step {idx+1}')
                if metrics_collector:
                    metrics_collector.start_step(name=step_name, index=idx + 1)
                if progress_tracker:
                    progress_tracker.start_step(step_index=idx + 1, step_name=step_name)

            future_to_idx = {
                executor.submit(
                    execute_step,
                    idx,
                    steps[idx],
                    cfg,
                    config_dir,
                    default_shell,
                    default_timeout,
                    default_exit_code,
                    args,
                    logger,
                    metrics_collector,
                    progress_tracker
                ): idx
                for idx in ready
            }

            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    result = future.result()
                    results.append(result)
                    total += 1

                    # Track step completion in metrics
                    if metrics_collector:
                        metrics_collector.end_step(
                            success=result.passed,
                            exit_code=result.exit_code,
                            timed_out=result.timed_out,
                            error_message='; '.join(result.reasons) if not result.passed else None
                        )

                    # Track step completion in progress
                    if progress_tracker:
                        progress_tracker.complete_step(success=result.passed)

                    if result.passed:
                        passed += 1
                        completed.add(idx)
                    else:
                        failed += 1
                        failed_steps.add(idx)
                        completed.add(idx)

                        if stop_on_failure:
                            logger.warning("\n=== Early termination (fail-fast enabled) ===")
                            # Block all remaining steps
                            for remaining_idx in range(len(steps)):
                                if remaining_idx not in completed and remaining_idx not in blocked_steps:
                                    blocked_steps.add(remaining_idx)
                                    step_name = steps[remaining_idx].get('name', f'Step {remaining_idx+1}')
                                    results.append(StepResult(
                                        index=remaining_idx + 1,
                                        name=step_name,
                                        passed=False,
                                        exit_code=-1,
                                        timed_out=False,
                                        duration_sec=0.0,
                                        reasons=["Skipped: fail-fast triggered"],
                                        stdout='',
                                        stderr=''
                                    ))
                                    total += 1
                                    failed += 1
                            return total, passed, failed, results

                except Exception as e:
                    logger.error(f"Exception executing step {idx}: {e}")
                    step_name = steps[idx].get('name', f'Step {idx+1}')
                    results.append(StepResult(
                        index=idx + 1,
                        name=step_name,
                        passed=False,
                        exit_code=-1,
                        timed_out=False,
                        duration_sec=0.0,
                        reasons=[f"Exception: {e}"],
                        stdout='',
                        stderr=''
                    ))
                    total += 1
                    failed += 1
                    failed_steps.add(idx)
                    completed.add(idx)

    logger.info(f"\n{'='*60}")
    logger.info(f"Parallel execution completed in {wave} waves")
    logger.info(f"{'='*60}\n")

    return total, passed, failed, results


def print_detailed_report(results: List[StepResult], logger, log_dir: Path = None, rata: str = None):
    """
    Stampa un report dettagliato alla fine della pipeline con lo stato di ogni step,
    i controlli superati/falliti e le dipendenze bloccate.
    Salva anche il report su file con riferimento alla rata.
    """
    # Buffer per salvare su file
    report_lines = []

    def add_line(line):
        logger.info(line)
        report_lines.append(line)

    add_line("\n" + "=" * 100)
    add_line("RESOCONTO FINALE ESECUZIONE PIPELINE")
    add_line("=" * 100)

    # Raggruppa risultati per flusso
    flows = {}
    for result in results:
        # Estrai il flusso dal nome dello step (es. "ORDINARIA - Transfer FILE_UTILITY")
        parts = result.name.split(' - ')
        if len(parts) >= 2:
            flow = parts[0].strip()
            step_type = parts[1].strip() if len(parts) > 1 else ""
        else:
            flow = "ALTRI"
            step_type = result.name

        if flow not in flows:
            flows[flow] = []
        flows[flow].append((step_type, result))

    # Stampa per ogni flusso
    for flow in sorted(flows.keys()):
        add_line(f"\n{'─' * 100}")
        add_line(f"FLUSSO: {flow}")
        add_line(f"{'─' * 100}")

        for step_type, result in flows[flow]:
            if result.passed:
                add_line(f"\n  ✓ {step_type}")
                add_line(f"    Stato: SUCCESSO")
                add_line(f"    Durata: {result.duration_sec:.2f}s")

                # Mostra dettagli preCheck se presenti
                if result.precheck_details:
                    add_line(f"\n    === PRECHECK DETAILS ===")
                    for line in result.precheck_details.split('\n'):
                        add_line(f"    {line}")

                # Mostra dettagli postCheck se presenti
                if result.postcheck_details:
                    add_line(f"\n    === POSTCHECK DETAILS ===")
                    for line in result.postcheck_details.split('\n'):
                        add_line(f"    {line}")

            else:
                # Step fallito
                if "Blocked: dependency failed" in result.reasons:
                    add_line(f"\n  ⊘ {step_type}")
                    add_line(f"    Stato: NON ESEGUITO (dipendenza fallita)")
                    add_line(f"    Motivo: Uno step precedente necessario è fallito")
                else:
                    add_line(f"\n  ✗ {step_type}")
                    add_line(f"    Stato: FALLITO")
                    add_line(f"    Durata: {result.duration_sec:.2f}s")

                    # Mostra controlli falliti
                    if result.reasons:
                        add_line(f"    Controlli falliti:")
                        for reason in result.reasons:
                            if "preCheck failed" in reason:
                                add_line(f"      - PreCheck: {reason.replace('preCheck failed: ', '')}")
                            elif "postCheck failed" in reason:
                                add_line(f"      - PostCheck: {reason.replace('postCheck failed: ', '')}")
                            elif "exit code" in reason:
                                add_line(f"      - Comando: {reason}")
                            else:
                                add_line(f"      - {reason}")

                    # Mostra dettagli preCheck se presenti (anche se fallito)
                    if result.precheck_details:
                        add_line(f"\n    === PRECHECK DETAILS ===")
                        for line in result.precheck_details.split('\n'):
                            add_line(f"    {line}")

                    # Mostra dettagli postCheck se presenti (anche se fallito)
                    if result.postcheck_details:
                        add_line(f"\n    === POSTCHECK DETAILS ===")
                        for line in result.postcheck_details.split('\n'):
                            add_line(f"    {line}")

    add_line(f"\n{'=' * 100}")
    add_line("RIEPILOGO GENERALE")
    add_line("=" * 100)

    total = len(results)
    passed = sum(1 for r in results if r.passed)
    failed = sum(1 for r in results if not r.passed and "Blocked" not in str(r.reasons))
    blocked = sum(1 for r in results if not r.passed and "Blocked" in str(r.reasons))

    add_line(f"  Totale step: {total}")
    add_line(f"  ✓ Successo: {passed}")
    add_line(f"  ✗ Falliti: {failed}")
    add_line(f"  ⊘ Bloccati (dipendenze): {blocked}")
    add_line("=" * 100 + "\n")

    # Salva su file se log_dir è fornito
    if log_dir:
        timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
        rata_suffix = f"_rata{rata}" if rata else ""
        report_file = log_dir / f"report{rata_suffix}_{timestamp}.txt"
        try:
            report_file.write_text('\n'.join(report_lines), encoding='utf-8')
            logger.info(f"Report dettagliato salvato in: {report_file}")
        except Exception as e:
            logger.warning(f"Impossibile salvare report su file: {e}")


def main() -> int:
    ap = argparse.ArgumentParser(description='opiRunner: esegue una pipeline di comandi con verifiche.')
    ap.add_argument('-c', '--config', default='pipeline.json', help='Percorso del file di configurazione JSON')
    ap.add_argument('--fail-fast', action='store_true', help='Interrompe alla prima failure')
    ap.add_argument('--dry-run', action='store_true', help='Mostra i comandi senza eseguirli')
    ap.add_argument('--log-dir', default='runlogs', help='Cartella dove salvare i log')
    ap.add_argument('--default-shell', choices=['powershell', 'cmd', 'sh'], default=None, help='Shell di default per i comandi')
    ap.add_argument('--json-out', default=None, help='Percorso file JSON per il sommario dei risultati')
    ap.add_argument('-v', '--verbose', action='store_true', help='Abilita output verboso')
    ap.add_argument('--version', action='version', version=f'opiRunner {__version__}')
    ap.add_argument('--parallel', action='store_true', help='Abilita esecuzione parallela con gestione automatica dipendenze')
    ap.add_argument('--max-workers', type=int, default=None, help='Numero massimo worker paralleli (default: min(8, CPU+4))')
    ap.add_argument('--start-from', type=int, default=1, help='Inizia esecuzione dallo step N (1-indexed)')
    ap.add_argument('--start-after', type=int, default=None, help='Inizia esecuzione dopo lo step N (riprendi da N+1)')
    # Modalità parametrica (-r, -t, -s)
    ap.add_argument('-r', dest='rata', help='Rata nel formato YYYYMM')
    ap.add_argument('-t', dest='flusso', help='Flusso: ORDINARIA | SPECIALE | RITENUTE')
    ap.add_argument('-s', dest='spesa', help='Spesa: SPT | PDG')
    ap.add_argument('--cwd', dest='cwd', default=None, help='Directory di lavoro per i comandi generati')
    args = ap.parse_args()

    # Riconosce modalità parametrica (-r, -t, -s)
    param_mode = bool(args.rata and args.flusso and args.spesa)

    # Setup logging early (temp log for validation errors)
    log_dir = Path(args.log_dir)
    ensure_dir(log_dir)

    # Cleanup old logs
    deleted_logs = cleanup_old_logs(log_dir)

    run_id = datetime.now().strftime('%Y%m%d-%H%M%S')
    log_file = log_dir / f"run-{run_id}.log"
    logger = setup_logging(log_file, args.verbose)

    if deleted_logs > 0:
        logger.debug(f"Cleaned up {deleted_logs} old log file(s)")

    # Initialize graceful shutdown handler
    global _shutdown_handler
    _shutdown_handler = GracefulShutdown(logger)

    # Costruzione configurazione
    cfg: Dict[str, Any]
    config_path: Optional[Path] = None
    config_label = ''

    if param_mode:
        # Validazioni
        rate = args.rata.strip()
        if not re.fullmatch(r"\d{6}", rate or ''):
            logger.error("'-r' must be in format YYYYMM (6 digits)")
            return 2
        year = int(rate[:4]); month = int(rate[4:])
        if month < 1 or month > 12 or year < MIN_YEAR:
            logger.error(f"'-r' must have month 01..12 and year >= {MIN_YEAR}")
            return 2

        flusso_raw = (args.flusso or '').upper()
        # accetta anche 'RITENUTA' come alias di 'RITENUTE'
        if flusso_raw == 'RITENUTA':
            flusso_raw = 'RITENUTE'
        allowed_t = {'ORDINARIA', 'SPECIALE', 'RITENUTE'}
        if flusso_raw not in allowed_t:
            logger.error(f"'-t' must be one of: {', '.join(allowed_t)}")
            return 2

        spesa_raw = (args.spesa or '').upper()
        allowed_s = {'SPT', 'PDG'}
        if spesa_raw not in allowed_s:
            logger.error(f"'-s' must be one of: {', '.join(allowed_s)}")
            return 2

        if flusso_raw in {'ORDINARIA', 'SPECIALE'}:
            f_values = ['FILE_UTILITY', 'EMISTI', 'FILE_22000X']
        else:  # RITENUTE
            f_values = ['FILE_UTILITY', 'ANASTI', 'FILE_22000X']

        generated_steps = []
        for fval in f_values:
            cmd = f"python3_launcher.sh opi-storage-transfer -r {rate} -t {flusso_raw} -s {spesa_raw} -f {fval}"
            generated_steps.append({
                'name': f"Transfer {fval}",
                'run': cmd,
                'expect': {
                    'exitCode': 0
                }
            })

        cfg = {
            'defaultShell': args.default_shell or 'sh',
            'defaultTimeout': 0,
            'defaultExitCode': 0,
            'stopOnFailure': bool(args.fail_fast),
            'variables': {},
            'steps': generated_steps,
        }
        config_label = '<generated -r/-t/-s>'
        config_dir = Path(args.cwd).resolve() if args.cwd else Path.cwd()
    else:
        config_path = Path(args.config).resolve()
        if not config_path.exists():
            logger.error(f"Configuration file not found: {config_path}")
            return 2
        cfg = read_json(config_path)
        config_label = str(config_path)
        config_dir = config_path.parent

    # Validate configuration structure
    validation_errors = validate_config(cfg)
    if validation_errors:
        logger.error("Configuration validation failed:")
        for err in validation_errors:
            logger.error(f"  - {err}")
        return 2

    # Validate against JSON Schema if available
    if not validate_config_with_schema(cfg, config_path, logger):
        logger.error("Configuration does not match JSON Schema")
        return 2

    stop_on_failure = args.fail_fast or cfg.get('stopOnFailure', False)
    default_shell = args.default_shell or cfg.get('defaultShell') or ('powershell' if os.name == 'nt' else 'sh')

    # Validate default shell
    try:
        validate_shell(default_shell)
    except ValueError as e:
        logger.error(str(e))
        return 2

    default_timeout = int(cfg.get('defaultTimeout', 0) or 0)
    default_exit_code = cfg.get('defaultExitCode', 0)

    # Validate and process start-from / start-after options
    total_steps = len(cfg.get('steps', []))
    start_step_index = 0  # 0-indexed for internal use

    if args.start_after is not None and args.start_from != 1:
        logger.error("Cannot use both --start-from and --start-after")
        return 2

    if args.start_after is not None:
        if args.start_after < 1 or args.start_after >= total_steps:
            logger.error(f"Invalid --start-after value: {args.start_after} (must be 1-{total_steps-1})")
            return 2
        start_step_index = args.start_after  # Start from step N+1 (0-indexed: N)
        logger.info(f"Resuming execution after step #{args.start_after} (starting from step #{start_step_index + 1})")
    elif args.start_from != 1:
        if args.start_from < 1 or args.start_from > total_steps:
            logger.error(f"Invalid --start-from value: {args.start_from} (must be 1-{total_steps})")
            return 2
        start_step_index = args.start_from - 1  # Convert to 0-indexed
        logger.info(f"Starting execution from step #{args.start_from}")

    # Validate max-workers
    if args.max_workers is not None:
        if args.max_workers < 1:
            logger.error(f"Invalid --max-workers value: {args.max_workers} (must be >= 1)")
            return 2

    # Validate MongoDB credentials if any step requires MongoDB checks
    mongodb_required = False
    for step in cfg.get('steps', []):
        expect = step.get('expect', {})
        precheck = expect.get('preCheck', {})
        postcheck = expect.get('postCheck', {})

        if isinstance(precheck, dict) and precheck.get('type', '').startswith('mongodb'):
            mongodb_required = True
            break
        if isinstance(postcheck, dict) and postcheck.get('type', '').startswith('mongodb'):
            mongodb_required = True
            break

    if mongodb_required:
        # Check if MongoDB credentials are available in environment or config variables
        cfg_vars = cfg.get('variables', {})
        mongo_host = cfg_vars.get('MONGO_HOST') or os.environ.get('MONGO_HOST')
        mongo_db = cfg_vars.get('MONGO_DB') or os.environ.get('MONGO_DB')

        if not mongo_host:
            logger.warning("MongoDB checks required but MONGO_HOST not configured (will use 'localhost')")
        if not mongo_db:
            logger.warning("MongoDB checks required but MONGO_DB not configured (will use default)")

        # Verify pymongo is available
        if not MONGODB_CHECKER_AVAILABLE:
            logger.error("MongoDB checks required but pymongo module not available")
            logger.error("Install with: pip install pymongo")
            return 2

    # Validate python3_launcher.sh existence if any step uses it
    launcher_required = False
    for step in cfg.get('steps', []):
        run_cmd = step.get('run', '')
        if 'python3_launcher.sh' in run_cmd:
            launcher_required = True
            break

    if launcher_required:
        # Check if python3_launcher.sh is in PATH or current directory
        launcher_found = False

        # Check if in PATH
        if shutil.which('python3_launcher.sh'):
            launcher_found = True
            logger.debug("python3_launcher.sh found in PATH")
        else:
            # Check in config directory and working directory
            possible_locations = [
                config_dir / 'python3_launcher.sh',
                Path.cwd() / 'python3_launcher.sh'
            ]
            for loc in possible_locations:
                if loc.exists() and loc.is_file():
                    launcher_found = True
                    logger.debug(f"python3_launcher.sh found at: {loc}")
                    break

        if not launcher_found:
            logger.warning("python3_launcher.sh is referenced in steps but not found in PATH or current directory")
            logger.warning("Execution may fail if the script is not available at runtime")

    # Set logger for MongoDB circuit breaker
    if MONGODB_CHECKER_AVAILABLE and set_mongodb_logger:
        set_mongodb_logger(logger)
        logger.debug("MongoDB logger configured")

    # Initialize metrics collector if available
    metrics_collector = None
    if METRICS_AVAILABLE and get_metrics_collector:
        metrics_collector = get_metrics_collector(pipeline_name=config_label, logger=logger)
        logger.debug("Metrics collector initialized")
        # Configure parallel settings
        if args.parallel:
            max_workers = args.max_workers or min(8, (os.cpu_count() or 1) + 4)
            metrics_collector.set_parallel_config(enabled=True, max_workers=max_workers)

    # Initialize progress tracker if available
    progress_tracker = None
    if PROGRESS_TRACKER_AVAILABLE and get_progress_tracker:
        progress_tracker = get_progress_tracker(total_steps=total_steps, pipeline_name=config_label)
        logger.debug("Progress tracker initialized")

    logger.info(f"=== opiRunner v{__version__} | run {run_id} ===")
    logger.info(f"Config: {config_label}")
    logger.info(f"FailFast: {stop_on_failure} | DefaultShell: {default_shell}")
    logger.info(f"Log: {log_file}")
    if args.dry_run:
        logger.info("DRY RUN MODE - Commands will not be executed")
    if args.parallel:
        logger.info("PARALLEL EXECUTION MODE - Using dependency-based wave execution")

    total = 0
    passed = 0
    failed = 0
    results: List[StepResult] = []

    # Extract rata for progressive report
    rata_value = None
    if 'variables' in cfg and 'RATA' in cfg['variables']:
        rata_value = cfg['variables']['RATA']
    elif param_mode and args.rata:
        rata_value = args.rata

    # Use parallel execution if --parallel flag is set
    if args.parallel:
        total, passed, failed, results = run_parallel(
            cfg=cfg,
            config_dir=config_dir,
            default_shell=default_shell,
            default_timeout=default_timeout,
            default_exit_code=default_exit_code,
            stop_on_failure=stop_on_failure,
            args=args,
            logger=logger,
            max_workers=args.max_workers,
            start_step_index=start_step_index,
            metrics_collector=metrics_collector,
            progress_tracker=progress_tracker
        )
    else:
        # Sequential execution using execute_step with progressive report
        cfg['__total_steps'] = len(cfg['steps'])

        # Build dependency graph for sequential execution
        try:
            dependencies = build_dependencies(cfg['steps'])
            failed_steps = set()  # Track failed step indices
        except Exception as e:
            logger.error(f"Failed to build dependency graph: {e}")
            logger.error("Continuing with empty dependencies (no dependency checking)")
            dependencies = {i: set() for i in range(len(cfg['steps']))}
            failed_steps = set()

        # Initialize progressive report writer
        with ProgressiveReportWriter(log_dir=log_dir, rata=rata_value, logger=logger) as report_writer:
            # Skip steps before start_step_index
            if start_step_index > 0:
                logger.info(f"\n=== Skipping steps 1-{start_step_index} (resume from step {start_step_index + 1}) ===\n")
                for step_index in range(start_step_index):
                    step_name = cfg['steps'][step_index].get('name', f'Step {step_index+1}')

                    # Track skipped step in metrics
                    if metrics_collector:
                        metrics_collector.skip_step(name=step_name, index=step_index + 1)

                    # Track skipped step in progress
                    if progress_tracker:
                        progress_tracker.skip_step()

                    skipped_result = StepResult(
                        index=step_index + 1,
                        name=step_name,
                        passed=True,
                        exit_code=0,
                        timed_out=False,
                        duration_sec=0.0,
                        reasons=["Skipped (resume mode)"],
                        stdout='',
                        stderr=''
                    )
                    results.append(skipped_result)
                    total += 1
                    passed += 1

            for step_index, step in enumerate(cfg['steps']):
                if step_index < start_step_index:
                    continue

                step_name = step.get('name', f'Step {step_index+1}')

                # Check if any dependency failed
                deps = dependencies.get(step_index, set())
                if deps & failed_steps:
                    # Block this step because a dependency failed
                    logger.warning(f"[{step_index+1}] {step_name} - BLOCCATO (dipendenza fallita)")

                    blocked_result = StepResult(
                        index=step_index + 1,
                        name=step_name,
                        passed=False,
                        exit_code=-1,
                        timed_out=False,
                        duration_sec=0.0,
                        reasons=["Blocked: dependency failed"],
                        stdout='',
                        stderr=''
                    )
                    results.append(blocked_result)
                    report_writer.write_step_result(blocked_result)
                    total += 1
                    failed += 1
                    failed_steps.add(step_index)

                    # Track in metrics and progress
                    if metrics_collector:
                        metrics_collector.start_step(name=step_name, index=step_index + 1)
                        metrics_collector.end_step(
                            success=False,
                            exit_code=-1,
                            timed_out=False,
                            error_message="Step skipped due to previous failure"
                        )
                    if progress_tracker:
                        progress_tracker.complete_step(success=False)

                    continue

                # Track step start in metrics
                if metrics_collector:
                    metrics_collector.start_step(name=step_name, index=step_index + 1)

                # Track step start in progress
                if progress_tracker:
                    progress_tracker.start_step(step_index=step_index + 1, step_name=step_name)

                result = execute_step(
                    step_index=step_index,
                    step=step,
                    cfg=cfg,
                    config_dir=config_dir,
                    default_shell=default_shell,
                    default_timeout=default_timeout,
                    default_exit_code=default_exit_code,
                    args=args,
                    logger=logger,
                    metrics_collector=metrics_collector,
                    progress_tracker=progress_tracker
                )
                results.append(result)
                total += 1

                # Write step result immediately to progressive report
                report_writer.write_step_result(result)

                # Track step end in metrics
                if metrics_collector:
                    metrics_collector.end_step(
                        success=result.passed,
                        exit_code=result.exit_code,
                        timed_out=result.timed_out,
                        error_message='; '.join(result.reasons) if not result.passed else None
                    )

                # Track step completion in progress
                if progress_tracker:
                    progress_tracker.complete_step(success=result.passed)

                if result.passed:
                    passed += 1
                else:
                    failed += 1
                    failed_steps.add(step_index)  # Track failed step for dependency checking
                    if stop_on_failure:
                        logger.warning("Early termination (fail-fast enabled)")
                        break

    # Finish progress tracker
    if progress_tracker:
        progress_tracker.finish()

    # Finish and print metrics
    if metrics_collector:
        metrics_collector.finish_pipeline()
        metrics_collector.print_summary()

    # Note: Detailed report is now written progressively during execution
    # The print_detailed_report function is kept for backward compatibility but not used in sequential mode
    # For parallel mode, it's still called since progressive reporting is not yet implemented there

    logger.info("\n=== Summary ===")
    logger.info(f"Total: {total} | Passed: {passed} | Failed: {failed}")

    if args.json_out:
        summary = {
            'runId': run_id,
            'config': config_label,  # Fix: use config_label instead of config_path
            'total': total,
            'passed': passed,
            'failed': failed,
            'timestamp': ts(),
            'results': [r.to_dict() for r in results] if args.verbose else []
        }
        try:
            Path(args.json_out).write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
            logger.info(f"Summary written to: {args.json_out}")
        except (IOError, OSError) as e:
            logger.warning(f"Failed to write json-out: {e}")

    # Close MongoDB connection pool
    if MONGODB_CHECKER_AVAILABLE and close_all_mongo_clients:
        try:
            close_all_mongo_clients()
            logger.debug("MongoDB connection pool closed")
        except Exception as e:
            logger.warning(f"Failed to close MongoDB connections: {e}")

    return 0 if failed == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
