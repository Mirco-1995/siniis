#!/usr/bin/env python3
"""
Metrics collection and reporting for opiRunner v1.2.0

Tracks execution metrics including:
- Step execution times and success rates
- MongoDB operation metrics
- Circuit breaker state
- Pipeline execution summary
"""

import time
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class StepMetrics:
    """Metrics for a single step execution"""
    name: str
    index: int
    start_time: float
    end_time: Optional[float] = None
    duration_sec: float = 0.0
    success: bool = False
    exit_code: int = 0
    timed_out: bool = False
    error_message: Optional[str] = None


@dataclass
class PipelineMetrics:
    """Metrics for entire pipeline execution"""
    pipeline_name: str
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    duration_sec: float = 0.0

    # Step metrics
    steps: List[StepMetrics] = field(default_factory=list)
    total_steps: int = 0
    completed_steps: int = 0
    successful_steps: int = 0
    failed_steps: int = 0
    skipped_steps: int = 0
    timed_out_steps: int = 0

    # Parallel execution metrics
    parallel_enabled: bool = False
    max_workers: int = 1
    waves_executed: int = 0

    # MongoDB metrics
    mongodb_queries: int = 0
    mongodb_errors: int = 0
    mongodb_connection_pool_hits: int = 0
    mongodb_connection_pool_misses: int = 0

    # Circuit breaker metrics
    circuit_breaker_states: Dict[str, str] = field(default_factory=dict)


class MetricsCollector:
    """Collects and manages pipeline execution metrics"""

    def __init__(self, pipeline_name: str, logger: Optional[logging.Logger] = None):
        """Initialize metrics collector.

        Args:
            pipeline_name: Name of the pipeline
            logger: Optional logger instance
        """
        self.logger = logger
        self.metrics = PipelineMetrics(pipeline_name=pipeline_name)
        self._current_step: Optional[StepMetrics] = None

    def set_parallel_config(self, enabled: bool, max_workers: int):
        """Set parallel execution configuration.

        Args:
            enabled: Whether parallel execution is enabled
            max_workers: Maximum number of workers
        """
        self.metrics.parallel_enabled = enabled
        self.metrics.max_workers = max_workers

    def start_step(self, name: str, index: int):
        """Start tracking a new step.

        Args:
            name: Step name
            index: Step index (1-indexed)
        """
        if self._current_step:
            self._log_warning(f"Step '{self._current_step.name}' not finished before starting '{name}'")

        self._current_step = StepMetrics(
            name=name,
            index=index,
            start_time=time.time()
        )
        self.metrics.total_steps = max(self.metrics.total_steps, index)

    def end_step(self, success: bool, exit_code: int = 0, timed_out: bool = False,
                 error_message: Optional[str] = None):
        """End tracking current step.

        Args:
            success: Whether step succeeded
            exit_code: Step exit code
            timed_out: Whether step timed out
            error_message: Optional error message
        """
        if not self._current_step:
            self._log_warning("end_step called without active step")
            return

        self._current_step.end_time = time.time()
        self._current_step.duration_sec = self._current_step.end_time - self._current_step.start_time
        self._current_step.success = success
        self._current_step.exit_code = exit_code
        self._current_step.timed_out = timed_out
        self._current_step.error_message = error_message

        # Add to metrics
        self.metrics.steps.append(self._current_step)
        self.metrics.completed_steps += 1

        if success:
            self.metrics.successful_steps += 1
        else:
            self.metrics.failed_steps += 1

        if timed_out:
            self.metrics.timed_out_steps += 1

        self._current_step = None

    def skip_step(self, name: str, index: int):
        """Mark a step as skipped (resume mode).

        Args:
            name: Step name
            index: Step index (1-indexed)
        """
        step = StepMetrics(
            name=name,
            index=index,
            start_time=time.time(),
            end_time=time.time(),
            duration_sec=0.0,
            success=True
        )

        self.metrics.steps.append(step)
        self.metrics.skipped_steps += 1
        self.metrics.total_steps = max(self.metrics.total_steps, index)

    def increment_wave_count(self):
        """Increment the number of parallel execution waves"""
        self.metrics.waves_executed += 1

    def record_mongodb_query(self):
        """Record a MongoDB query execution"""
        self.metrics.mongodb_queries += 1

    def record_mongodb_error(self):
        """Record a MongoDB error"""
        self.metrics.mongodb_errors += 1

    def record_mongodb_pool_hit(self):
        """Record a MongoDB connection pool hit"""
        self.metrics.mongodb_connection_pool_hits += 1

    def record_mongodb_pool_miss(self):
        """Record a MongoDB connection pool miss"""
        self.metrics.mongodb_connection_pool_misses += 1

    def update_circuit_breaker_state(self, name: str, state: str):
        """Update circuit breaker state.

        Args:
            name: Circuit breaker name
            state: Circuit breaker state (CLOSED, OPEN, HALF_OPEN)
        """
        self.metrics.circuit_breaker_states[name] = state

    def finish_pipeline(self):
        """Mark pipeline as finished and calculate final metrics"""
        self.metrics.end_time = time.time()
        self.metrics.duration_sec = self.metrics.end_time - self.metrics.start_time

    def get_summary(self) -> Dict[str, Any]:
        """Get metrics summary as dictionary.

        Returns:
            Dict with all metrics
        """
        return {
            'pipeline_name': self.metrics.pipeline_name,
            'start_time': datetime.fromtimestamp(self.metrics.start_time).isoformat(),
            'end_time': datetime.fromtimestamp(self.metrics.end_time).isoformat() if self.metrics.end_time else None,
            'duration_sec': self.metrics.duration_sec,
            'duration_formatted': self._format_duration(self.metrics.duration_sec),
            'steps': {
                'total': self.metrics.total_steps,
                'completed': self.metrics.completed_steps,
                'successful': self.metrics.successful_steps,
                'failed': self.metrics.failed_steps,
                'skipped': self.metrics.skipped_steps,
                'timed_out': self.metrics.timed_out_steps,
                'success_rate': f"{self.metrics.successful_steps / self.metrics.total_steps * 100:.1f}%" if self.metrics.total_steps > 0 else "N/A"
            },
            'parallel': {
                'enabled': self.metrics.parallel_enabled,
                'max_workers': self.metrics.max_workers,
                'waves_executed': self.metrics.waves_executed
            },
            'mongodb': {
                'queries': self.metrics.mongodb_queries,
                'errors': self.metrics.mongodb_errors,
                'error_rate': f"{self.metrics.mongodb_errors / self.metrics.mongodb_queries * 100:.1f}%" if self.metrics.mongodb_queries > 0 else "N/A",
                'pool_hits': self.metrics.mongodb_connection_pool_hits,
                'pool_misses': self.metrics.mongodb_connection_pool_misses,
                'pool_hit_rate': f"{self.metrics.mongodb_connection_pool_hits / (self.metrics.mongodb_connection_pool_hits + self.metrics.mongodb_connection_pool_misses) * 100:.1f}%" if (self.metrics.mongodb_connection_pool_hits + self.metrics.mongodb_connection_pool_misses) > 0 else "N/A"
            },
            'circuit_breakers': self.metrics.circuit_breaker_states,
            'step_details': [
                {
                    'index': step.index,
                    'name': step.name,
                    'duration_sec': step.duration_sec,
                    'duration_formatted': self._format_duration(step.duration_sec),
                    'success': step.success,
                    'exit_code': step.exit_code,
                    'timed_out': step.timed_out,
                    'error_message': step.error_message
                }
                for step in self.metrics.steps
            ]
        }

    def print_summary(self):
        """Print formatted metrics summary"""
        summary = self.get_summary()

        self._log_info("")
        self._log_info("=" * 80)
        self._log_info("PIPELINE EXECUTION METRICS")
        self._log_info("=" * 80)
        self._log_info(f"Pipeline: {summary['pipeline_name']}")
        self._log_info(f"Start time: {summary['start_time']}")
        self._log_info(f"End time: {summary['end_time']}")
        self._log_info(f"Total duration: {summary['duration_formatted']}")
        self._log_info("")

        # Steps summary
        steps = summary['steps']
        self._log_info("Steps Summary:")
        self._log_info(f"  Total steps: {steps['total']}")
        self._log_info(f"  Completed: {steps['completed']}")
        self._log_info(f"  Successful: {steps['successful']}")
        self._log_info(f"  Failed: {steps['failed']}")
        self._log_info(f"  Skipped: {steps['skipped']}")
        self._log_info(f"  Timed out: {steps['timed_out']}")
        self._log_info(f"  Success rate: {steps['success_rate']}")
        self._log_info("")

        # Parallel execution
        parallel = summary['parallel']
        if parallel['enabled']:
            self._log_info("Parallel Execution:")
            self._log_info(f"  Max workers: {parallel['max_workers']}")
            self._log_info(f"  Waves executed: {parallel['waves_executed']}")
            self._log_info("")

        # MongoDB
        mongodb = summary['mongodb']
        if mongodb['queries'] > 0:
            self._log_info("MongoDB Operations:")
            self._log_info(f"  Queries: {mongodb['queries']}")
            self._log_info(f"  Errors: {mongodb['errors']}")
            self._log_info(f"  Error rate: {mongodb['error_rate']}")
            self._log_info(f"  Pool hits: {mongodb['pool_hits']}")
            self._log_info(f"  Pool misses: {mongodb['pool_misses']}")
            self._log_info(f"  Pool hit rate: {mongodb['pool_hit_rate']}")
            self._log_info("")

        # Circuit breakers
        if summary['circuit_breakers']:
            self._log_info("Circuit Breakers:")
            for name, state in summary['circuit_breakers'].items():
                self._log_info(f"  {name}: {state}")
            self._log_info("")

        # Top 5 slowest steps
        slowest = sorted(summary['step_details'], key=lambda s: s['duration_sec'], reverse=True)[:5]
        if slowest:
            self._log_info("Top 5 Slowest Steps:")
            for i, step in enumerate(slowest, 1):
                # Use ASCII-safe characters for better Windows compatibility
                status = "OK" if step['success'] else "FAIL"
                self._log_info(f"  {i}. [{status}] Step {step['index']}: {step['name']} - {step['duration_formatted']}")
            self._log_info("")

        self._log_info("=" * 80)

    def _format_duration(self, seconds: float) -> str:
        """Format duration in human-readable format.

        Args:
            seconds: Duration in seconds

        Returns:
            Formatted string (e.g., "1h 23m 45s")
        """
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            mins = int(seconds // 60)
            secs = seconds % 60
            return f"{mins}m {secs:.1f}s"
        else:
            hours = int(seconds // 3600)
            mins = int((seconds % 3600) // 60)
            secs = seconds % 60
            return f"{hours}h {mins}m {secs:.1f}s"

    def _log_info(self, msg: str):
        if self.logger:
            self.logger.info(msg)
        else:
            print(msg)

    def _log_warning(self, msg: str):
        if self.logger:
            self.logger.warning(msg)
        else:
            print(f"WARNING: {msg}")


# Global metrics collector instance
_metrics_collector: Optional[MetricsCollector] = None


def get_metrics_collector(pipeline_name: str = "default",
                         logger: Optional[logging.Logger] = None) -> MetricsCollector:
    """Get or create the global metrics collector.

    Args:
        pipeline_name: Pipeline name
        logger: Optional logger instance

    Returns:
        MetricsCollector instance
    """
    global _metrics_collector
    if _metrics_collector is None:
        _metrics_collector = MetricsCollector(pipeline_name, logger)
    return _metrics_collector


def reset_metrics_collector():
    """Reset the global metrics collector"""
    global _metrics_collector
    _metrics_collector = None
