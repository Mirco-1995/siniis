#!/usr/bin/env python3
"""
Real-time progress tracking for opiRunner v1.2.0

Provides live progress updates during pipeline execution with:
- Progress bar visualization
- Current step status
- Estimated time remaining
- Success/failure counters
"""

import sys
import time
import threading
from typing import Optional
from datetime import datetime


class ProgressTracker:
    """Real-time progress tracker for pipeline execution"""

    def __init__(self, total_steps: int, pipeline_name: str = "Pipeline"):
        """Initialize progress tracker.

        Args:
            total_steps: Total number of steps in pipeline
            pipeline_name: Name of the pipeline
        """
        self.total_steps = total_steps
        self.pipeline_name = pipeline_name
        self.completed = 0
        self.successful = 0
        self.failed = 0
        self.skipped = 0
        self.current_step_name = ""
        self.current_step_index = 0
        self.start_time = time.time()
        self.lock = threading.Lock()
        self.enabled = sys.stdout.isatty()  # Only enable if output is a terminal

    def start_step(self, step_index: int, step_name: str):
        """Mark a step as started.

        Args:
            step_index: Step index (1-indexed)
            step_name: Step name
        """
        with self.lock:
            self.current_step_index = step_index
            self.current_step_name = step_name
            self._render()

    def complete_step(self, success: bool):
        """Mark current step as completed.

        Args:
            success: Whether step succeeded
        """
        with self.lock:
            self.completed += 1
            if success:
                self.successful += 1
            else:
                self.failed += 1
            self._render()

    def skip_step(self):
        """Mark current step as skipped"""
        with self.lock:
            self.completed += 1
            self.skipped += 1
            self._render()

    def finish(self):
        """Mark pipeline as finished and print final status"""
        with self.lock:
            if self.enabled:
                print()  # New line after progress bar

    def _render(self):
        """Render progress bar and status (internal use only)"""
        if not self.enabled:
            return

        # Calculate progress
        progress = self.completed / self.total_steps if self.total_steps > 0 else 0
        bar_width = 40
        filled = int(bar_width * progress)
        bar = "█" * filled + "░" * (bar_width - filled)

        # Calculate time metrics
        elapsed = time.time() - self.start_time
        if self.completed > 0:
            avg_time_per_step = elapsed / self.completed
            remaining_steps = self.total_steps - self.completed
            eta_seconds = avg_time_per_step * remaining_steps
            eta = self._format_duration(eta_seconds)
        else:
            eta = "N/A"

        # Format step info
        step_info = f"Step {self.current_step_index}/{self.total_steps}"
        if len(self.current_step_name) > 40:
            step_name_short = self.current_step_name[:37] + "..."
        else:
            step_name_short = self.current_step_name

        # Build status line (use ASCII-safe characters for Windows compatibility)
        status = (
            f"\r{self.pipeline_name} [{bar}] {self.completed}/{self.total_steps} "
            f"(OK:{self.successful} | FAIL:{self.failed} | SKIP:{self.skipped}) "
            f"| {step_info}: {step_name_short} "
            f"| ETA: {eta}   "
        )

        # Print status (with carriage return to overwrite)
        sys.stdout.write(status)
        sys.stdout.flush()

    def _format_duration(self, seconds: float) -> str:
        """Format duration in human-readable format.

        Args:
            seconds: Duration in seconds

        Returns:
            Formatted string (e.g., "1h 23m" or "45s")
        """
        if seconds < 60:
            return f"{int(seconds)}s"
        elif seconds < 3600:
            mins = int(seconds // 60)
            secs = int(seconds % 60)
            return f"{mins}m {secs}s"
        else:
            hours = int(seconds // 3600)
            mins = int((seconds % 3600) // 60)
            return f"{hours}h {mins}m"


# Global progress tracker instance
_progress_tracker: Optional[ProgressTracker] = None


def get_progress_tracker(total_steps: int = 0, pipeline_name: str = "Pipeline") -> ProgressTracker:
    """Get or create the global progress tracker.

    Args:
        total_steps: Total number of steps
        pipeline_name: Pipeline name

    Returns:
        ProgressTracker instance
    """
    global _progress_tracker
    if _progress_tracker is None:
        _progress_tracker = ProgressTracker(total_steps, pipeline_name)
    return _progress_tracker


def reset_progress_tracker():
    """Reset the global progress tracker"""
    global _progress_tracker
    _progress_tracker = None
