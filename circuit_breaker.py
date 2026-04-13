#!/usr/bin/env python3
"""
Circuit Breaker pattern implementation for opiRunner v1.2.0

Prevents cascade failures by temporarily blocking operations
after a threshold of failures is reached.
"""

import time
from enum import Enum
from typing import Callable, Any, TypeVar, Optional
import logging

T = TypeVar('T')


class CircuitState(Enum):
    """Circuit breaker states"""
    CLOSED = "CLOSED"  # Normal operation
    OPEN = "OPEN"  # Blocking all requests
    HALF_OPEN = "HALF_OPEN"  # Testing if service recovered


class CircuitBreaker:
    """Circuit breaker for protecting external services.

    States:
    - CLOSED: Normal operation, requests pass through
    - OPEN: Too many failures, blocking all requests
    - HALF_OPEN: Testing recovery after timeout

    Usage:
        breaker = CircuitBreaker(failure_threshold=5, timeout=60)

        try:
            result = breaker.call(lambda: mongodb_operation())
        except CircuitBreakerOpen as e:
            logger.error(f"Circuit breaker is open: {e}")
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        timeout: int = 60,
        name: str = "default",
        logger: Optional[logging.Logger] = None
    ):
        """Initialize circuit breaker.

        Args:
            failure_threshold: Number of failures before opening circuit
            timeout: Seconds before attempting recovery (HALF_OPEN)
            name: Name for logging
            logger: Optional logger instance
        """
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.name = name
        self.logger = logger

        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[float] = None
        self.state = CircuitState.CLOSED

    def call(self, func: Callable[[], T]) -> T:
        """Execute function through circuit breaker.

        Args:
            func: Function to execute

        Returns:
            Result from function

        Raises:
            CircuitBreakerOpen: If circuit is open
            Exception: Original exception from func
        """
        if self.state == CircuitState.OPEN:
            # Check if enough time has passed to attempt recovery
            if self.last_failure_time and time.time() - self.last_failure_time > self.timeout:
                self._log_info(f"Circuit breaker '{self.name}' entering HALF_OPEN state (testing recovery)")
                self.state = CircuitState.HALF_OPEN
            else:
                remaining = self.timeout - (time.time() - (self.last_failure_time or 0))
                raise CircuitBreakerOpen(
                    f"Circuit breaker '{self.name}' is OPEN. "
                    f"Retry in {remaining:.0f}s (failures: {self.failure_count}/{self.failure_threshold})"
                )

        try:
            result = func()
            self._on_success()
            return result

        except Exception as e:
            self._on_failure()
            raise

    def _on_success(self):
        """Handle successful execution"""
        self.success_count += 1

        if self.state == CircuitState.HALF_OPEN:
            # Recovery successful, close circuit
            self._log_info(f"Circuit breaker '{self.name}' closing (service recovered)")
            self.state = CircuitState.CLOSED
            self.failure_count = 0
            self.last_failure_time = None

    def _on_failure(self):
        """Handle failed execution"""
        self.failure_count += 1
        self.last_failure_time = time.time()

        if self.state == CircuitState.HALF_OPEN:
            # Recovery attempt failed, reopen circuit
            self._log_warning(f"Circuit breaker '{self.name}' reopening (recovery failed)")
            self.state = CircuitState.OPEN

        elif self.failure_count >= self.failure_threshold:
            # Threshold reached, open circuit
            self._log_error(
                f"Circuit breaker '{self.name}' opening "
                f"(failures: {self.failure_count}/{self.failure_threshold})"
            )
            self.state = CircuitState.OPEN

    def reset(self):
        """Manually reset circuit breaker to CLOSED state"""
        self._log_info(f"Circuit breaker '{self.name}' manually reset")
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None

    def get_state(self) -> dict:
        """Get current circuit breaker state

        Returns:
            Dict with state, failure_count, success_count
        """
        return {
            'name': self.name,
            'state': self.state.value,
            'failure_count': self.failure_count,
            'success_count': self.success_count,
            'failure_threshold': self.failure_threshold,
            'timeout': self.timeout,
            'last_failure_time': self.last_failure_time
        }

    def _log_info(self, msg: str):
        if self.logger:
            self.logger.info(msg)

    def _log_warning(self, msg: str):
        if self.logger:
            self.logger.warning(msg)

    def _log_error(self, msg: str):
        if self.logger:
            self.logger.error(msg)


class CircuitBreakerOpen(Exception):
    """Exception raised when circuit breaker is open"""
    pass


# Global circuit breakers registry
_circuit_breakers: dict[str, CircuitBreaker] = {}


def get_circuit_breaker(
    name: str,
    failure_threshold: int = 5,
    timeout: int = 60,
    logger: Optional[logging.Logger] = None
) -> CircuitBreaker:
    """Get or create a circuit breaker by name.

    Args:
        name: Circuit breaker name (e.g., 'mongodb', 'api')
        failure_threshold: Failures before opening
        timeout: Recovery timeout in seconds
        logger: Optional logger

    Returns:
        CircuitBreaker instance
    """
    if name not in _circuit_breakers:
        _circuit_breakers[name] = CircuitBreaker(
            failure_threshold=failure_threshold,
            timeout=timeout,
            name=name,
            logger=logger
        )
    return _circuit_breakers[name]


def reset_all_circuit_breakers():
    """Reset all circuit breakers to CLOSED state"""
    for breaker in _circuit_breakers.values():
        breaker.reset()


def get_all_circuit_breakers_state() -> dict:
    """Get state of all circuit breakers

    Returns:
        Dict mapping breaker name to state dict
    """
    return {
        name: breaker.get_state()
        for name, breaker in _circuit_breakers.items()
    }
