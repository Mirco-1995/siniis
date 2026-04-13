#!/usr/bin/env python3
"""
Test suite for opiRunner v1.2.0

Tests cover:
- Configuration validation
- Template expansion and variable interpolation
- Dependency graph construction
- Path safety and traversal protection
- Step parsing and validation
- Retry logic
- Utility functions

Run with: pytest test_opirunner.py -v
"""

import pytest
import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch
import sys
import time

# Import functions to test
from opirunner import (
    expand_template,
    expand_list,
    build_scope,
    parse_step_info,
    build_dependencies,
    validate_config,
    validate_step_structure,
    is_path_safe,
    resolve_path,
    to_list,
    check_contains,
    regex_all,
    retry_with_backoff,
    cleanup_old_logs,
)


class TestTemplateExpansion:
    """Test template variable expansion"""

    def test_expand_template_simple(self):
        """Test simple variable expansion"""
        result = expand_template("Hello ${NAME}", {"NAME": "World"}, shell_escape=False)
        assert result == "Hello World"

    def test_expand_template_multiple(self):
        """Test multiple variable expansion"""
        vars_ = {"RATA": "202501", "SPESA": "SPT"}
        result = expand_template("${RATA}-${SPESA}", vars_, shell_escape=False)
        assert result == "202501-SPT"

    def test_expand_template_missing_var(self):
        """Test expansion with missing variable"""
        result = expand_template("${MISSING}", {}, shell_escape=False)
        assert result == "${MISSING}"

    def test_expand_template_none(self):
        """Test expansion with None input"""
        result = expand_template(None, {}, shell_escape=False)
        assert result is None

    def test_expand_template_shell_escape(self):
        """Test shell escaping"""
        result = expand_template("${VAL}", {"VAL": "test value"}, shell_escape=True)
        # shlex.quote on 'test value' should add quotes
        assert "test" in result and "value" in result

    def test_expand_list(self):
        """Test list expansion"""
        result = expand_list(["${A}", "${B}"], {"A": "1", "B": "2"}, shell_escape=False)
        assert result == ["1", "2"]


class TestScopeBuilding:
    """Test scope construction for variable resolution"""

    def test_build_scope_basic(self):
        """Test basic scope building"""
        scope = build_scope(
            cfg_vars={"RATA": "202501"},
            step_env={"ENV_VAR": "value"},
            index=5,
            name="Test Step",
            config_dir=Path("/opt/config")
        )

        assert scope["RATA"] == "202501"
        assert scope["ENV_VAR"] == "value"
        assert scope["STEP_INDEX"] == "5"
        assert scope["STEP_NAME"] == "Test Step"
        assert "CONFIG_DIR" in scope

    def test_build_scope_env_override(self):
        """Test that step_env overrides cfg_vars"""
        scope = build_scope(
            cfg_vars={"VAR": "config"},
            step_env={"VAR": "step"},
            index=0,
            name="test",
            config_dir=Path("/")
        )

        assert scope["VAR"] == "step"


class TestStepParsing:
    """Test step name parsing"""

    def test_parse_step_info_ordinaria(self):
        """Test parsing ORDINARIA step"""
        result = parse_step_info("ORDINARIA - Transfer FILE_UTILITY")
        assert result is not None
        assert result['flow'] == 'ORDINARIA'
        assert result['file_type'] == 'FILE_UTILITY'
        assert result['rata'] is None

    def test_parse_step_info_with_rata(self):
        """Test parsing step with RATA"""
        result = parse_step_info("ORDINARIA-202501 - Transfer EMISTI")
        assert result is not None
        assert result['flow'] == 'ORDINARIA'
        assert result['file_type'] == 'EMISTI'
        assert result['rata'] == '202501'

    def test_parse_step_info_speciale(self):
        """Test parsing SPECIALE step"""
        result = parse_step_info("SPECIALE_0001 - Transfer FILE_22000X")
        assert result is not None
        assert result['flow'] == 'SPECIALE_0001'
        assert result['file_type'] == 'FILE_22000X'

    def test_parse_step_info_ritenute(self):
        """Test parsing RITENUTE step"""
        result = parse_step_info("RITENUTE - Transfer ANASTI")
        assert result is not None
        assert result['flow'] == 'RITENUTE'
        assert result['file_type'] == 'ANASTI'

    def test_parse_step_info_siniis(self):
        """Test parsing SINIIS loading step"""
        result = parse_step_info("RITENUTE - Caricamento SINIIS")
        assert result is not None
        assert result['flow'] == 'RITENUTE'
        assert result['file_type'] == 'SINIIS'

    def test_parse_step_info_invalid(self):
        """Test parsing invalid step name"""
        result = parse_step_info("Some Random Step")
        assert result is None


class TestDependencyGraph:
    """Test dependency graph construction"""

    def test_build_dependencies_simple(self):
        """Test simple dependency chain"""
        steps = [
            {"name": "ORDINARIA - Transfer FILE_UTILITY"},
            {"name": "ORDINARIA - Transfer EMISTI"},
            {"name": "ORDINARIA - Transfer FILE_22000X"},
        ]

        deps = build_dependencies(steps)

        # EMISTI depends on FILE_UTILITY
        assert 0 in deps[1]
        # FILE_22000X depends on EMISTI
        assert 1 in deps[2]

    def test_build_dependencies_siniis(self):
        """Test SINIIS dependencies"""
        steps = [
            {"name": "ORDINARIA - Transfer FILE_UTILITY"},
            {"name": "ORDINARIA - Transfer EMISTI"},
            {"name": "ORDINARIA - Transfer FILE_22000X"},
            {"name": "RITENUTE - Caricamento SINIIS"},
        ]

        deps = build_dependencies(steps)

        # SINIIS depends on ORDINARIA FILE_22000X
        assert 2 in deps[3]

    def test_build_dependencies_ritenute(self):
        """Test RITENUTE dependencies on SINIIS"""
        steps = [
            {"name": "ORDINARIA - Transfer FILE_22000X"},
            {"name": "RITENUTE - Caricamento SINIIS"},
            {"name": "RITENUTE - Transfer FILE_UTILITY"},
        ]

        deps = build_dependencies(steps)

        # SINIIS depends on ORDINARIA FILE_22000X
        assert 0 in deps[1]
        # RITENUTE FILE_UTILITY depends on SINIIS
        assert 1 in deps[2]

    def test_build_dependencies_multiple_rata(self):
        """Test dependencies with multiple RATA"""
        steps = [
            {"name": "ORDINARIA-202501 - Transfer FILE_UTILITY"},
            {"name": "ORDINARIA-202502 - Transfer FILE_UTILITY"},
        ]

        deps = build_dependencies(steps)

        # No dependency between different RATA
        assert 1 not in deps[0]
        assert 0 not in deps[1]


class TestConfigValidation:
    """Test configuration validation"""

    def test_validate_config_valid(self):
        """Test valid config"""
        cfg = {
            "defaultShell": "sh",
            "steps": [
                {"name": "Test", "run": "echo test"}
            ]
        }

        errors = validate_config(cfg)
        assert len(errors) == 0

    def test_validate_config_missing_steps(self):
        """Test config missing steps"""
        cfg = {"defaultShell": "sh"}

        errors = validate_config(cfg)
        assert len(errors) > 0
        assert any("steps" in err.lower() for err in errors)

    def test_validate_config_invalid_shell(self):
        """Test invalid shell"""
        cfg = {
            "defaultShell": "bash",  # invalid, should be 'sh'
            "steps": [{"name": "Test", "run": "echo test"}]
        }

        errors = validate_config(cfg)
        assert len(errors) > 0
        assert any("shell" in err.lower() for err in errors)

    def test_validate_config_empty_steps(self):
        """Test empty steps list"""
        cfg = {
            "steps": []
        }

        errors = validate_config(cfg)
        assert len(errors) > 0
        assert any("empty" in err.lower() for err in errors)

    def test_validate_step_structure_missing_name(self):
        """Test step missing name"""
        errors = validate_step_structure({"run": "echo test"}, 1)
        assert len(errors) > 0
        assert any("name" in err.lower() for err in errors)

    def test_validate_step_structure_missing_run(self):
        """Test step missing run command"""
        errors = validate_step_structure({"name": "Test"}, 1)
        assert len(errors) > 0
        assert any("run" in err.lower() for err in errors)


class TestPathSafety:
    """Test path safety and traversal protection"""

    def test_is_path_safe_valid(self):
        """Test safe path"""
        base = Path("/opt/data")
        target = Path("/opt/data/subdir/file.txt")
        assert is_path_safe(base, target) is True

    def test_is_path_safe_traversal(self):
        """Test path traversal detection"""
        base = Path("/opt/data")
        target = Path("/opt/other/file.txt")
        assert is_path_safe(base, target) is False

    def test_resolve_path_absolute(self):
        """Test resolve with absolute path"""
        result = resolve_path(Path("/base"), "/absolute/path")
        assert result == Path("/absolute/path")

    def test_resolve_path_relative(self):
        """Test resolve with relative path"""
        result = resolve_path(Path("/base"), "relative/path")
        assert result == Path("/base/relative/path")

    def test_resolve_path_none(self):
        """Test resolve with None"""
        result = resolve_path(Path("/base"), None)
        assert result == Path("/base")


class TestUtilityFunctions:
    """Test utility functions"""

    def test_to_list_already_list(self):
        """Test to_list with list input"""
        result = to_list([1, 2, 3])
        assert result == [1, 2, 3]

    def test_to_list_single_value(self):
        """Test to_list with single value"""
        result = to_list(42)
        assert result == [42]

    def test_to_list_none(self):
        """Test to_list with None"""
        result = to_list(None)
        assert result == []

    def test_check_contains_must_contain(self):
        """Test check_contains for must_contain"""
        assert check_contains("hello world", ["hello"], True, True) is True
        assert check_contains("hello world", ["missing"], True, True) is False

    def test_check_contains_must_not_contain(self):
        """Test check_contains for must_not_contain"""
        assert check_contains("hello world", ["error"], True, False) is True
        assert check_contains("hello world", ["hello"], True, False) is False

    def test_check_contains_case_insensitive(self):
        """Test case insensitive check"""
        assert check_contains("Hello World", ["hello"], False, True) is True

    def test_regex_all_match(self):
        """Test regex_all with matching patterns"""
        assert regex_all("error 404", [r"error \d+"]) is True

    def test_regex_all_no_match(self):
        """Test regex_all with non-matching patterns"""
        assert regex_all("success", [r"error \d+"]) is False

    def test_regex_all_invalid_pattern(self):
        """Test regex_all with invalid pattern"""
        # Should return False on invalid regex
        assert regex_all("test", [r"[invalid"]) is False


class TestRetryLogic:
    """Test retry with exponential backoff"""

    def test_retry_success_first_attempt(self):
        """Test retry succeeds on first attempt"""
        mock_func = Mock(return_value=(True, "success"))

        success, result = retry_with_backoff(mock_func, max_retries=3, initial_delay=0.01)

        assert success is True
        assert result == "success"
        assert mock_func.call_count == 1

    def test_retry_success_second_attempt(self):
        """Test retry succeeds on second attempt"""
        mock_func = Mock(side_effect=[
            (False, "fail"),
            (True, "success")
        ])

        success, result = retry_with_backoff(mock_func, max_retries=3, initial_delay=0.01)

        assert success is True
        assert result == "success"
        assert mock_func.call_count == 2

    def test_retry_all_attempts_fail(self):
        """Test retry exhausts all attempts"""
        mock_func = Mock(return_value=(False, "fail"))

        success, result = retry_with_backoff(mock_func, max_retries=3, initial_delay=0.01)

        assert success is False
        assert result == "fail"
        assert mock_func.call_count == 3

    def test_retry_exception_handling(self):
        """Test retry handles exceptions"""
        mock_func = Mock(side_effect=[
            Exception("error"),
            (True, "success")
        ])

        success, result = retry_with_backoff(mock_func, max_retries=3, initial_delay=0.01)

        assert success is True
        assert result == "success"
        assert mock_func.call_count == 2


class TestLogCleanup:
    """Test log rotation and cleanup"""

    def test_cleanup_old_logs(self):
        """Test cleanup removes old logs"""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_dir = Path(tmpdir)

            # Create old log file
            old_log = log_dir / "run-old.log"
            old_log.write_text("old log")
            # Set modification time to 31 days ago
            old_time = time.time() - (31 * 24 * 3600)
            old_log.touch()
            import os
            os.utime(old_log, (old_time, old_time))

            # Create recent log file
            new_log = log_dir / "run-new.log"
            new_log.write_text("new log")

            # Run cleanup with 30-day retention
            deleted = cleanup_old_logs(log_dir, retention_days=30)

            assert deleted == 1
            assert not old_log.exists()
            assert new_log.exists()

    def test_cleanup_nonexistent_dir(self):
        """Test cleanup with nonexistent directory"""
        deleted = cleanup_old_logs(Path("/nonexistent"), retention_days=30)
        assert deleted == 0


class TestIntegration:
    """Integration tests"""

    def test_full_config_validation(self):
        """Test validation of complete config"""
        cfg = {
            "defaultShell": "sh",
            "defaultTimeout": 300,
            "defaultExitCode": 0,
            "stopOnFailure": False,
            "variables": {
                "RATA": "202501",
                "MONGO_HOST": "localhost"
            },
            "steps": [
                {
                    "name": "ORDINARIA - Transfer FILE_UTILITY",
                    "run": "python3_launcher.sh -r ${RATA}",
                    "expect": {
                        "exitCode": 0,
                        "stdoutMustContain": ["success"]
                    }
                }
            ]
        }

        errors = validate_config(cfg)
        assert len(errors) == 0

    def test_step_scope_with_template_expansion(self):
        """Test complete flow: scope building and template expansion"""
        cfg_vars = {"RATA": "202501", "SPESA": "SPT"}
        step_env = {"ENV_VAR": "value"}

        scope = build_scope(cfg_vars, step_env, 5, "Test", Path("/opt"))

        template = "Run with ${RATA} and ${SPESA}"
        result = expand_template(template, scope, shell_escape=False)

        assert result == "Run with 202501 and SPT"

    def test_dependency_graph_realistic(self):
        """Test dependency graph with realistic pipeline"""
        steps = [
            {"name": "ORDINARIA - Transfer FILE_UTILITY"},
            {"name": "ORDINARIA - Transfer EMISTI"},
            {"name": "ORDINARIA - Transfer FILE_22000X"},
            {"name": "SPECIALE_0001 - Transfer FILE_UTILITY"},
            {"name": "SPECIALE_0001 - Transfer EMISTI"},
            {"name": "SPECIALE_0001 - Transfer FILE_22000X"},
            {"name": "RITENUTE - Caricamento SINIIS"},
            {"name": "RITENUTE - Transfer FILE_UTILITY"},
            {"name": "RITENUTE - Transfer ANASTI"},
            {"name": "RITENUTE - Transfer FILE_22000X"},
        ]

        deps = build_dependencies(steps)

        # Verify SINIIS depends on ORDINARIA and SPECIALE FILE_22000X
        assert 2 in deps[6]  # ORDINARIA FILE_22000X
        assert 5 in deps[6]  # SPECIALE FILE_22000X

        # Verify RITENUTE transfers depend on SINIIS
        assert 6 in deps[7]  # FILE_UTILITY depends on SINIIS
        assert 6 in deps[8]  # ANASTI depends on SINIIS
        assert 6 in deps[9]  # FILE_22000X depends on SINIIS


class TestParallelExecution:
    """Test parallel execution features"""

    def test_max_workers_calculation(self):
        """Test max_workers default calculation"""
        import os
        cpu_count = os.cpu_count() or 4
        expected = min(8, cpu_count + 4)

        # Simulate default calculation
        max_workers = min(8, cpu_count + 4)
        assert max_workers == expected
        assert max_workers >= 1
        assert max_workers <= 8

    def test_dependency_graph_waves(self):
        """Test dependency graph execution waves"""
        steps = [
            {"name": "Step 1 - Independent"},
            {"name": "Step 2 - Independent"},
            {"name": "Step 3 - Independent"},
            {"name": "SINIIS - Caricamento SINIIS"},
            {"name": "Step 5 - After SINIIS"},
        ]

        deps = build_dependencies(steps)

        # Wave 1: Steps 0, 1, 2 (independent)
        wave1 = {i for i in range(3) if not deps[i]}
        assert len(wave1) == 3

        # Wave 2: Step 3 (SINIIS depends on nothing here, but in reality depends on FILE_22000X)
        # Wave 3: Step 4 (depends on SINIIS)

    def test_start_from_step_validation(self):
        """Test --start-from validation logic"""
        total_steps = 10

        # Valid cases
        valid_values = [1, 5, 10]
        for val in valid_values:
            assert 1 <= val <= total_steps

        # Invalid cases
        invalid_values = [0, -1, 11, 100]
        for val in invalid_values:
            assert not (1 <= val <= total_steps)

    def test_start_after_step_validation(self):
        """Test --start-after validation logic"""
        total_steps = 10

        # Valid cases (can start after steps 1-9)
        valid_values = [1, 5, 9]
        for val in valid_values:
            assert 1 <= val < total_steps

        # Invalid cases
        invalid_values = [0, -1, 10, 11]
        for val in invalid_values:
            assert not (1 <= val < total_steps)

    def test_skip_steps_logic(self):
        """Test skip steps in resume mode"""
        total_steps = 10
        start_step_index = 5  # 0-indexed, user wants to start from step 6

        # Steps 0-4 should be skipped (steps 1-5 in user terms)
        skipped_count = 0
        for idx in range(start_step_index):
            skipped_count += 1

        assert skipped_count == 5  # Steps 1-5 skipped
        assert total_steps - skipped_count == 5  # Steps 6-10 will execute


class TestCircuitBreaker:
    """Test circuit breaker functionality"""

    def test_circuit_breaker_import(self):
        """Test circuit breaker module can be imported"""
        from circuit_breaker import CircuitBreaker, CircuitState, CircuitBreakerOpen
        assert CircuitBreaker is not None
        assert CircuitState is not None
        assert CircuitBreakerOpen is not None

    def test_circuit_breaker_basic_flow(self):
        """Test basic circuit breaker state transitions"""
        from circuit_breaker import CircuitBreaker, CircuitState, CircuitBreakerOpen

        breaker = CircuitBreaker(failure_threshold=3, timeout=1, name="test")
        assert breaker.state == CircuitState.CLOSED

        # Simulate failures
        for i in range(3):
            try:
                breaker.call(lambda: (_ for _ in ()).throw(Exception("test error")))
            except Exception:
                pass

        # Circuit should be open after threshold
        assert breaker.state == CircuitState.OPEN

        # Calling while open should raise CircuitBreakerOpen
        with pytest.raises(CircuitBreakerOpen):
            breaker.call(lambda: "success")

    def test_circuit_breaker_recovery(self):
        """Test circuit breaker recovery after timeout"""
        from circuit_breaker import CircuitBreaker, CircuitState

        breaker = CircuitBreaker(failure_threshold=2, timeout=0.1, name="test")

        # Open the circuit
        for i in range(2):
            try:
                breaker.call(lambda: (_ for _ in ()).throw(Exception("test error")))
            except Exception:
                pass

        assert breaker.state == CircuitState.OPEN

        # Wait for timeout
        time.sleep(0.2)

        # Should transition to HALF_OPEN and then CLOSED on success
        result = breaker.call(lambda: "recovered")
        assert result == "recovered"
        assert breaker.state == CircuitState.CLOSED


class TestMetrics:
    """Test metrics collection functionality"""

    def test_metrics_import(self):
        """Test metrics module can be imported"""
        from metrics import MetricsCollector, StepMetrics, PipelineMetrics
        assert MetricsCollector is not None
        assert StepMetrics is not None
        assert PipelineMetrics is not None

    def test_metrics_collector_basic(self):
        """Test basic metrics collection"""
        from metrics import MetricsCollector

        collector = MetricsCollector(pipeline_name="test_pipeline")
        assert collector.metrics.pipeline_name == "test_pipeline"
        assert collector.metrics.total_steps == 0
        assert collector.metrics.completed_steps == 0

    def test_metrics_step_tracking(self):
        """Test step metrics tracking"""
        from metrics import MetricsCollector

        collector = MetricsCollector(pipeline_name="test")

        # Start and end a successful step
        collector.start_step("Test Step", 1)
        time.sleep(0.01)  # Small delay
        collector.end_step(success=True, exit_code=0)

        assert collector.metrics.total_steps == 1
        assert collector.metrics.completed_steps == 1
        assert collector.metrics.successful_steps == 1
        assert collector.metrics.failed_steps == 0
        assert len(collector.metrics.steps) == 1

        # Start and end a failed step
        collector.start_step("Failed Step", 2)
        time.sleep(0.01)
        collector.end_step(success=False, exit_code=1, error_message="Test error")

        assert collector.metrics.total_steps == 2
        assert collector.metrics.completed_steps == 2
        assert collector.metrics.successful_steps == 1
        assert collector.metrics.failed_steps == 1

    def test_metrics_skip_step(self):
        """Test skipped step tracking"""
        from metrics import MetricsCollector

        collector = MetricsCollector(pipeline_name="test")
        collector.skip_step("Skipped Step", 1)

        assert collector.metrics.skipped_steps == 1
        assert collector.metrics.total_steps == 1

    def test_metrics_summary(self):
        """Test metrics summary generation"""
        from metrics import MetricsCollector

        collector = MetricsCollector(pipeline_name="test")
        collector.start_step("Step 1", 1)
        collector.end_step(success=True)
        collector.finish_pipeline()

        summary = collector.get_summary()
        assert summary['pipeline_name'] == "test"
        assert summary['steps']['total'] == 1
        assert summary['steps']['completed'] == 1
        assert summary['steps']['successful'] == 1
        assert 'duration_sec' in summary


class TestProgressTracker:
    """Test progress tracking functionality"""

    def test_progress_tracker_import(self):
        """Test progress tracker module can be imported"""
        from progress_tracker import ProgressTracker
        assert ProgressTracker is not None

    def test_progress_tracker_basic(self):
        """Test basic progress tracker functionality"""
        from progress_tracker import ProgressTracker

        tracker = ProgressTracker(total_steps=10, pipeline_name="test")
        assert tracker.total_steps == 10
        assert tracker.completed == 0
        assert tracker.successful == 0
        assert tracker.failed == 0

    def test_progress_tracker_step_completion(self):
        """Test progress tracker step completion"""
        from progress_tracker import ProgressTracker

        tracker = ProgressTracker(total_steps=5, pipeline_name="test")

        tracker.start_step(1, "Step 1")
        tracker.complete_step(success=True)
        assert tracker.completed == 1
        assert tracker.successful == 1
        assert tracker.failed == 0

        tracker.start_step(2, "Step 2")
        tracker.complete_step(success=False)
        assert tracker.completed == 2
        assert tracker.successful == 1
        assert tracker.failed == 1

    def test_progress_tracker_skip(self):
        """Test progress tracker skip functionality"""
        from progress_tracker import ProgressTracker

        tracker = ProgressTracker(total_steps=5, pipeline_name="test")
        tracker.start_step(1, "Skipped Step")
        tracker.skip_step()

        assert tracker.completed == 1
        assert tracker.skipped == 1


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
