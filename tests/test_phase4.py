"""
Phase 4 tests — Build/test/patch (mocked, no real compile).

Tests verify:
  - Project scaffolding (pom.xml / build.gradle.kts generation)
  - Compile invocation (mocked subprocess)
  - Test invocation (mocked subprocess)
  - Critique-and-patch LLM call (mocked)
  - Retry loop logic (compile → patch → recompile)
  - Circuit breaker at max attempts
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def temp_output_dir(tmp_path):
    return str(tmp_path)


@pytest.fixture
def sample_scala_files():
    return {
        "src/main/scala/com/example/spark/App.scala":
            "package com.example.spark\nobject App { def main(args: Array[String]): Unit = {} }",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Project Scaffolding
# ─────────────────────────────────────────────────────────────────────────────

class TestProjectScaffold:

    def test_scaffold_creates_project_dir(self, temp_output_dir, sample_scala_files):
        from src.tools.project_scaffold import scaffold_spark_project
        result = scaffold_spark_project(
            generated_files=sample_scala_files,
            build_tool="maven",
            versions={"spark": "4.0.1", "scala": "2.13.16", "jdk": "17", "hadoop": "3.4.2"},
            output_dir=temp_output_dir,
        )
        project_path = Path(result["project_path"])
        assert project_path.exists()
        assert project_path.is_dir()

    def test_scaffold_writes_scala_files(self, temp_output_dir, sample_scala_files):
        from src.tools.project_scaffold import scaffold_spark_project
        result = scaffold_spark_project(
            generated_files=sample_scala_files,
            build_tool="maven",
            versions={},
            output_dir=temp_output_dir,
        )
        project_path = Path(result["project_path"])
        scala_file = project_path / "src/main/scala/com/example/spark/App.scala"
        assert scala_file.exists()
        content = scala_file.read_text()
        assert "object App" in content

    def test_scaffold_maven_creates_pom(self, temp_output_dir, sample_scala_files):
        from src.tools.project_scaffold import scaffold_spark_project
        result = scaffold_spark_project(
            generated_files=sample_scala_files,
            build_tool="maven",
            versions={"spark": "4.0.1"},
            output_dir=temp_output_dir,
        )
        project_path = Path(result["project_path"])
        pom = project_path / "pom.xml"
        assert pom.exists()
        content = pom.read_text()
        assert "<spark.version>4.0.1</spark.version>" in content
        assert result["build_file"] == "pom.xml"

    def test_scaffold_gradle_creates_build_gradle(self, temp_output_dir, sample_scala_files):
        from src.tools.project_scaffold import scaffold_spark_project
        result = scaffold_spark_project(
            generated_files=sample_scala_files,
            build_tool="gradle",
            versions={"spark": "4.0.1"},
            output_dir=temp_output_dir,
        )
        project_path = Path(result["project_path"])
        gradle_file = project_path / "build.gradle.kts"
        assert gradle_file.exists()
        content = gradle_file.read_text()
        assert "spark-sql" in content
        assert result["build_file"] == "build.gradle.kts"

    def test_scaffold_reports_file_count(self, temp_output_dir, sample_scala_files):
        from src.tools.project_scaffold import scaffold_spark_project
        result = scaffold_spark_project(
            generated_files=sample_scala_files,
            build_tool="maven",
            versions={},
            output_dir=temp_output_dir,
        )
        # 1 Scala file + 1 pom.xml = 2
        assert result["files_written"] == 2


# ─────────────────────────────────────────────────────────────────────────────
# Compile (mocked subprocess)
# ─────────────────────────────────────────────────────────────────────────────

class TestCompile:

    def test_compile_success(self, temp_output_dir):
        from src.tools.build_runner import compile_scala
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "BUILD SUCCESS"
        mock_result.stderr = ""

        with patch("subprocess.run", return_value=mock_result):
            result = compile_scala(temp_output_dir, "maven")

        assert result["success"] is True
        assert result["error_lines"] == []

    def test_compile_failure_extracts_errors(self, temp_output_dir):
        from src.tools.build_runner import compile_scala
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        mock_result.stderr = "[ERROR] cannot find symbol: class NonExistent\n[ERROR] value foo is not a member of Bar"

        with patch("subprocess.run", return_value=mock_result):
            result = compile_scala(temp_output_dir, "maven")

        assert result["success"] is False
        assert len(result["error_lines"]) == 2
        assert "cannot find symbol" in result["error_lines"][0]

    def test_compile_timeout_handled(self, temp_output_dir):
        from src.tools.build_runner import compile_scala
        import subprocess

        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("mvn", 60)):
            result = compile_scala(temp_output_dir, "maven", timeout_seconds=60)

        assert result["success"] is False
        assert "timed out" in result["stderr"].lower()

    def test_compile_command_not_found(self, temp_output_dir):
        from src.tools.build_runner import compile_scala

        with patch("subprocess.run", side_effect=FileNotFoundError("mvn not found")):
            result = compile_scala(temp_output_dir, "maven")

        assert result["success"] is False
        assert "not found" in result["stderr"].lower()


# ─────────────────────────────────────────────────────────────────────────────
# Test Runner (mocked subprocess)
# ─────────────────────────────────────────────────────────────────────────────

class TestRunner:

    def test_run_tests_success(self, temp_output_dir):
        from src.tools.build_runner import run_tests
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "All tests passed"
        mock_result.stderr = ""

        with patch("subprocess.run", return_value=mock_result):
            result = run_tests(temp_output_dir, "maven")

        assert result["passed"] is True

    def test_run_tests_failure(self, temp_output_dir):
        from src.tools.build_runner import run_tests
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        mock_result.stderr = "Test org.example.AppTest failed"

        with patch("subprocess.run", return_value=mock_result):
            result = run_tests(temp_output_dir, "maven")

        assert result["passed"] is False
        assert "failed" in result["details"].lower()

    def test_run_tests_timeout(self, temp_output_dir):
        from src.tools.build_runner import run_tests
        import subprocess

        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("mvn", 120)):
            result = run_tests(temp_output_dir, "maven")

        assert result["passed"] is False
        assert "timed out" in result["details"].lower()


# ─────────────────────────────────────────────────────────────────────────────
# Critique and Patch (mocked LLM)
# ─────────────────────────────────────────────────────────────────────────────

class TestCritiqueAndPatch:

    @pytest.fixture
    def claude_config(self):
        from src.models.state import (
            PipelineConfig, SourceConfig, VersionConfig, BuildTool,
            RiskConfig, CodegenConfig, SourceMode, LLMProvider,
        )
        return PipelineConfig(
            source=SourceConfig(mode=SourceMode.LOCAL, local_path="/tmp"),
            versions=VersionConfig(),
            build_tool=BuildTool.MAVEN,
            risk=RiskConfig(prefixes=[]),
            llm_provider=LLMProvider.CLAUDE,
            model_name="claude-sonnet-4-5-20250929",
        )

    def test_critique_and_patch_returns_patched_files(self, claude_config):
        from src.tools.build_runner import critique_and_patch_llm
        error_lines = ["[ERROR] cannot find symbol: class Foo"]
        files = {"App.scala": "object App {}"}

        llm_response = {
            "explanation": "Added missing import",
            "patched_files": {"App.scala": "import Foo\nobject App {}"}
        }

        with patch("src.tools.build_runner._call_llm") as mock_llm:
            mock_llm.return_value = json.dumps(llm_response)
            result = critique_and_patch_llm(error_lines, files, claude_config)

        assert "patched_files" in result
        assert "App.scala" in result["patched_files"]
        assert "import Foo" in result["patched_files"]["App.scala"]

    def test_critique_and_patch_validates_response(self, claude_config):
        from src.tools.build_runner import critique_and_patch_llm
        # Missing patched_files key
        llm_response = {"explanation": "Did something"}

        with patch("src.tools.build_runner._call_llm") as mock_llm:
            mock_llm.return_value = json.dumps(llm_response)
            with pytest.raises(RuntimeError, match="LLM JSON parse error"):
                critique_and_patch_llm([], {}, claude_config)

    def test_critique_and_patch_caps_error_lines(self, claude_config):
        from src.tools.build_runner import critique_and_patch_llm
        # 20 error lines — should cap to 15
        errors = [f"[ERROR] error {i}" for i in range(20)]
        files = {}

        with patch("src.tools.build_runner._call_llm") as mock_llm:
            mock_llm.return_value = json.dumps({"patched_files": {}})
            critique_and_patch_llm(errors, files, claude_config)
            # Check the prompt sent to LLM
            prompt = mock_llm.call_args[0][1]
            # Should contain at most 15 errors
            assert prompt.count("[ERROR]") <= 15


# ─────────────────────────────────────────────────────────────────────────────
# Full pipeline — compile → patch → recompile loop (mocked)
# ─────────────────────────────────────────────────────────────────────────────

class TestPhase4Pipeline:

    def _initial_state(self, config):
        return {
            "config": config,
            "node_progress": {},
            "errors": [],
            "patch_attempts": 0,
            "human_review_required": False,
            "compile_results": [],
            "risk_items": [],
            "java_classes": [],
            "detected_schemas": [],
            "mr_jobs": [],
            "generated_files": {"App.scala": "object App {}"},
        }

    @pytest.fixture
    def claude_config(self):
        from src.models.state import (
            PipelineConfig, SourceConfig, VersionConfig, BuildTool,
            RiskConfig, CodegenConfig, SourceMode, LLMProvider,
        )
        return PipelineConfig(
            source=SourceConfig(mode=SourceMode.LOCAL, local_path="/tmp"),
            versions=VersionConfig(),
            build_tool=BuildTool.MAVEN,
            llm_provider=LLMProvider.CLAUDE,
        )

    def test_compile_success_skips_patch(self, claude_config):
        from src.agents.graph import pipeline_graph
        from src.models.state import NodeStatus

        compile_ok = MagicMock(returncode=0, stdout="SUCCESS", stderr="")
        test_ok = MagicMock(returncode=0, stdout="Tests passed", stderr="")

        with patch("src.tools.llm_codegen._call_llm") as mock_llm, \
             patch("subprocess.run") as mock_run:
            mock_llm.side_effect = [
                json.dumps({"jobs": [{"job_name": "J", "driver": None, "mapper": None, "reducer": None}],
                           "chain_count": 1, "complexity": "simple"}),
                json.dumps({"files": {"App.scala": "object App {}"}, "file_count": 1, "warnings": []}),
            ]
            mock_run.side_effect = [compile_ok, test_ok]
            state = pipeline_graph.invoke(self._initial_state(claude_config))

        progress = state.get("node_progress", {})
        # compile passed → critique_and_patch should be SKIPPED
        assert "critique_and_patch" not in progress or progress["critique_and_patch"].status == NodeStatus.SKIPPED

    def test_compile_fail_triggers_patch(self, claude_config):
        from src.agents.graph import pipeline_graph

        compile_fail = MagicMock(returncode=1, stdout="", stderr="[ERROR] symbol not found")
        compile_ok = MagicMock(returncode=0, stdout="SUCCESS", stderr="")
        test_ok = MagicMock(returncode=0, stdout="Tests passed", stderr="")

        with patch("src.tools.llm_codegen._call_llm") as mock_llm, \
             patch("subprocess.run") as mock_run:
            mock_llm.side_effect = [
                json.dumps({"jobs": [{"job_name": "J", "driver": None, "mapper": None, "reducer": None}],
                           "chain_count": 1, "complexity": "simple"}),
                json.dumps({"files": {"App.scala": "object App {}"}, "file_count": 1, "warnings": []}),
                json.dumps({"patched_files": {"App.scala": "import Foo\nobject App {}"},
                           "explanation": "Added import"}),
            ]
            mock_run.side_effect = [compile_fail, compile_ok, test_ok]
            state = pipeline_graph.invoke(self._initial_state(claude_config))

        # Should have patched_files in state
        assert "patched_files" in state
        assert state["patch_attempts"] >= 1

    def test_max_patch_attempts_triggers_human_review(self, claude_config):
        from src.agents.graph import pipeline_graph

        compile_fail = MagicMock(returncode=1, stdout="", stderr="[ERROR] persistent error")

        with patch("src.tools.llm_codegen._call_llm") as mock_llm, \
             patch("subprocess.run", return_value=compile_fail):
            mock_llm.side_effect = [
                json.dumps({"jobs": [{"job_name": "J", "driver": None, "mapper": None, "reducer": None}],
                           "chain_count": 1, "complexity": "simple"}),
                json.dumps({"files": {"App.scala": "object App {}"}, "file_count": 1, "warnings": []}),
                # Patch attempts 1, 2, 3 all return the same file
                json.dumps({"patched_files": {"App.scala": "object App {}"}, "explanation": "Try 1"}),
                json.dumps({"patched_files": {"App.scala": "object App {}"}, "explanation": "Try 2"}),
                json.dumps({"patched_files": {"App.scala": "object App {}"}, "explanation": "Try 3"}),
            ]
            state = pipeline_graph.invoke(self._initial_state(claude_config))

        # After 3 failed patch attempts, should escalate to human review
        assert state["patch_attempts"] >= 3
        assert state["human_review_required"] is True
