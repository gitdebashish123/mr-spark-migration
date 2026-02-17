"""
Phase 5 tests — Risk scanning, report generation, final packaging.

All tests use mocked LLM calls and subprocess execution.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


# ─────────────────────────────────────────────────────────────────────────────
# Risk Scanner
# ─────────────────────────────────────────────────────────────────────────────

class TestRiskScanner:

    def test_detects_custom_imports(self):
        from src.tools.risk_scanner import scan_for_risks
        classes = [
            {"class_name": "App", "file_path": "App.java",
             "imports": ["com.company.internal.CustomUtil", "org.apache.spark.sql.SparkSession"]},
        ]
        result = scan_for_risks(classes, [], ["com.company"])
        assert result["total_count"] == 1
        assert result["risk_items"][0]["artifact"] == "com.company.internal.CustomUtil"

    def test_detects_maven_dependencies(self, tmp_path):
        from src.tools.risk_scanner import scan_for_risks
        pom = tmp_path / "pom.xml"
        pom.write_text("""
            <groupId>com.company</groupId>
            <artifactId>custom-lib</artifactId>
        """)
        result = scan_for_risks([], [str(pom)], ["com.company"])
        assert result["total_count"] == 1
        assert "custom-lib" in result["risk_items"][0]["artifact"]

    def test_detects_gradle_dependencies(self, tmp_path):
        from src.tools.risk_scanner import scan_for_risks
        gradle = tmp_path / "build.gradle"
        gradle.write_text('implementation("com.company:custom-lib:1.0")')
        result = scan_for_risks([], [str(gradle)], ["com.company"])
        assert result["total_count"] == 1

    def test_deduplicates_risks(self):
        from src.tools.risk_scanner import scan_for_risks
        # Same import in two files
        classes = [
            {"class_name": "A", "file_path": "A.java", "imports": ["com.company.Util"]},
            {"class_name": "B", "file_path": "B.java", "imports": ["com.company.Util"]},
        ]
        result = scan_for_risks(classes, [], ["com.company"])
        # Should deduplicate by (artifact, location)
        assert result["total_count"] == 2  # Different locations

    def test_empty_prefixes_returns_zero_risks(self):
        from src.tools.risk_scanner import scan_for_risks
        classes = [{"class_name": "A", "imports": ["com.company.Util"]}]
        result = scan_for_risks(classes, [], [])
        assert result["total_count"] == 0

    def test_suggests_alternatives(self):
        from src.tools.risk_scanner import scan_for_risks
        classes = [{"class_name": "A", "file_path": "A.java", "imports": ["org.apache.hadoop.io.Text"]}]
        result = scan_for_risks(classes, [], ["org.apache.hadoop"])
        assert result["total_count"] == 1
        assert "Dataset/DataFrame" in result["risk_items"][0]["suggestion"]

    def test_blocked_when_action_is_block(self):
        from src.tools.risk_scanner import scan_for_risks
        classes = [{"class_name": "A", "imports": ["com.company.Util"]}]
        result = scan_for_risks(classes, [], ["com.company"], risk_action="block")
        assert result["blocked"] is True


# ─────────────────────────────────────────────────────────────────────────────
# Report Generator
# ─────────────────────────────────────────────────────────────────────────────

class TestReportGenerator:

    @pytest.fixture
    def sample_state(self):
        from src.models.state import (
            PipelineConfig, SourceConfig, VersionConfig, BuildTool,
            RiskConfig, SourceMode, LLMProvider, NodeProgress, NodeStatus,
            JavaClass, DetectedSchema, SchemaField, MRJob, RiskItem,
            CompileResult, TestResult,
        )
        return {
            "config": PipelineConfig(
                source=SourceConfig(mode=SourceMode.GIT, repo_url="https://github.com/example/mr-project", branch="main"),
                versions=VersionConfig(),
                build_tool=BuildTool.MAVEN,
                llm_provider=LLMProvider.CLAUDE,
            ),
            "node_progress": {
                "config_validate": NodeProgress(node_name="config_validate", status=NodeStatus.SUCCESS),
                "source_resolve": NodeProgress(node_name="source_resolve", status=NodeStatus.SUCCESS),
            },
            "java_classes": [
                JavaClass(class_name="WordCountDriver", file_path="Driver.java", role="driver"),
                JavaClass(class_name="WordCountMapper", file_path="Mapper.java", role="mapper"),
            ],
            "detected_schemas": [
                DetectedSchema(source_class="CompositeKey", fields=[
                    SchemaField(name="groupId", java_type="Text", spark_type="StringType")
                ], format="writable"),
            ],
            "mr_jobs": [
                MRJob(job_name="WordCountJob", driver=JavaClass(class_name="Driver", file_path="", role="driver"),
                      mapper=JavaClass(class_name="Mapper", file_path="", role="mapper"),
                      reducer=JavaClass(class_name="Reducer", file_path="", role="reducer")),
            ],
            "generated_files": {
                "src/main/scala/com/example/App.scala": "object App {}",
            },
            "compile_results": [CompileResult(success=True, stdout="BUILD SUCCESS", stderr="", error_lines=[])],
            "test_result": TestResult(passed=True, details="All tests passed"),
            "risk_items": [
                RiskItem(matched_prefix="com.company", artifact="com.company:custom-lib", location="pom.xml", is_transitive=False),
            ],
            "errors": [],
            "patch_attempts": 0,
            "human_review_required": False,
        }

    def test_generates_markdown_report(self, sample_state):
        from src.tools.report_generator import generate_migration_report
        report = generate_migration_report(sample_state)
        assert "# MapReduce → Spark Migration Report" in report
        assert isinstance(report, str)
        assert len(report) > 100

    def test_includes_job_summary(self, sample_state):
        from src.tools.report_generator import generate_migration_report
        report = generate_migration_report(sample_state)
        assert "WordCountJob" in report
        assert "Mapper" in report

    def test_includes_risk_items(self, sample_state):
        from src.tools.report_generator import generate_migration_report
        report = generate_migration_report(sample_state)
        assert "Risk Assessment" in report
        assert "custom-lib" in report

    def test_includes_compile_status(self, sample_state):
        from src.tools.report_generator import generate_migration_report
        report = generate_migration_report(sample_state)
        assert "Quality Gate" in report
        assert "✅ PASS" in report

    def test_includes_next_steps(self, sample_state):
        from src.tools.report_generator import generate_migration_report
        report = generate_migration_report(sample_state)
        assert "Next Steps" in report

    def test_handles_failed_compilation(self, sample_state):
        from src.models.state import CompileResult
        from src.tools.report_generator import generate_migration_report
        sample_state["compile_results"] = [
            CompileResult(success=False, stdout="", stderr="ERROR", error_lines=["symbol not found"])
        ]
        report = generate_migration_report(sample_state)
        assert "❌ FAIL" in report
        assert "Fix Compilation Errors" in report

    def test_caps_long_lists(self, sample_state):
        from src.models.state import DetectedSchema, SchemaField
        # Add 20 schemas
        sample_state["detected_schemas"] = [
            DetectedSchema(source_class=f"Schema{i}", fields=[], format="writable")
            for i in range(20)
        ]
        from src.tools.report_generator import generate_migration_report
        report = generate_migration_report(sample_state)
        assert "and 10 more" in report  # Should cap at 10


# ─────────────────────────────────────────────────────────────────────────────
# Full Pipeline Phase 5
# ─────────────────────────────────────────────────────────────────────────────

class TestPhase5Pipeline:

    @pytest.fixture
    def claude_config(self):
        from src.models.state import (
            PipelineConfig, SourceConfig, VersionConfig, BuildTool,
            RiskConfig, SourceMode, LLMProvider,
        )
        return PipelineConfig(
            source=SourceConfig(mode=SourceMode.LOCAL, local_path="/tmp"),
            versions=VersionConfig(),
            build_tool=BuildTool.MAVEN,
            risk=RiskConfig(prefixes=["com.company"]),
            llm_provider=LLMProvider.CLAUDE,
        )

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

    def test_risk_scan_node_runs(self, claude_config):
        from src.agents.graph import pipeline_graph
        from src.models.state import NodeStatus

        with patch("src.tools.llm_codegen._call_llm") as mock_llm, \
             patch("subprocess.run") as mock_run:
            mock_llm.side_effect = [
                json.dumps({"jobs": [{"job_name": "J", "driver": None, "mapper": None, "reducer": None}],
                           "chain_count": 1, "complexity": "simple"}),
                json.dumps({"files": {"App.scala": "object App {}"}, "file_count": 1, "warnings": []}),
            ]
            mock_run.return_value = MagicMock(returncode=0, stdout="SUCCESS", stderr="")
            
            state = pipeline_graph.invoke(self._initial_state(claude_config))

        progress = state.get("node_progress", {})
        assert "risk_scan" in progress
        assert progress["risk_scan"].status == NodeStatus.SUCCESS

    def test_package_export_generates_report(self, claude_config):
        from src.agents.graph import pipeline_graph

        with patch("src.tools.llm_codegen._call_llm") as mock_llm, \
             patch("subprocess.run") as mock_run:
            mock_llm.side_effect = [
                json.dumps({"jobs": [{"job_name": "J", "driver": None, "mapper": None, "reducer": None}],
                           "chain_count": 1, "complexity": "simple"}),
                json.dumps({"files": {"App.scala": "object App {}"}, "file_count": 1, "warnings": []}),
            ]
            mock_run.return_value = MagicMock(returncode=0, stdout="SUCCESS", stderr="")
            
            state = pipeline_graph.invoke(self._initial_state(claude_config))

        assert "migration_report" in state
        assert "Migration Report" in state["migration_report"]
        assert len(state["migration_report"]) > 100

    def test_full_pipeline_end_to_end(self, claude_config):
        from src.agents.graph import pipeline_graph
        from src.models.state import NodeStatus

        with patch("src.tools.llm_codegen._call_llm") as mock_llm, \
             patch("subprocess.run") as mock_run:
            mock_llm.side_effect = [
                json.dumps({"jobs": [{"job_name": "J", "driver": None, "mapper": None, "reducer": None}],
                           "chain_count": 1, "complexity": "simple"}),
                json.dumps({"files": {"App.scala": "object App {}"}, "file_count": 1, "warnings": []}),
            ]
            mock_run.return_value = MagicMock(returncode=0, stdout="SUCCESS", stderr="")
            
            state = pipeline_graph.invoke(self._initial_state(claude_config))

        # Verify all critical keys exist
        assert "migration_report" in state
        assert "risk_items" in state
        assert state.get("errors", []) == []
        
        # Verify final node ran
        progress = state.get("node_progress", {})
        assert "package_export" in progress
        assert progress["package_export"].status == NodeStatus.SUCCESS
