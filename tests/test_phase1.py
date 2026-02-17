"""
Phase 1 smoke tests — verify the full LangGraph pipeline runs end-to-end
with stub tools without errors. No LLM calls, no file system access required.
"""
from __future__ import annotations

import pytest
from src.models.state import (
    PipelineConfig, SourceConfig, VersionConfig, RiskConfig,
    SourceMode, BuildTool, NodeStatus,
)
from src.agents.graph import pipeline_graph


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def git_config() -> PipelineConfig:
    return PipelineConfig(
        source=SourceConfig(
            mode=SourceMode.GIT,
            repo_url="https://github.com/example/wordcount.git",
            branch="main",
        ),
        versions=VersionConfig(),
        build_tool=BuildTool.MAVEN,
        risk=RiskConfig(prefixes=["com.company-name"]),
    )


@pytest.fixture
def local_config() -> PipelineConfig:
    return PipelineConfig(
        source=SourceConfig(
            mode=SourceMode.LOCAL,
            local_path="/tmp/fake-mr-project",
        ),
        versions=VersionConfig(),
        build_tool=BuildTool.GRADLE,
        risk=RiskConfig(prefixes=["com.internal"]),
    )


def _initial_state(config: PipelineConfig) -> dict:
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
        "generated_files": {},
    }


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestPhase1GraphExecution:

    def test_git_pipeline_runs_without_error(self, git_config):
        """Full pipeline runs end-to-end with git config and no exceptions."""
        state = pipeline_graph.invoke(_initial_state(git_config))
        assert state is not None
        assert state.get("errors", []) == [], f"Unexpected errors: {state.get('errors')}"

    def test_local_pipeline_runs_without_error(self, local_config):
        """Full pipeline runs end-to-end with local config."""
        state = pipeline_graph.invoke(_initial_state(local_config))
        assert state is not None

    def test_all_main_nodes_complete(self, git_config):
        """All non-parallel-terminal nodes reach SUCCESS status."""
        state = pipeline_graph.invoke(_initial_state(git_config))
        progress = state.get("node_progress", {})

        expected_success = [
            "config_validate", "source_resolve",
            "driver_analysis", "schema_extract",
            "jobgraph_infer", "spark_codegen",
            "risk_scan", "project_scaffold",
            "compile_check", "test_check",
            "package_export",
        ]
        for node in expected_success:
            assert node in progress, f"Node '{node}' missing from progress"
            status = progress[node].status
            assert status == NodeStatus.SUCCESS, (
                f"Node '{node}' expected SUCCESS, got {status}: {progress[node].error}"
            )

    def test_java_classes_populated(self, git_config):
        """driver_analysis node populates java_classes."""
        state = pipeline_graph.invoke(_initial_state(git_config))
        classes = state.get("java_classes", [])
        assert len(classes) > 0, "No java classes found"
        roles = {c.role for c in classes}
        assert "driver" in roles, "No driver class detected"
        assert "mapper" in roles, "No mapper class detected"
        assert "reducer" in roles, "No reducer class detected"

    def test_schemas_extracted(self, git_config):
        """
        schema_extract node runs without error.
        Phase 1 uses a fake git path so no real .java or .avsc files exist —
        the real extractor correctly returns 0 schemas. We verify the node
        ran (SUCCESS status) and the state key is present, not that schemas > 0.
        Schema content is validated in test_phase2.py against real fixture files.
        """
        state = pipeline_graph.invoke(_initial_state(git_config))
        progress = state.get("node_progress", {})
        assert "schema_extract" in progress, "schema_extract node did not run"
        from src.models.state import NodeStatus
        assert progress["schema_extract"].status == NodeStatus.SUCCESS
        # detected_schemas key must exist (may be empty list for stub paths)
        assert "detected_schemas" in state

    def test_mr_jobs_inferred(self, git_config):
        """jobgraph_infer node populates mr_jobs."""
        state = pipeline_graph.invoke(_initial_state(git_config))
        jobs = state.get("mr_jobs", [])
        assert len(jobs) > 0, "No MR jobs inferred"
        assert jobs[0].job_name == "WordCountJob"

    def test_scala_files_generated(self, git_config):
        """spark_codegen node produces Scala files."""
        state = pipeline_graph.invoke(_initial_state(git_config))
        files = state.get("generated_files", {})
        assert len(files) > 0, "No Scala files generated"
        # All generated files should have Scala extension
        for path in files:
            assert path.endswith(".scala"), f"Unexpected file: {path}"

    def test_risk_items_detected(self, git_config):
        """risk_scan finds the stub risk item for com.company-name prefix."""
        state = pipeline_graph.invoke(_initial_state(git_config))
        risks = state.get("risk_items", [])
        assert len(risks) > 0, "Expected at least one risk item from stub"
        assert any(r.matched_prefix == "com.company-name" for r in risks)

    def test_compile_succeeds(self, git_config):
        """compile_check stub always succeeds in Phase 1."""
        state = pipeline_graph.invoke(_initial_state(git_config))
        compile_results = state.get("compile_results", [])
        assert len(compile_results) > 0
        assert compile_results[-1].success is True

    def test_test_check_passes(self, git_config):
        """test_check stub passes in Phase 1."""
        state = pipeline_graph.invoke(_initial_state(git_config))
        result = state.get("test_result")
        assert result is not None
        assert result.passed is True
        assert result.mr_row_count == result.spark_row_count

    def test_artifact_produced(self, git_config):
        """package_export produces an artifact path and migration report."""
        state = pipeline_graph.invoke(_initial_state(git_config))
        assert state.get("artifact_path") is not None
        report = state.get("migration_report")
        assert report is not None
        assert "WordCountJob" in report

    def test_no_human_review_on_clean_run(self, git_config):
        """Clean stub run should not require human review."""
        state = pipeline_graph.invoke(_initial_state(git_config))
        assert state.get("human_review_required") is False

    def test_config_validate_catches_missing_branch(self):
        """config_validate should record error when git branch is missing."""
        bad_config = PipelineConfig(
            source=SourceConfig(
                mode=SourceMode.GIT,
                repo_url="https://github.com/example/repo.git",
                branch=None,   # intentionally missing
            ),
        )
        # Even with bad config, graph should not raise — errors in state
        state = pipeline_graph.invoke(_initial_state(bad_config))
        errors = state.get("errors", [])
        assert len(errors) > 0, "Expected validation error for missing branch"

    def test_node_duration_recorded(self, git_config):
        """Completed nodes should have duration_ms set."""
        state = pipeline_graph.invoke(_initial_state(git_config))
        progress = state.get("node_progress", {})
        node = progress.get("source_resolve")
        assert node is not None
        assert node.duration_ms is not None
        assert node.duration_ms >= 0
