"""
Phase 3 tests — LLM codegen (mocked, no API keys required).

Tests verify:
  - Job graph inference prompt construction
  - Spark codegen prompt construction
  - JSON response parsing and validation
  - MRJob hydration from LLM output
  - Provider routing (Claude / Gemini)
  - JSON parse error handling and retry logic
  - Full pipeline run with mocked LLM
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "wordcount"

# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

SAMPLE_JOB_GRAPH = {
    "jobs": [
        {
            "job_name": "WordCountJob",
            "driver":   {"class_name": "WordCountDriver",   "role": "driver"},
            "mapper":   {"class_name": "WordCountMapper",   "role": "mapper"},
            "reducer":  {"class_name": "WordCountReducer",  "role": "reducer"},
            "combiner": {"class_name": "WordCountCombiner", "role": "combiner"},
            "partitioner": None,
            "composite_keys": [{"class_name": "CompositeKey", "role": "composite_key"}],
            "input_paths":  ["/data/input"],
            "output_paths": ["/data/output"],
            "input_format":  "TextInputFormat",
            "output_format": "TextOutputFormat",
            "chained_after": None,
        }
    ],
    "chain_count": 1,
    "complexity": "medium",
}

SAMPLE_CODEGEN = {
    "files": {
        "src/main/scala/com/example/spark/driver/WordCountApp.scala":
            "package com.example.spark.driver\nobject WordCountApp { def main(args: Array[String]): Unit = {} }",
        "src/main/scala/com/example/spark/jobs/WordCountJob.scala":
            "package com.example.spark.jobs\nobject WordCountJob { def run(): Unit = {} }",
        "src/main/scala/com/example/spark/service/WordCountService.scala":
            "package com.example.spark.service\nobject WordCountService { def count(): Unit = {} }",
    },
    "file_count": 3,
    "warnings": [],
}


@pytest.fixture
def claude_config():
    from src.models.state import (
        PipelineConfig, SourceConfig, VersionConfig,
        RiskConfig, CodegenConfig, SourceMode, BuildTool, LLMProvider,
    )
    return PipelineConfig(
        source=SourceConfig(mode=SourceMode.LOCAL, local_path=str(FIXTURE_DIR)),
        versions=VersionConfig(),
        build_tool=BuildTool.MAVEN,
        risk=RiskConfig(prefixes=[]),
        llm_provider=LLMProvider.CLAUDE,
        model_name="claude-sonnet-4-5-20250929",
    )


@pytest.fixture
def gemini_config(claude_config):
    from src.models.state import LLMProvider
    cfg = claude_config.model_copy()
    cfg.llm_provider = LLMProvider.GEMINI
    cfg.model_name = "gemini-2.0-flash-exp"
    return cfg


# ─────────────────────────────────────────────────────────────────────────────
# Prompt construction
# ─────────────────────────────────────────────────────────────────────────────

class TestPromptConstruction:

    def test_jobgraph_prompt_contains_classes(self):
        from src.tools.llm_codegen import _build_jobgraph_prompt
        classes = [{"class_name": "WordCountMapper", "role": "mapper",
                    "extends": "Mapper<LongWritable, Text, Text, IntWritable>",
                    "implements": []}]
        schemas = []
        prompt = _build_jobgraph_prompt(classes, schemas, None)
        assert "WordCountMapper" in prompt
        assert "mapper" in prompt

    def test_jobgraph_prompt_contains_schemas(self):
        from src.tools.llm_codegen import _build_jobgraph_prompt
        schemas = [{"source_class": "CompositeKey",
                    "fields": [{"name": "groupId", "java_type": "Text", "spark_type": "StringType"}],
                    "format": "writable"}]
        prompt = _build_jobgraph_prompt([], schemas, None)
        assert "CompositeKey" in prompt
        assert "groupId" in prompt

    def test_jobgraph_prompt_includes_build_file(self):
        from src.tools.llm_codegen import _build_jobgraph_prompt
        prompt = _build_jobgraph_prompt([], [], "<project>...</project>")
        assert "Build File" in prompt
        assert "<project>" in prompt

    def test_codegen_prompt_contains_versions(self):
        from src.tools.llm_codegen import _build_codegen_prompt
        versions = {"spark": "4.0.1", "scala": "2.13.16", "jdk": "17", "hadoop": "3.4.2"}
        prompt = _build_codegen_prompt(SAMPLE_JOB_GRAPH, versions, {})
        assert "4.0.1" in prompt
        assert "2.13.16" in prompt

    def test_codegen_prompt_contains_job_names(self):
        from src.tools.llm_codegen import _build_codegen_prompt
        prompt = _build_codegen_prompt(SAMPLE_JOB_GRAPH, {}, {})
        assert "WordCountJob" in prompt

    def test_codegen_prompt_mentions_test_stubs_when_enabled(self):
        from src.tools.llm_codegen import _build_codegen_prompt
        prompt = _build_codegen_prompt(SAMPLE_JOB_GRAPH, {}, {"generate_tests": True})
        assert "test" in prompt.lower()

    def test_codegen_prompt_no_test_stubs_when_disabled(self):
        from src.tools.llm_codegen import _build_codegen_prompt
        prompt = _build_codegen_prompt(SAMPLE_JOB_GRAPH, {}, {"generate_tests": False})
        assert "test stub" not in prompt.lower()


# ─────────────────────────────────────────────────────────────────────────────
# JSON parsing and validation
# ─────────────────────────────────────────────────────────────────────────────

class TestJsonParsing:

    def test_clean_json_parses(self):
        from src.tools.llm_codegen import _parse_json_response, _validate_job_graph
        raw = json.dumps(SAMPLE_JOB_GRAPH)
        result = _parse_json_response(raw, "test", _validate_job_graph)
        assert result["jobs"][0]["job_name"] == "WordCountJob"

    def test_strips_markdown_fences(self):
        from src.tools.llm_codegen import _parse_json_response, _validate_job_graph
        raw = f"```json\n{json.dumps(SAMPLE_JOB_GRAPH)}\n```"
        result = _parse_json_response(raw, "test", _validate_job_graph)
        assert len(result["jobs"]) == 1

    def test_strips_plain_fences(self):
        from src.tools.llm_codegen import _parse_json_response, _validate_job_graph
        raw = f"```\n{json.dumps(SAMPLE_JOB_GRAPH)}\n```"
        result = _parse_json_response(raw, "test", _validate_job_graph)
        assert "jobs" in result

    def test_invalid_json_raises_after_retries(self):
        from src.tools.llm_codegen import _parse_json_response, _validate_job_graph
        with pytest.raises(RuntimeError, match="LLM JSON parse error"):
            _parse_json_response("not valid json {{{", "test", _validate_job_graph)

    def test_job_graph_validator_fills_defaults(self):
        from src.tools.llm_codegen import _validate_job_graph
        minimal = {"jobs": [{"job_name": "X", "driver": None, "mapper": None, "reducer": None}]}
        result = _validate_job_graph(minimal)
        assert result["jobs"][0]["combiner"] is None
        assert result["jobs"][0]["composite_keys"] == []
        assert result["chain_count"] == 1
        assert result["complexity"] == "medium"

    def test_job_graph_validator_rejects_missing_jobs(self):
        from src.tools.llm_codegen import _validate_job_graph
        with pytest.raises(ValueError, match="Missing 'jobs'"):
            _validate_job_graph({"something": "else"})

    def test_codegen_validator_fills_defaults(self):
        from src.tools.llm_codegen import _validate_codegen
        data = {"files": {"App.scala": "object App {}"}}
        result = _validate_codegen(data)
        assert result["file_count"] == 1
        assert result["warnings"] == []

    def test_codegen_validator_rejects_missing_files(self):
        from src.tools.llm_codegen import _validate_codegen
        with pytest.raises(ValueError, match="Missing 'files'"):
            _validate_codegen({"something": "else"})

    def test_codegen_validator_rejects_non_string_content(self):
        from src.tools.llm_codegen import _validate_codegen
        with pytest.raises(ValueError, match="not a string"):
            _validate_codegen({"files": {"App.scala": 42}})


# ─────────────────────────────────────────────────────────────────────────────
# LLM provider routing (mocked)
# ─────────────────────────────────────────────────────────────────────────────

class TestLLMProviderRouting:

    def test_claude_provider_called(self, claude_config):
        from src.tools.llm_codegen import _call_llm
        with patch("src.tools.llm_codegen._call_claude") as mock_claude:
            mock_claude.return_value = json.dumps(SAMPLE_JOB_GRAPH)
            _call_llm("sys", "user", claude_config, max_tokens=1024)
            mock_claude.assert_called_once()

    def test_gemini_provider_called(self, gemini_config):
        from src.tools.llm_codegen import _call_llm
        with patch("src.tools.llm_codegen._call_gemini") as mock_gemini:
            mock_gemini.return_value = json.dumps(SAMPLE_JOB_GRAPH)
            _call_llm("sys", "user", gemini_config, max_tokens=1024)
            mock_gemini.assert_called_once()

    def test_claude_passes_model_name(self, claude_config):
        from src.tools.llm_codegen import _call_llm
        with patch("src.tools.llm_codegen._call_claude") as mock_claude:
            mock_claude.return_value = json.dumps(SAMPLE_JOB_GRAPH)
            _call_llm("sys", "user", claude_config)
            _, kwargs = mock_claude.call_args
            args = mock_claude.call_args[0]
            assert claude_config.model_name in args

    def test_unknown_provider_raises(self, claude_config):
        from src.tools.llm_codegen import _call_llm
        cfg = claude_config.model_copy()
        cfg.llm_provider = "invalid_provider"  # type: ignore
        with pytest.raises((ValueError, AttributeError)):
            _call_llm("sys", "user", cfg)


# ─────────────────────────────────────────────────────────────────────────────
# Full infer_job_graph_llm and generate_spark_code_llm (mocked LLM)
# ─────────────────────────────────────────────────────────────────────────────

class TestLLMFunctions:

    def test_infer_job_graph_returns_valid_graph(self, claude_config):
        from src.tools.llm_codegen import infer_job_graph_llm
        with patch("src.tools.llm_codegen._call_llm") as mock_llm:
            mock_llm.return_value = json.dumps(SAMPLE_JOB_GRAPH)
            result = infer_job_graph_llm(
                classes=[{"class_name": "WordCountMapper", "role": "mapper",
                          "extends": "Mapper<LongWritable, Text, Text, IntWritable>",
                          "implements": []}],
                schemas=[],
                build_file_content=None,
                config=claude_config,
            )
        assert "jobs" in result
        assert result["jobs"][0]["job_name"] == "WordCountJob"

    def test_infer_job_graph_validates_output(self, claude_config):
        from src.tools.llm_codegen import infer_job_graph_llm
        # Response missing combiner — validator should fill it
        minimal = {"jobs": [{"job_name": "J", "driver": None, "mapper": None, "reducer": None}],
                   "chain_count": 1, "complexity": "simple"}
        with patch("src.tools.llm_codegen._call_llm") as mock_llm:
            mock_llm.return_value = json.dumps(minimal)
            result = infer_job_graph_llm([], [], None, claude_config)
        assert result["jobs"][0]["combiner"] is None

    def test_generate_spark_code_returns_scala_files(self, claude_config):
        from src.tools.llm_codegen import generate_spark_code_llm
        with patch("src.tools.llm_codegen._call_llm") as mock_llm:
            mock_llm.return_value = json.dumps(SAMPLE_CODEGEN)
            result = generate_spark_code_llm(
                job_graph=SAMPLE_JOB_GRAPH,
                versions={"spark": "4.0.1", "scala": "2.13.16", "jdk": "17", "hadoop": "3.4.2"},
                codegen_config={"api_strategy": "dataframe_enforced", "generate_tests": True},
                config=claude_config,
            )
        assert result["file_count"] == 3
        scala_paths = list(result["files"].keys())
        assert all(p.endswith(".scala") for p in scala_paths)

    def test_generate_spark_code_uses_large_token_budget(self, claude_config):
        from src.tools.llm_codegen import generate_spark_code_llm
        with patch("src.tools.llm_codegen._call_llm") as mock_llm:
            mock_llm.return_value = json.dumps(SAMPLE_CODEGEN)
            generate_spark_code_llm(SAMPLE_JOB_GRAPH, {}, {}, claude_config)
            _, kwargs = mock_llm.call_args
            assert mock_llm.call_args[1].get("max_tokens", 0) >= 4096 or \
                   mock_llm.call_args[0][3] >= 4096


# ─────────────────────────────────────────────────────────────────────────────
# Full pipeline — Phase 3 with mocked LLM
# ─────────────────────────────────────────────────────────────────────────────

class TestPhase3Pipeline:

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
            "generated_files": {},
        }

    def test_full_pipeline_with_mocked_llm(self, claude_config):
        from src.agents.graph import pipeline_graph
        from src.models.state import NodeStatus

        with patch("src.tools.llm_codegen._call_llm") as mock_llm:
            # First call → job graph, second call → spark code
            mock_llm.side_effect = [
                json.dumps(SAMPLE_JOB_GRAPH),
                json.dumps(SAMPLE_CODEGEN),
            ]
            state = pipeline_graph.invoke(self._initial_state(claude_config))

        assert state.get("errors", []) == []
        assert state.get("mr_jobs") is not None
        assert len(state["mr_jobs"]) == 1
        assert state["mr_jobs"][0].job_name == "WordCountJob"

    def test_mr_jobs_fully_hydrated(self, claude_config):
        from src.agents.graph import pipeline_graph

        with patch("src.tools.llm_codegen._call_llm") as mock_llm:
            mock_llm.side_effect = [
                json.dumps(SAMPLE_JOB_GRAPH),
                json.dumps(SAMPLE_CODEGEN),
            ]
            state = pipeline_graph.invoke(self._initial_state(claude_config))

        job = state["mr_jobs"][0]
        assert job.driver is not None and job.driver.class_name == "WordCountDriver"
        assert job.mapper is not None and job.mapper.class_name == "WordCountMapper"
        assert job.reducer is not None and job.reducer.class_name == "WordCountReducer"
        assert job.combiner is not None and job.combiner.class_name == "WordCountCombiner"

    def test_generated_files_are_scala(self, claude_config):
        from src.agents.graph import pipeline_graph

        with patch("src.tools.llm_codegen._call_llm") as mock_llm:
            mock_llm.side_effect = [
                json.dumps(SAMPLE_JOB_GRAPH),
                json.dumps(SAMPLE_CODEGEN),
            ]
            state = pipeline_graph.invoke(self._initial_state(claude_config))

        files = state.get("generated_files", {})
        assert len(files) == 3
        assert all(p.endswith(".scala") for p in files.keys())

    def test_jobgraph_infer_node_success(self, claude_config):
        from src.agents.graph import pipeline_graph
        from src.models.state import NodeStatus

        with patch("src.tools.llm_codegen._call_llm") as mock_llm:
            mock_llm.side_effect = [
                json.dumps(SAMPLE_JOB_GRAPH),
                json.dumps(SAMPLE_CODEGEN),
            ]
            state = pipeline_graph.invoke(self._initial_state(claude_config))

        progress = state.get("node_progress", {})
        ji = progress.get("jobgraph_infer")
        assert ji is not None
        assert ji.status == NodeStatus.SUCCESS
        assert "1 MR job" in ji.message

    def test_spark_codegen_node_success(self, claude_config):
        from src.agents.graph import pipeline_graph
        from src.models.state import NodeStatus

        with patch("src.tools.llm_codegen._call_llm") as mock_llm:
            mock_llm.side_effect = [
                json.dumps(SAMPLE_JOB_GRAPH),
                json.dumps(SAMPLE_CODEGEN),
            ]
            state = pipeline_graph.invoke(self._initial_state(claude_config))

        progress = state.get("node_progress", {})
        sc = progress.get("spark_codegen")
        assert sc is not None
        assert sc.status == NodeStatus.SUCCESS

    def test_llm_json_error_surfaced_in_state(self, claude_config):
        from src.agents.graph import pipeline_graph
        from src.models.state import NodeStatus

        with patch("src.tools.llm_codegen._call_llm") as mock_llm:
            mock_llm.side_effect = [
                "this is not json at all !!!",  # jobgraph fails
                json.dumps(SAMPLE_CODEGEN),
            ]
            state = pipeline_graph.invoke(self._initial_state(claude_config))

        # Node should be FAILED and error surfaced
        progress = state.get("node_progress", {})
        ji = progress.get("jobgraph_infer")
        assert ji is not None
        assert ji.status == NodeStatus.FAILED
