"""
Phase 2 tests — real static analysis (no LLM, no network required).

Tests cover:
  - Local directory scanning (git_resolver.scan_directory)
  - Java AST parsing + role classification (java_analyzer.analyze_files)
  - Schema extraction from WritableComparable fields (schema_extractor)
  - Avro schema file parsing (schema_extractor)
  - Full pipeline with a real local source (PipelineState end-to-end)
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Make src/ importable
sys.path.insert(0, str(Path(__file__).parent.parent))

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "wordcount"
JAVA_DIR = FIXTURE_DIR / "src" / "main" / "java"


# ─────────────────────────────────────────────────────────────────────────────
# git_resolver — scan_directory
# ─────────────────────────────────────────────────────────────────────────────

class TestScanDirectory:

    def test_finds_all_java_files(self):
        from src.tools.git_resolver import scan_directory
        result = scan_directory(str(FIXTURE_DIR))
        assert result["java_count"] == 5
        paths = [f["path"] for f in result["files"]]
        assert any("WordCountDriver" in p for p in paths)
        assert any("WordCountMapper" in p for p in paths)
        assert any("CompositeKey" in p for p in paths)

    def test_finds_build_files(self):
        from src.tools.git_resolver import scan_directory
        result = scan_directory(str(FIXTURE_DIR))
        assert "pom.xml" in result["build_files"]

    def test_abs_path_included(self):
        from src.tools.git_resolver import scan_directory
        result = scan_directory(str(FIXTURE_DIR))
        for f in result["files"]:
            assert "abs_path" in f
            assert Path(f["abs_path"]).exists()

    def test_skips_build_dirs(self, tmp_path):
        from src.tools.git_resolver import scan_directory
        # create a fake target/ dir with a .java file
        (tmp_path / "target").mkdir()
        (tmp_path / "target" / "Generated.java").write_text("class Generated {}")
        (tmp_path / "Real.java").write_text("class Real {}")
        result = scan_directory(str(tmp_path), skip_build_dirs=True)
        names = [Path(f["path"]).name for f in result["files"]]
        assert "Real.java" in names
        assert "Generated.java" not in names

    def test_raises_on_missing_path(self):
        from src.tools.git_resolver import scan_directory
        with pytest.raises(RuntimeError, match="does not exist"):
            scan_directory("/nonexistent/path/xyz")

    def test_local_path_validation(self):
        from src.tools.git_resolver import resolve_local_source
        path = resolve_local_source(str(FIXTURE_DIR))
        assert path == str(FIXTURE_DIR)

    def test_local_path_subdir(self):
        from src.tools.git_resolver import resolve_local_source
        path = resolve_local_source(str(FIXTURE_DIR), subdir="src/main/java")
        assert "src/main/java" in path
        assert Path(path).exists()

    def test_local_path_missing_raises(self):
        from src.tools.git_resolver import resolve_local_source
        with pytest.raises(RuntimeError, match="does not exist"):
            resolve_local_source("/no/such/path")

    def test_local_subdir_missing_raises(self):
        from src.tools.git_resolver import resolve_local_source
        with pytest.raises(RuntimeError, match="not found"):
            resolve_local_source(str(FIXTURE_DIR), subdir="nonexistent_subdir")


# ─────────────────────────────────────────────────────────────────────────────
# java_analyzer — analyze_files
# ─────────────────────────────────────────────────────────────────────────────

class TestJavaAnalyzer:

    @pytest.fixture(scope="class")
    def analysis(self):
        from src.tools.java_analyzer import analyze_files
        java_files = [str(p) for p in JAVA_DIR.rglob("*.java")]
        return analyze_files(file_paths=java_files, source_root=str(FIXTURE_DIR))

    def test_correct_role_counts(self, analysis):
        assert analysis["drivers"]       == 1
        assert analysis["mappers"]       == 1
        assert analysis["reducers"]      == 1
        assert analysis["combiners"]     == 1
        assert analysis["composite_keys"] == 1
        assert analysis["unclassified"]  == 0

    def test_driver_detected(self, analysis):
        drivers = [c for c in analysis["classes"] if c["role"] == "driver"]
        assert len(drivers) == 1
        assert drivers[0]["class_name"] == "WordCountDriver"

    def test_mapper_detected(self, analysis):
        mappers = [c for c in analysis["classes"] if c["role"] == "mapper"]
        assert len(mappers) == 1
        assert mappers[0]["class_name"] == "WordCountMapper"
        assert "Mapper" in (mappers[0]["extends"] or "")

    def test_reducer_detected(self, analysis):
        reducers = [c for c in analysis["classes"] if c["role"] == "reducer"]
        assert len(reducers) == 1
        assert reducers[0]["class_name"] == "WordCountReducer"

    def test_combiner_detected(self, analysis):
        combiners = [c for c in analysis["classes"] if c["role"] == "combiner"]
        assert len(combiners) == 1
        assert combiners[0]["class_name"] == "WordCountCombiner"

    def test_composite_key_detected(self, analysis):
        keys = [c for c in analysis["classes"] if c["role"] == "composite_key"]
        assert len(keys) == 1
        assert keys[0]["class_name"] == "CompositeKey"

    def test_package_extracted(self, analysis):
        for cls in analysis["classes"]:
            assert cls["package"] == "com.example.mr"

    def test_imports_extracted(self, analysis):
        mapper = next(c for c in analysis["classes"] if c["class_name"] == "WordCountMapper")
        assert any("hadoop" in imp for imp in mapper["imports"])

    def test_extends_extracted(self, analysis):
        mapper = next(c for c in analysis["classes"] if c["class_name"] == "WordCountMapper")
        assert mapper["extends"] is not None
        assert "Mapper" in mapper["extends"]

    def test_no_parse_errors(self, analysis):
        assert analysis["parse_errors"] == [], \
            f"Unexpected parse errors: {analysis['parse_errors']}"

    def test_empty_file_list(self):
        from src.tools.java_analyzer import analyze_files
        result = analyze_files(file_paths=[], source_root=str(FIXTURE_DIR))
        assert result["classes"] == []
        assert result["drivers"] == 0


# ─────────────────────────────────────────────────────────────────────────────
# schema_extractor — extract_schemas
# ─────────────────────────────────────────────────────────────────────────────

class TestSchemaExtractor:

    @pytest.fixture(scope="class")
    def class_metadata(self):
        from src.tools.java_analyzer import analyze_files
        java_files = [str(p) for p in JAVA_DIR.rglob("*.java")]
        result = analyze_files(file_paths=java_files, source_root=str(FIXTURE_DIR))
        return result["classes"]

    @pytest.fixture(scope="class")
    def schemas(self, class_metadata):
        from src.tools.schema_extractor import extract_schemas
        result = extract_schemas(
            class_metadata=class_metadata,
            source_root=str(FIXTURE_DIR),
        )
        return result

    def test_composite_key_schema_extracted(self, schemas):
        schema_names = [s["source_class"] for s in schemas["schemas"]]
        assert "CompositeKey" in schema_names

    def test_composite_key_fields(self, schemas):
        ck = next(s for s in schemas["schemas"] if s["source_class"] == "CompositeKey")
        field_names = [f["name"] for f in ck["fields"]]
        assert "groupId" in field_names
        assert "timestamp" in field_names

    def test_composite_key_spark_types(self, schemas):
        ck = next(s for s in schemas["schemas"] if s["source_class"] == "CompositeKey")
        type_map = {f["name"]: f["spark_type"] for f in ck["fields"]}
        assert type_map["groupId"] == "StringType"
        assert type_map["timestamp"] == "LongType"

    def test_mapper_generics_schema(self, schemas):
        mapper_schemas = [s for s in schemas["schemas"] if "Mapper" in s.get("source_class", "") or
                         any(f["name"] == "input_key" for f in s.get("fields", []))]
        assert len(mapper_schemas) >= 1

    def test_avro_schema_loaded(self, schemas):
        avro_schemas = [s for s in schemas["schemas"] if s.get("format") == "avro"]
        assert len(avro_schemas) >= 1
        wc = next((s for s in avro_schemas if s["source_class"] == "WordCount"), None)
        assert wc is not None
        field_names = [f["name"] for f in wc["fields"]]
        assert "word" in field_names
        assert "count" in field_names

    def test_avro_spark_types(self, schemas):
        avro = next(s for s in schemas["schemas"] if s.get("format") == "avro")
        type_map = {f["name"]: f["spark_type"] for f in avro["fields"]}
        assert type_map["word"] == "StringType"
        assert type_map["count"] == "IntegerType"

    def test_no_unresolved_for_fixture(self, schemas):
        # All composite keys in fixture should resolve
        assert schemas["unresolved"] == []

    def test_writable_type_mapping(self):
        from src.tools.schema_extractor import WRITABLE_TO_SPARK
        assert WRITABLE_TO_SPARK["Text"]        == "StringType"
        assert WRITABLE_TO_SPARK["IntWritable"] == "IntegerType"
        assert WRITABLE_TO_SPARK["LongWritable"] == "LongType"
        assert WRITABLE_TO_SPARK["DoubleWritable"] == "DoubleType"


# ─────────────────────────────────────────────────────────────────────────────
# Full pipeline — local source end-to-end (Phase 2 nodes)
# ─────────────────────────────────────────────────────────────────────────────

class TestPhase2Pipeline:

    @pytest.fixture
    def local_config(self):
        from src.models.state import (
            PipelineConfig, SourceConfig, VersionConfig,
            RiskConfig, SourceMode, BuildTool,
        )
        return PipelineConfig(
            source=SourceConfig(
                mode=SourceMode.LOCAL,
                local_path=str(FIXTURE_DIR),
            ),
            versions=VersionConfig(),
            build_tool=BuildTool.MAVEN,
            risk=RiskConfig(prefixes=["com.company-name"]),
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
            "generated_files": {},
        }

    def test_pipeline_runs_with_real_source(self, local_config):
        from src.agents.graph import pipeline_graph
        state = pipeline_graph.invoke(self._initial_state(local_config))
        assert state is not None
        assert state.get("errors", []) == []

    def test_real_java_classes_in_state(self, local_config):
        from src.agents.graph import pipeline_graph
        from src.models.state import NodeStatus
        state = pipeline_graph.invoke(self._initial_state(local_config))

        classes = state.get("java_classes", [])
        assert len(classes) == 5, f"Expected 5 classes, got {len(classes)}"

        roles = {c.role for c in classes}
        assert "driver" in roles
        assert "mapper" in roles
        assert "reducer" in roles
        assert "combiner" in roles
        assert "composite_key" in roles

    def test_real_schemas_in_state(self, local_config):
        from src.agents.graph import pipeline_graph
        state = pipeline_graph.invoke(self._initial_state(local_config))

        schemas = state.get("detected_schemas", [])
        assert len(schemas) > 0

        schema_names = {s.source_class for s in schemas}
        assert "CompositeKey" in schema_names

    def test_source_resolve_node_succeeds(self, local_config):
        from src.agents.graph import pipeline_graph
        from src.models.state import NodeStatus
        state = pipeline_graph.invoke(self._initial_state(local_config))

        progress = state.get("node_progress", {})
        sr = progress.get("source_resolve")
        assert sr is not None
        assert sr.status == NodeStatus.SUCCESS
        assert str(FIXTURE_DIR) in sr.message

    def test_driver_analysis_node_succeeds(self, local_config):
        from src.agents.graph import pipeline_graph
        from src.models.state import NodeStatus
        state = pipeline_graph.invoke(self._initial_state(local_config))

        progress = state.get("node_progress", {})
        da = progress.get("driver_analysis")
        assert da is not None
        assert da.status == NodeStatus.SUCCESS
        assert "drivers=1" in da.message

    def test_schema_extract_node_succeeds(self, local_config):
        from src.agents.graph import pipeline_graph
        from src.models.state import NodeStatus
        state = pipeline_graph.invoke(self._initial_state(local_config))

        progress = state.get("node_progress", {})
        se = progress.get("schema_extract")
        assert se is not None
        assert se.status == NodeStatus.SUCCESS
