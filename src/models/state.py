"""
Pipeline State — the single TypedDict that flows through every LangGraph node.

Every field is Optional so nodes can be added incrementally. Pydantic models
are used for sub-structures to enforce shape at boundaries.
"""
from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Optional
from typing_extensions import Annotated, TypedDict

from pydantic import BaseModel, Field


# ─────────────────────────────────────────────────────────────────────────────
# Enums
# ─────────────────────────────────────────────────────────────────────────────

class SourceMode(str, Enum):
    GIT   = "git"
    LOCAL = "local"


class BuildTool(str, Enum):
    MAVEN  = "maven"
    GRADLE = "gradle"


class LLMProvider(str, Enum):
    CLAUDE = "claude"
    GEMINI = "gemini"


class RiskAction(str, Enum):
    WARN  = "warn"
    BLOCK = "block"
    INFO  = "info"


class NodeStatus(str, Enum):
    PENDING  = "pending"
    RUNNING  = "running"
    SUCCESS  = "success"
    FAILED   = "failed"
    SKIPPED  = "skipped"
    WAITING  = "waiting"   # waiting for human input


class ApiStrategy(str, Enum):
    DATAFRAME = "dataframe_enforced"
    RDD       = "rdd_only"


# ─────────────────────────────────────────────────────────────────────────────
# Sub-models (Pydantic — validated at node boundaries)
# ─────────────────────────────────────────────────────────────────────────────

class SourceConfig(BaseModel):
    mode: SourceMode = SourceMode.GIT

    # Git fields
    repo_url:   Optional[str] = None
    branch:     Optional[str] = None
    git_token:  Optional[str] = None
    subdir:     Optional[str] = None
    commit_tag: Optional[str] = None

    # Local fields
    local_path:     Optional[str] = None
    local_subdir:   Optional[str] = None
    local_pattern:  str = "**/*.java"
    recurse_submodules: bool = True
    skip_build_dirs:    bool = True
    read_build_files:   bool = True

    @property
    def display_name(self) -> str:
        if self.mode == SourceMode.GIT:
            repo = (self.repo_url or "").split("/")[-1].replace(".git", "")
            return f"{repo}@{self.branch or 'HEAD'}"
        return self.local_path or "local"


class VersionConfig(BaseModel):
    spark:  str = "4.0.1"
    scala:  str = "2.13.16"
    jdk:    str = "17"
    hadoop: str = "3.4.2"


class RiskConfig(BaseModel):
    prefixes:              list[str] = Field(default_factory=list)
    action:                RiskAction = RiskAction.WARN
    transitive_dep_scan:   bool = True
    generate_risk_report:  bool = True


class CodegenConfig(BaseModel):
    api_strategy:       ApiStrategy = ApiStrategy.DATAFRAME
    typed_dataset:      bool = True
    emit_sql_comments:  bool = False
    rdd_fallback_stubs: bool = True
    generate_tests:     bool = True


class PipelineConfig(BaseModel):
    """Top-level user configuration — produced by the Streamlit UI."""
    run_id:      str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    created_at:  datetime = Field(default_factory=datetime.utcnow)

    source:      SourceConfig   = Field(default_factory=SourceConfig)
    versions:    VersionConfig  = Field(default_factory=VersionConfig)
    build_tool:  BuildTool      = BuildTool.MAVEN
    codegen:     CodegenConfig  = Field(default_factory=CodegenConfig)
    risk:        RiskConfig     = Field(default_factory=RiskConfig)

    llm_provider: LLMProvider = LLMProvider.CLAUDE
    model_name:   str = "claude-sonnet-4-5-20250929"


class JavaClass(BaseModel):
    """A single Java class detected during scan."""
    class_name:  str
    file_path:   str
    package:     Optional[str] = None
    role:        Optional[str] = None   # driver|mapper|reducer|combiner|partitioner|composite_key
    imports:     list[str] = Field(default_factory=list)
    extends:     Optional[str] = None
    implements:  list[str] = Field(default_factory=list)
    raw_source:  Optional[str] = None


class SchemaField(BaseModel):
    name:      str
    java_type: str
    spark_type: Optional[str] = None   # resolved in schema_extract


class DetectedSchema(BaseModel):
    source_class: str
    fields:       list[SchemaField] = Field(default_factory=list)
    format:       Optional[str] = None   # parquet|avro|text|sequence


class MRJob(BaseModel):
    """Represents one MapReduce job inferred from the source."""
    job_name:    str
    driver:      Optional[JavaClass] = None
    mapper:      Optional[JavaClass] = None
    reducer:     Optional[JavaClass] = None
    combiner:    Optional[JavaClass] = None
    partitioner: Optional[JavaClass] = None
    composite_keys: list[JavaClass] = Field(default_factory=list)
    input_paths:    list[str] = Field(default_factory=list)
    output_paths:   list[str] = Field(default_factory=list)
    input_format:   Optional[str] = None
    output_format:  Optional[str] = None
    chained_after:  Optional[str] = None   # job_name of predecessor


class RiskItem(BaseModel):
    matched_prefix: str
    artifact:       str    # full import or groupId:artifactId
    location:       str    # file path where found
    is_transitive:  bool = False
    suggestion:     Optional[str] = None


class CompileResult(BaseModel):
    success:     bool
    stdout:      str = ""
    stderr:      str = ""
    error_lines: list[str] = Field(default_factory=list)
    attempt:     int = 1


class TestResult(BaseModel):
    passed:       bool
    mr_row_count:    Optional[int] = None
    spark_row_count: Optional[int] = None
    checksum_match:  Optional[bool] = None
    details:      str = ""


class NodeProgress(BaseModel):
    """Tracks execution status of each pipeline node — used by Streamlit UI."""
    node_name:  str
    status:     NodeStatus = NodeStatus.PENDING
    started_at: Optional[datetime] = None
    ended_at:   Optional[datetime] = None
    message:    str = ""
    error:      Optional[str] = None

    @property
    def duration_ms(self) -> Optional[int]:
        if self.started_at and self.ended_at:
            return int((self.ended_at - self.started_at).total_seconds() * 1000)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Reducer helpers for Annotated fields
# ─────────────────────────────────────────────────────────────────────────────

def _merge_dicts(a: dict, b: dict) -> dict:
    """Merge two dicts — b wins on key collision. Used for node_progress."""
    return {**a, **b}


def _extend_list(a: list, b: list) -> list:
    """Concatenate two lists — used for list fields written by parallel nodes."""
    return list(a) + list(b)


def _extend_errors(a: list[str], b: list[str]) -> list[str]:
    return list(a) + list(b)


# ─────────────────────────────────────────────────────────────────────────────
# LangGraph State TypedDict
# ─────────────────────────────────────────────────────────────────────────────

class PipelineState(TypedDict, total=False):
    """
    The single state object that flows through every LangGraph node.

    Fields written by parallel nodes (driver_analysis + schema_extract run
    simultaneously) MUST use Annotated[T, reducer_fn] so LangGraph knows
    how to merge concurrent writes instead of raising InvalidUpdateError.

    Fields only ever written by a single node use plain types.
    """

    # ── Config (set by UI before graph starts — single writer) ──
    config: PipelineConfig

    # ── Phase 1: Scan (single writer: source_resolve) ──
    repo_local_path: Optional[str]
    java_files:      list[str]

    # ── Parallel fan-out outputs — MUST have reducers ──
    # driver_analysis writes java_classes; schema_extract writes detected_schemas.
    # Both also write node_progress and potentially errors in the same step.
    java_classes:     Annotated[list[JavaClass],     _extend_list]
    detected_schemas: Annotated[list[DetectedSchema], _extend_list]

    # node_progress is written by EVERY node including parallel ones
    node_progress: Annotated[dict[str, NodeProgress], _merge_dicts]

    # errors can be appended by any node including parallel ones
    errors: Annotated[list[str], _extend_errors]

    # ── Phase 2: Job Graph Inference (single writer: jobgraph_infer) ──
    job_graph_json: Optional[dict[str, Any]]
    mr_jobs:        list[MRJob]

    # ── Phase 3: Code Generation (single writer: spark_codegen) ──
    scaffold_path:   Optional[str]
    generated_files: dict[str, str]

    # ── Phase 4: Quality Gate (single writers per node) ──
    compile_results: list[CompileResult]
    patch_attempts:  int
    patched_files:   dict[str, str]
    test_result:     Optional[TestResult]

    # ── Phase 5: Risk Detection (single writer: risk_scan) ──
    risk_items: list[RiskItem]

    # ── Output (single writer: package_export) ──
    artifact_path:    Optional[str]
    migration_report: Optional[str]

    # ── Human review (single writer: human_review node) ──
    human_review_required: bool
    human_review_message:  Optional[str]
