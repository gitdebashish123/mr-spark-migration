"""
Pipeline Nodes — one function per LangGraph node.

Phase 1: each node calls the corresponding FastMCP stub tool and updates state.
Phase 2-4: the function bodies are replaced with real implementations.
The node signatures (input/output state keys) NEVER change between phases.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from src.models.state import (
    PipelineState, NodeProgress, NodeStatus,
    JavaClass, DetectedSchema, SchemaField,
    MRJob, RiskItem, CompileResult, TestResult,
    PipelineConfig,
)
from src.tools.base_tools import (
    scan_repo,
    analyze_java_classes,
    extract_schema,
    infer_job_graph,
    generate_spark_code,
    scaffold_project,
    compile_check as compile_check_tool,
    scan_risk_patterns,
)
from src.tools.git_resolver import (
    resolve_git_source,
    resolve_local_source,
    cleanup_clone,
)
from src.tools.build_runner import critique_and_patch_llm, run_tests
from src.tools.report_generator import generate_migration_report
from src.utils.logging import get_logger

logger = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

# ── per-node start timestamps (module-level, keyed by node name) ──
_node_start_times: dict[str, datetime] = {}


def _start_node(state: PipelineState, name: str) -> None:
    """
    Record start time for this node. Does NOT write to state —
    the timestamp is stored locally and flushed by _end_node.
    Avoids a redundant state write that would conflict with _end_node
    in the same execution step.
    """
    _node_start_times[name] = datetime.utcnow()
    logger.info("node.start", node=name)


def _end_node(
    state: PipelineState,
    name: str,
    updates: dict[str, Any],
    message: str = "",
    error: str | None = None,
) -> dict[str, Any]:
    """
    Return the node's result updates plus its progress entry.

    IMPORTANT: node_progress emits only {name: NodeProgress} for THIS node.
    The Annotated[dict, _merge_dicts] reducer in PipelineState merges it with
    progress entries from other concurrent nodes automatically.
    Emitting the full copied dict would cause InvalidUpdateError on parallel steps.
    """
    started_at = _node_start_times.pop(name, datetime.utcnow())
    progress_entry = NodeProgress(
        node_name=name,
        status=NodeStatus.FAILED if error else NodeStatus.SUCCESS,
        started_at=started_at,
        ended_at=datetime.utcnow(),
        message=message,
        error=error,
    )
    logger.info("node.end", node=name, status=progress_entry.status, message=message)
    return {**updates, "node_progress": {name: progress_entry}}


# ─────────────────────────────────────────────────────────────────────────────
# Node 1 — config_validate
# ─────────────────────────────────────────────────────────────────────────────

def config_validate_node(state: PipelineState) -> dict[str, Any]:
    """Validate the PipelineConfig before touching any external resource."""
    _start_node(state, "config_validate")
    config: PipelineConfig = state.get("config")

    # Collect only NEW errors from this node — the reducer appends them
    new_errors: list[str] = []

    if config is None:
        new_errors.append("No pipeline config provided.")
        return _end_node(
            state, "config_validate",
            {"errors": new_errors},
            error="Missing config",
        )

    if config.source.mode.value == "git":
        if not config.source.repo_url:
            new_errors.append("Git mode selected but repo_url is empty.")
        if not config.source.branch:
            new_errors.append("Git mode selected but branch is empty.")
    else:
        if not config.source.local_path:
            new_errors.append("Local mode selected but local_path is empty.")

    if new_errors:
        return _end_node(
            state, "config_validate",
            {"errors": new_errors},
            error="; ".join(new_errors),
        )

    return _end_node(
        state, "config_validate", {},
        message=f"Config valid · run_id={config.run_id}",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Node 2 — source_resolve
# ─────────────────────────────────────────────────────────────────────────────

def source_resolve_node(state: PipelineState) -> dict[str, Any]:
    """
    Git mode:  clone repo with GitPython, return local path.
    Local mode: validate path exists, return as-is.
    Then scan for Java files using the real filesystem walker.
    """
    _start_node(state, "source_resolve")
    config: PipelineConfig = state["config"]
    src = config.source

    try:
        if src.mode.value == "git":
            resolved_path = resolve_git_source(
                repo_url=src.repo_url,
                branch=src.branch,
                run_id=config.run_id,
                token=src.git_token,
                subdir=src.subdir,
                commit_tag=src.commit_tag,
            )
        else:
            resolved_path = resolve_local_source(
                local_path=src.local_path,
                subdir=src.local_subdir,
            )
    except RuntimeError as exc:
        return _end_node(
            state, "source_resolve", {"errors": [str(exc)]},
            error=str(exc),
        )

    scan_result = scan_repo(
        source_path=resolved_path,
        pattern=src.local_pattern if src.mode.value == "local" else "**/*.java",
        skip_build_dirs=src.skip_build_dirs if src.mode.value == "local" else True,
    )

    # Use abs_path when available (Phase 2) else fall back to relative path
    java_files = [
        f.get("abs_path", f["path"])
        for f in scan_result.get("files", [])
        if f["path"].endswith(".java")
    ]

    return _end_node(
        state, "source_resolve",
        {
            "repo_local_path": resolved_path,
            "java_files": java_files,
        },
        message=f"Found {len(java_files)} Java files in {resolved_path}",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Node 3 — driver_analysis  (parallel with schema_extract)
# ─────────────────────────────────────────────────────────────────────────────

def driver_analysis_node(state: PipelineState) -> dict[str, Any]:
    """Parse Java classes with real javalang AST and classify by MR role."""
    _start_node(state, "driver_analysis")
    java_files = state.get("java_files", [])
    source_root = state.get("repo_local_path", "")

    result = analyze_java_classes(
        file_paths=java_files,
        source_root=source_root,
    )

    classes = [JavaClass(**c) for c in result.get("classes", [])]
    parse_errors = result.get("parse_errors", [])

    summary = (
        f"drivers={result['drivers']} mappers={result['mappers']} "
        f"reducers={result['reducers']} combiners={result['combiners']} "
        f"partitioners={result['partitioners']} composite_keys={result['composite_keys']}"
    )
    if parse_errors:
        summary += f" · {len(parse_errors)} parse error(s)"
        logger.warning("driver_analysis.parse_errors", count=len(parse_errors), errors=parse_errors)

    return _end_node(
        state, "driver_analysis",
        {"java_classes": classes},
        message=summary,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Node 4 — schema_extract  (parallel with driver_analysis)
# ─────────────────────────────────────────────────────────────────────────────

def schema_extract_node(state: PipelineState) -> dict[str, Any]:
    """Resolve WritableComparable / Parquet / Avro types to Spark schemas."""
    _start_node(state, "schema_extract")
    classes = [c.model_dump() for c in state.get("java_classes", [])]
    source_root = state.get("repo_local_path", "")

    result = extract_schema(
        class_metadata=classes,
        source_root=source_root,
    )

    schemas = [
        DetectedSchema(
            source_class=s["source_class"],
            fields=[SchemaField(**f) for f in s.get("fields", [])],
            format=s.get("format"),
        )
        for s in result.get("schemas", [])
    ]

    unresolved = result.get("unresolved", [])
    msg = f"Resolved {len(schemas)} schemas"
    if unresolved:
        msg += f" · {len(unresolved)} unresolved: {unresolved}"

    return _end_node(
        state, "schema_extract",
        {"detected_schemas": schemas},
        message=msg,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Node 5 — jobgraph_infer
# ─────────────────────────────────────────────────────────────────────────────

def jobgraph_infer_node(state: PipelineState) -> dict[str, Any]:
    """Infer the MR job graph using the configured LLM (Phase 3)."""
    _start_node(state, "jobgraph_infer")
    config: PipelineConfig = state["config"]
    classes = [c.model_dump() for c in state.get("java_classes", [])]
    schemas = [s.model_dump() for s in state.get("detected_schemas", [])]

    try:
        result = infer_job_graph(
            classes=classes,
            schemas=schemas,
            config=config,
        )
    except (RuntimeError, Exception) as exc:
        return _end_node(
            state, "jobgraph_infer", {"errors": [str(exc)]},
            error=str(exc),
        )
    job_data = result.get("jobs", [])

    # Fully hydrate MRJob models from LLM output
    mr_jobs = []
    for j in job_data:
        def _to_class(d) -> JavaClass | None:
            if not d:
                return None
            return JavaClass(class_name=d["class_name"], file_path="", role=d.get("role"))

        mr_jobs.append(MRJob(
            job_name=j["job_name"],
            driver=_to_class(j.get("driver")),
            mapper=_to_class(j.get("mapper")),
            reducer=_to_class(j.get("reducer")),
            combiner=_to_class(j.get("combiner")),
            partitioner=_to_class(j.get("partitioner")),
            composite_keys=[_to_class(k) for k in j.get("composite_keys", []) if k],
            input_paths=j.get("input_paths", []),
            output_paths=j.get("output_paths", []),
            input_format=j.get("input_format"),
            output_format=j.get("output_format"),
            chained_after=j.get("chained_after"),
        ))

    return _end_node(
        state, "jobgraph_infer",
        {"job_graph_json": result, "mr_jobs": mr_jobs},
        message=f"Inferred {len(mr_jobs)} MR job(s) · complexity={result.get('complexity','?')}",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Node 6 — diagram_render  (parallel, non-blocking)
# ─────────────────────────────────────────────────────────────────────────────

def diagram_render_node(state: PipelineState) -> dict[str, Any]:
    """
    Generate a visual job graph diagram.
    Non-blocking: runs in parallel and terminates at END.
    Phase 1: stub — logs only.
    """
    _start_node(state, "diagram_render")
    jobs = state.get("mr_jobs", [])
    logger.info("diagram_render.stub", job_count=len(jobs))

    return _end_node(
        state, "diagram_render", {},
        message=f"Diagram rendered for {len(jobs)} job(s) [stub]",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Node 7 — spark_codegen
# ─────────────────────────────────────────────────────────────────────────────

def spark_codegen_node(state: PipelineState) -> dict[str, Any]:
    """Generate Spark/Scala source files using the configured LLM (Phase 3)."""
    _start_node(state, "spark_codegen")
    config: PipelineConfig = state["config"]

    result = generate_spark_code(
        job_graph=state.get("job_graph_json", {}),
        versions=config.versions.model_dump(),
        codegen_config=config.codegen.model_dump(),
        config=config,
    )

    files = result.get("files", {})
    warnings = result.get("warnings", [])

    if warnings:
        for w in warnings:
            logger.warning("spark_codegen.warning", msg=w)

    return _end_node(
        state, "spark_codegen",
        {"generated_files": files},
        message=f"Generated {len(files)} Scala file(s)",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Node 8 — risk_scan
# ─────────────────────────────────────────────────────────────────────────────

def risk_scan_node(state: PipelineState) -> dict[str, Any]:
    """Scan imports and build files for custom library risk patterns."""
    _start_node(state, "risk_scan")
    config: PipelineConfig = state["config"]
    classes = [c.model_dump() for c in state.get("java_classes", [])]

    result = scan_risk_patterns(
        classes=classes,
        build_file_content=None,
        prefixes=config.risk.prefixes,
        transitive=config.risk.transitive_dep_scan,
    )

    risk_items = [RiskItem(**r) for r in result.get("risk_items", [])]
    total = result.get("total_risks", 0)
    blocked = result.get("blocked", False)

    msg = f"{total} risk(s) found"
    if blocked:
        msg += " · BLOCKED by policy"

    return _end_node(
        state, "risk_scan",
        {"risk_items": risk_items},
        message=msg,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Node 9 — project_scaffold
# ─────────────────────────────────────────────────────────────────────────────

def project_scaffold_node(state: PipelineState) -> dict[str, Any]:
    """Write generated files + build descriptor to disk."""
    _start_node(state, "project_scaffold")
    config: PipelineConfig = state["config"]
    files = state.get("generated_files", {})

    import tempfile
    output_dir = tempfile.mkdtemp(prefix="mr_migration_")

    result = scaffold_project(
        generated_files=files,
        build_tool=config.build_tool.value,
        versions=config.versions.model_dump(),
        output_dir=output_dir,
    )

    return _end_node(
        state, "project_scaffold",
        {"scaffold_path": result["project_path"]},
        message=f"Scaffolded to {result['project_path']} · {result['files_written']} files",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Node 10 — compile_check
# ─────────────────────────────────────────────────────────────────────────────

def compile_check_node(state: PipelineState) -> dict[str, Any]:
    """Run scalac via Maven/Gradle. Circuit breaker reads patch_attempts."""
    _start_node(state, "compile_check")
    config: PipelineConfig = state["config"]
    scaffold_path = state.get("scaffold_path", "")

    result_raw = compile_check_tool(
        project_path=scaffold_path,
        build_tool=config.build_tool.value,
    )

    compile_result = CompileResult(**result_raw)
    existing = list(state.get("compile_results", []))
    compile_result.attempt = len(existing) + 1
    existing.append(compile_result)

    status = "PASS" if compile_result.success else "FAIL"
    return _end_node(
        state, "compile_check",
        {"compile_results": existing},
        message=f"Compile attempt {compile_result.attempt}: {status}",
        error=None if compile_result.success else compile_result.stderr[:200],
    )


# ─────────────────────────────────────────────────────────────────────────────
# Node 11 — critique_and_patch
# ─────────────────────────────────────────────────────────────────────────────

def critique_and_patch_node(state: PipelineState) -> dict[str, Any]:
    """LLM reads compile errors and patches generated code (Phase 3: real LLM)."""
    _start_node(state, "critique_and_patch")

    attempts = state.get("patch_attempts", 0) + 1
    compile_results = state.get("compile_results", [])
    last_error = compile_results[-1].stderr if compile_results else "unknown error"

    logger.info("critique_and_patch.stub", attempt=attempts, error_preview=last_error[:100])

    # Phase 1 stub: no actual patching
    patched = dict(state.get("generated_files", {}))

    return _end_node(
        state, "critique_and_patch",
        {"patch_attempts": attempts, "patched_files": patched},
        message=f"Patch attempt {attempts} [stub]",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Node 12 — test_check
# ─────────────────────────────────────────────────────────────────────────────

def test_check_node(state: PipelineState) -> dict[str, Any]:
    """Run data equivalence check between original MR job and generated Spark job."""
    _start_node(state, "test_check")

    # Phase 1 stub: always passes
    result = TestResult(
        passed=True,
        mr_row_count=1000,
        spark_row_count=1000,
        checksum_match=True,
        details="[STUB] Phase 1 — test skipped, returning pass",
    )

    return _end_node(
        state, "test_check",
        {"test_result": result},
        message=f"Test {'PASSED' if result.passed else 'FAILED'} [stub]",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Node 13 — human_review
# ─────────────────────────────────────────────────────────────────────────────

def human_review_node(state: PipelineState) -> dict[str, Any]:
    """
    Pipeline pauses here when circuit breaker trips or risk blocks.
    Phase 5: replaced with real LangGraph interrupt() for Streamlit UI interaction.
    """
    _start_node(state, "human_review")

    compile_results = state.get("compile_results", [])
    risk_items = state.get("risk_items", [])
    attempts = state.get("patch_attempts", 0)

    reasons = []
    if compile_results and not compile_results[-1].success:
        reasons.append(f"Compile failed after {attempts} patch attempt(s)")
    if risk_items:
        reasons.append(f"{len(risk_items)} custom library risk(s) detected")

    message = "Human review required: " + " · ".join(reasons) if reasons else "Human review required"

    logger.warning("human_review.required", message=message)

    return _end_node(
        state, "human_review",
        {
            "human_review_required": True,
            "human_review_message": message,
        },
        message=message,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Node 14 — package_export
# ─────────────────────────────────────────────────────────────────────────────

def package_export_node(state: PipelineState) -> dict[str, Any]:
    """Package final uber-JAR and generate migration report."""
    _start_node(state, "package_export")
    scaffold_path = state.get("scaffold_path", "")
    config: PipelineConfig = state["config"]

    # Phase 1 stub
    artifact = f"{scaffold_path}/target/spark-migration-output.jar"

    risk_items = state.get("risk_items", [])
    jobs = state.get("mr_jobs", [])
    compile_results = state.get("compile_results", [])
    test_result = state.get("test_result")

    report = f"""# Migration Report — {config.run_id}

## Source
- Mode: {config.source.mode.value}
- Target: {config.source.display_name}

## Jobs Migrated
{chr(10).join(f'- {j.job_name}' for j in jobs)}

## Versions
- Spark {config.versions.spark} · Scala {config.versions.scala} · JDK {config.versions.jdk}

## Compile
- Attempts: {len(compile_results)}
- Final: {'PASS' if compile_results and compile_results[-1].success else 'N/A'}

## Test Equivalence
- {'PASSED' if test_result and test_result.passed else 'NOT RUN'}
{f'- Rows: MR={test_result.mr_row_count} Spark={test_result.spark_row_count}' if test_result else ''}

## Custom Library Risks
{chr(10).join(f'- [{r.matched_prefix}] {r.artifact} in {r.location}' for r in risk_items) or '- None detected'}

## Artifact
`{artifact}` [STUB — Phase 1]
"""

    return _end_node(
        state, "package_export",
        {
            "artifact_path": artifact,
            "migration_report": report,
        },
        message=f"Export complete → {artifact}",
    )
