"""
Pipeline Graph — LangGraph StateGraph definition.

This module wires all pipeline nodes into the directed graph.
Phase 1: all nodes call the FastMCP stub tools.
Phase 2-4: node implementations are swapped in-place — graph topology unchanged.

Node execution order:
  [START]
    └─ config_validate
         └─ source_resolve          (git clone OR validate local path)
              └─ [PARALLEL FAN-OUT]
                   ├─ driver_analysis
                   └─ schema_extract
              └─ [JOIN] jobgraph_infer   (LLM + rules)
                   └─ [PARALLEL FAN-OUT]
                        ├─ diagram_render   (non-blocking, no downstream dep)
                        └─ spark_codegen    (LLM)
                   └─ [JOIN] risk_scan
                        └─ project_scaffold
                             └─ compile_check
                                  ├─ [FAIL → patch_attempts < max] critique_and_patch ─┐
                                  │                                                     │ (loop)
                                  └─ [PASS] test_check ◄────────────────────────────────┘
                                       ├─ [FAIL] human_review
                                       └─ [PASS] package_export
                                                      └─ [END]
"""
from __future__ import annotations

from langgraph.graph import StateGraph, START, END

from src.models.state import PipelineState
from src.utils.logging import get_logger
from .nodes import (
    config_validate_node,
    source_resolve_node,
    driver_analysis_node,
    schema_extract_node,
    jobgraph_infer_node,
    diagram_render_node,
    spark_codegen_node,
    risk_scan_node,
    project_scaffold_node,
    compile_check_node,
    critique_and_patch_node,
    test_check_node,
    human_review_node,
    package_export_node,
)

logger = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Conditional edge functions
# ─────────────────────────────────────────────────────────────────────────────

def route_after_compile(state: PipelineState) -> str:
    """
    After compile_check:
      - If success → test_check
      - If failed AND attempts < max → critique_and_patch
      - If failed AND attempts >= max → human_review
    """
    from src.utils.config import settings

    results = state.get("compile_results", [])
    attempts = state.get("patch_attempts", 0)
    last_result = results[-1] if results else None

    if last_result and last_result.success:
        logger.info("route.compile", decision="test_check")
        return "test_check"

    if attempts >= settings.max_patch_retries:
        logger.warning(
            "route.compile",
            decision="human_review",
            attempts=attempts,
            max=settings.max_patch_retries,
        )
        return "human_review"

    logger.info("route.compile", decision="critique_and_patch", attempt=attempts + 1)
    return "critique_and_patch"


def route_after_patch(state: PipelineState) -> str:
    """After critique_and_patch, always go back to compile_check."""
    return "compile_check"


def route_after_test(state: PipelineState) -> str:
    """After test_check: pass → package_export, fail → human_review."""
    result = state.get("test_result")
    if result and result.passed:
        logger.info("route.test", decision="package_export")
        return "package_export"
    logger.warning("route.test", decision="human_review")
    return "human_review"


def route_after_risk(state: PipelineState) -> str:
    """After risk_scan: if BLOCK action and risks found → human_review."""
    from src.models.state import RiskAction

    config = state.get("config")
    risk_items = state.get("risk_items", [])

    if (
        config
        and config.risk.action == RiskAction.BLOCK
        and len(risk_items) > 0
    ):
        logger.warning(
            "route.risk",
            decision="human_review",
            risk_count=len(risk_items),
        )
        return "human_review"

    return "project_scaffold"


# ─────────────────────────────────────────────────────────────────────────────
# Graph builder
# ─────────────────────────────────────────────────────────────────────────────

def build_pipeline_graph() -> StateGraph:
    """
    Construct and compile the full migration pipeline graph.
    Returns a compiled LangGraph runnable.
    """
    graph = StateGraph(PipelineState)

    # ── Register all nodes ──
    graph.add_node("config_validate",      config_validate_node)
    graph.add_node("source_resolve",       source_resolve_node)
    graph.add_node("driver_analysis",      driver_analysis_node)
    graph.add_node("schema_extract",       schema_extract_node)
    graph.add_node("jobgraph_infer",       jobgraph_infer_node)
    graph.add_node("diagram_render",       diagram_render_node)
    graph.add_node("spark_codegen",        spark_codegen_node)
    graph.add_node("risk_scan",            risk_scan_node)
    graph.add_node("project_scaffold",     project_scaffold_node)
    graph.add_node("compile_check",        compile_check_node)
    graph.add_node("critique_and_patch",   critique_and_patch_node)
    graph.add_node("test_check",           test_check_node)
    graph.add_node("human_review",         human_review_node)
    graph.add_node("package_export",       package_export_node)

    # ── Linear edges ──
    graph.add_edge(START,              "config_validate")
    graph.add_edge("config_validate",  "source_resolve")

    # ── Parallel fan-out: source_resolve → driver_analysis + schema_extract ──
    graph.add_edge("source_resolve",   "driver_analysis")
    graph.add_edge("source_resolve",   "schema_extract")

    # ── Join both parallel branches into jobgraph_infer ──
    graph.add_edge("driver_analysis",  "jobgraph_infer")
    graph.add_edge("schema_extract",   "jobgraph_infer")

    # ── Parallel fan-out: jobgraph_infer → diagram_render + spark_codegen ──
    graph.add_edge("jobgraph_infer",   "diagram_render")
    graph.add_edge("jobgraph_infer",   "spark_codegen")

    # ── diagram_render is non-blocking (no downstream) ──
    graph.add_edge("diagram_render",   END)

    # ── spark_codegen → risk_scan → conditional route ──
    graph.add_edge("spark_codegen",    "risk_scan")
    graph.add_conditional_edges(
        "risk_scan",
        route_after_risk,
        {"project_scaffold": "project_scaffold", "human_review": "human_review"},
    )

    # ── Scaffold → compile ──
    graph.add_edge("project_scaffold", "compile_check")

    # ── Compile → conditional (pass/fail/circuit-break) ──
    graph.add_conditional_edges(
        "compile_check",
        route_after_compile,
        {
            "test_check":           "test_check",
            "critique_and_patch":   "critique_and_patch",
            "human_review":         "human_review",
        },
    )

    # ── Patch → back to compile ──
    graph.add_edge("critique_and_patch", "compile_check")

    # ── Test → conditional ──
    graph.add_conditional_edges(
        "test_check",
        route_after_test,
        {"package_export": "package_export", "human_review": "human_review"},
    )

    # ── Terminal nodes ──
    graph.add_edge("package_export", END)
    graph.add_edge("human_review",   END)

    return graph


# Compiled singleton — import this for running the pipeline
pipeline_graph = build_pipeline_graph().compile()
