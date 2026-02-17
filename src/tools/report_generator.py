"""
Phase 5 — Migration Report Generator.

Produces a comprehensive markdown report summarizing:
  - Source repository details
  - Detected MapReduce jobs
  - Generated Spark code structure
  - Compilation/test results
  - Risk assessment
  - Next steps
"""
from __future__ import annotations

import textwrap
from datetime import datetime
from pathlib import Path
from typing import Any

from src.models.state import PipelineState, NodeStatus
from src.utils.logging import get_logger

logger = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Main API
# ─────────────────────────────────────────────────────────────────────────────

def generate_migration_report(state: PipelineState) -> str:
    """
    Generate a comprehensive markdown migration report from pipeline state.

    Args:
        state: Final pipeline state after all nodes have run.

    Returns:
        Markdown-formatted report string.
    """
    config = state.get("config")
    if not config:
        return "# Migration Report\n\nError: No config found in state."

    sections = [
        _header_section(config),
        _summary_section(state),
        _source_analysis_section(state),
        _job_graph_section(state),
        _code_generation_section(state),
        _quality_gate_section(state),
        _risk_assessment_section(state),
        _next_steps_section(state),
        _footer_section(),
    ]

    report = "\n\n".join(sections)
    logger.info("report.generated", size=len(report))
    return report


# ─────────────────────────────────────────────────────────────────────────────
# Report Sections
# ─────────────────────────────────────────────────────────────────────────────

def _header_section(config) -> str:
    return textwrap.dedent(f"""\
        # MapReduce → Spark Migration Report
        
        **Run ID:** `{config.run_id}`  
        **Date:** {datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")}  
        **Source:** {config.source.display_name}  
        **LLM Provider:** {config.llm_provider.value} ({config.model_name})
    """)


def _summary_section(state: PipelineState) -> str:
    progress = state.get("node_progress", {})
    total_nodes = len(progress)
    success = sum(1 for p in progress.values() if p.status == NodeStatus.SUCCESS)
    failed = sum(1 for p in progress.values() if p.status == NodeStatus.FAILED)
    
    mr_jobs = state.get("mr_jobs", [])
    generated_files = state.get("generated_files", {})
    scala_files = [p for p in generated_files.keys() if p.endswith(".scala")]
    
    return textwrap.dedent(f"""\
        ## Executive Summary
        
        - **MapReduce Jobs Detected:** {len(mr_jobs)}
        - **Scala Files Generated:** {len(scala_files)}
        - **Pipeline Nodes:** {success}/{total_nodes} succeeded, {failed} failed
        - **Human Review Required:** {"Yes" if state.get("human_review_required") else "No"}
    """)


def _source_analysis_section(state: PipelineState) -> str:
    java_classes = state.get("java_classes", [])
    schemas = state.get("detected_schemas", [])
    
    role_counts = {}
    for cls in java_classes:
        role = cls.role or "unclassified"
        role_counts[role] = role_counts.get(role, 0) + 1
    
    role_summary = "\n".join(f"- {role.title()}: {count}" for role, count in sorted(role_counts.items()))
    
    schema_summary = "\n".join(
        f"- `{s.source_class}`: {len(s.fields)} fields ({s.format or 'unknown'})"
        for s in schemas[:10]  # cap at 10
    )
    if len(schemas) > 10:
        schema_summary += f"\n- ... and {len(schemas) - 10} more"
    
    return textwrap.dedent(f"""\
        ## Source Analysis
        
        ### Java Classes ({len(java_classes)} total)
        {role_summary}
        
        ### Detected Schemas ({len(schemas)} total)
        {schema_summary if schemas else "No schemas detected"}
    """)


def _job_graph_section(state: PipelineState) -> str:
    mr_jobs = state.get("mr_jobs", [])
    if not mr_jobs:
        return "## Job Graph\n\nNo MapReduce jobs inferred."
    
    job_details = []
    for job in mr_jobs:
        mapper = job.mapper.class_name if job.mapper else "None"
        reducer = job.reducer.class_name if job.reducer else "None"
        combiner = job.combiner.class_name if job.combiner else "None"
        
        job_details.append(textwrap.dedent(f"""\
            ### Job: `{job.job_name}`
            - **Driver:** `{job.driver.class_name if job.driver else "None"}`
            - **Mapper:** `{mapper}`
            - **Reducer:** `{reducer}`
            - **Combiner:** `{combiner}`
            - **Input:** `{", ".join(job.input_paths)}`
            - **Output:** `{", ".join(job.output_paths)}`
        """))
    
    return "## Job Graph\n\n" + "\n".join(job_details)


def _code_generation_section(state: PipelineState) -> str:
    generated_files = state.get("generated_files", {})
    patched_files = state.get("patched_files", {})
    
    scala_files = [p for p in generated_files.keys() if p.endswith(".scala")]
    
    file_list = "\n".join(f"- `{p}`" for p in sorted(scala_files)[:15])
    if len(scala_files) > 15:
        file_list += f"\n- ... and {len(scala_files) - 15} more"
    
    patch_info = ""
    if patched_files:
        patch_info = f"\n\n**Note:** {len(patched_files)} file(s) were patched after initial compilation failures."
    
    return textwrap.dedent(f"""\
        ## Code Generation
        
        **Files Generated:** {len(scala_files)} Scala files
        
        {file_list}
        {patch_info}
    """)


def _quality_gate_section(state: PipelineState) -> str:
    compile_results = state.get("compile_results", [])
    test_result = state.get("test_result")
    patch_attempts = state.get("patch_attempts", 0)
    
    if not compile_results:
        return "## Quality Gate\n\nNo compilation attempted."
    
    last_compile = compile_results[-1]
    compile_status = "✅ PASS" if last_compile.success else "❌ FAIL"
    
    compile_summary = f"**Compilation:** {compile_status} (attempt {last_compile.attempt}/{len(compile_results)})"
    if not last_compile.success and last_compile.error_lines:
        compile_summary += "\n\n**Sample Errors:**\n```\n" + "\n".join(last_compile.error_lines[:5]) + "\n```"
    
    patch_summary = ""
    if patch_attempts > 0:
        patch_summary = f"\n\n**Patch Attempts:** {patch_attempts} (LLM-assisted fixes applied)"
    
    test_summary = ""
    if test_result:
        test_status = "✅ PASS" if test_result.passed else "❌ FAIL"
        test_summary = f"\n\n**Tests:** {test_status}"
        if not test_result.passed:
            test_summary += f"\n\n**Test Output:**\n```\n{test_result.details[:300]}\n```"
    
    return textwrap.dedent(f"""\
        ## Quality Gate
        
        {compile_summary}
        {patch_summary}
        {test_summary}
    """)


def _risk_assessment_section(state: PipelineState) -> str:
    risk_items = state.get("risk_items", [])
    if not risk_items:
        return "## Risk Assessment\n\n✅ No custom library risks detected."
    
    risk_list = []
    for item in risk_items[:10]:  # cap at 10
        suggestion = item.get("suggestion", "Review manually")
        risk_list.append(f"- **{item['artifact']}** (in `{item['location']}`)\n  - *Suggestion:* {suggestion}")
    
    if len(risk_items) > 10:
        risk_list.append(f"- ... and {len(risk_items) - 10} more")
    
    blocked = any(item.get("is_transitive", False) for item in risk_items)
    status = "⚠️ WARNING" if not blocked else "🛑 BLOCKED"
    
    return textwrap.dedent(f"""\
        ## Risk Assessment
        
        **Status:** {status}  
        **Total Risks:** {len(risk_items)}
        
        {chr(10).join(risk_list)}
    """)


def _next_steps_section(state: PipelineState) -> str:
    steps = []
    
    compile_results = state.get("compile_results", [])
    if compile_results and not compile_results[-1].success:
        steps.append("1. **Fix Compilation Errors** - Review error log and apply fixes manually or re-run with increased patch attempts")
    
    test_result = state.get("test_result")
    if test_result and not test_result.passed:
        steps.append("2. **Fix Failing Tests** - Review test output and adjust generated code")
    
    risk_items = state.get("risk_items", [])
    if risk_items:
        steps.append(f"3. **Address {len(risk_items)} Risk Item(s)** - Review custom library usage and find Spark alternatives")
    
    if state.get("human_review_required"):
        steps.append("4. **Human Review Required** - Manual intervention needed before production deployment")
    
    if not steps:
        steps = [
            "1. **Review Generated Code** - Examine Scala files for correctness",
            "2. **Run Integration Tests** - Test with sample data",
            "3. **Performance Tuning** - Optimize Spark job configuration",
            "4. **Deploy to Production** - Package JAR and deploy to cluster",
        ]
    
    return "## Next Steps\n\n" + "\n".join(steps)


def _footer_section() -> str:
    return textwrap.dedent(f"""\
        ---
        
        *Report generated by MapReduce→Spark Migration Pipeline v1.0*
    """)
