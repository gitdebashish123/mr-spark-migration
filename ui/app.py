"""
Streamlit Application — MR → Spark Migration Pipeline UI.

Page layout:
  Sidebar  : configuration form (mirrors the HTML UI we designed)
  Main area: tab 1 = pipeline progress, tab 2 = generated code, tab 3 = report
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

# ── make src/ importable ──
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st

from src.utils.config import settings
from src.utils.logging import configure_logging, get_logger
from src.models.state import (
    PipelineConfig, SourceConfig, VersionConfig, RiskConfig, CodegenConfig,
    SourceMode, BuildTool, LLMProvider, RiskAction, NodeStatus,
    PipelineState,
)
from src.agents.graph import pipeline_graph

configure_logging(settings.log_level, settings.log_format)
logger = get_logger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Page config
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="MR → Spark Migration",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
# Custom CSS
# ─────────────────────────────────────────────────────────────────────────────

st.markdown("""
<style>
/* Dark theme refinements */
[data-testid="stSidebar"] {
    background-color: #0c0f1a;
    border-right: 1px solid #1c2a3f;
}
.stTabs [data-baseweb="tab-list"] {
    background-color: #0c0f1a;
    border-bottom: 1px solid #1c2a3f;
}
.stTabs [data-baseweb="tab"] {
    color: #94a3b8;
    font-weight: 600;
}
.stTabs [aria-selected="true"] {
    color: #4f8ef7 !important;
    border-bottom: 2px solid #4f8ef7;
}

/* Node status cards */
.node-card {
    padding: 10px 16px;
    border-radius: 8px;
    margin-bottom: 6px;
    border: 1px solid #1c2a3f;
    font-family: 'Fira Code', monospace;
    font-size: 12px;
    display: flex;
    align-items: center;
    gap: 10px;
}
.node-card.pending  { background: #0c0f1a; color: #475569; }
.node-card.running  { background: rgba(79,142,247,0.08); border-color:#4f8ef7; color:#4f8ef7; }
.node-card.success  { background: rgba(52,211,153,0.06); border-color:#34d399; color:#34d399; }
.node-card.failed   { background: rgba(248,113,113,0.06); border-color:#f87171; color:#f87171; }
.node-card.waiting  { background: rgba(251,191,36,0.06); border-color:#fbbf24; color:#fbbf24; }
.node-card.skipped  { background: #0c0f1a; color: #334155; }

.risk-chip {
    display:inline-block; padding:2px 8px;
    border-radius:4px; font-size:11px; font-weight:700;
    background:rgba(248,113,113,0.12); color:#f87171;
    border:1px solid rgba(248,113,113,0.25);
}
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# Session state init
# ─────────────────────────────────────────────────────────────────────────────

def _init_session():
    defaults = {
        "pipeline_running": False,
        "pipeline_state":   None,
        "run_log":          [],
        "risk_patterns":    ["com.company-name", "com.internal"],
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

_init_session()


# ─────────────────────────────────────────────────────────────────────────────
# Sidebar — Configuration
# ─────────────────────────────────────────────────────────────────────────────

def render_sidebar() -> PipelineConfig | None:
    with st.sidebar:
        st.markdown("### ⚡ MR → Spark Migration")
        st.markdown(
            "<div style='font-size:11px;color:#475569;margin-bottom:20px;'>"
            "Pipeline v2.0 · Phase 1</div>",
            unsafe_allow_html=True,
        )

        # ── Source Mode ──
        st.markdown("#### 🔗 Source Repository")
        source_mode = st.radio(
            "Source mode",
            options=["Git Repository", "Local Directory"],
            horizontal=True,
            label_visibility="collapsed",
        )

        if source_mode == "Git Repository":
            repo_url = st.text_input(
                "Repository URL",
                placeholder="https://github.com/org/repo.git",
            )
            branch = st.text_input("Branch", placeholder="main", value="main")
            git_token = st.text_input(
                "Git Token", type="password",
                placeholder="ghp_... (optional for public repos)",
            )
            subdir = st.text_input(
                "Sub-directory (optional)",
                placeholder="src/main/java/com/company/mr",
            )
            src_cfg = SourceConfig(
                mode=SourceMode.GIT,
                repo_url=repo_url or None,
                branch=branch or None,
                git_token=git_token or None,
                subdir=subdir or None,
            )
        else:
            local_path = st.text_input(
                "Absolute path",
                placeholder="/home/user/my-mr-project",
            )
            local_subdir = st.text_input(
                "Sub-directory (optional)",
                placeholder="src/main/java",
            )
            local_pattern = st.text_input(
                "File pattern", value="**/*.java",
            )
            col_a, col_b = st.columns(2)
            recurse = col_a.checkbox("Recurse sub-modules", value=True)
            skip_bd = col_b.checkbox("Skip build dirs", value=True)
            src_cfg = SourceConfig(
                mode=SourceMode.LOCAL,
                local_path=local_path or None,
                local_subdir=local_subdir or None,
                local_pattern=local_pattern,
                recurse_submodules=recurse,
                skip_build_dirs=skip_bd,
            )

        st.divider()

        # ── Build Tool ──
        st.markdown("#### ⚙️ Build Tool")
        build_tool_str = st.radio(
            "Build tool",
            options=["Maven", "Gradle"],
            horizontal=True,
            label_visibility="collapsed",
        )
        build_tool = BuildTool.MAVEN if build_tool_str == "Maven" else BuildTool.GRADLE

        st.divider()

        # ── Versions ──
        st.markdown("#### 📦 Target Versions")
        col1, col2 = st.columns(2)
        spark_v  = col1.selectbox("Spark",  ["4.0.1", "3.5.1", "3.4.2", "3.3.4"], index=0)
        scala_v  = col2.selectbox("Scala",  ["2.13.16", "2.12.18", "2.11.12"], index=0)
        jdk_v    = col1.selectbox("JDK",    ["17", "11", "21"], index=0)
        hadoop_v = col2.selectbox("Hadoop", ["3.4.2", "3.3.6", "3.2.4", "2.10.2"], index=0)

        # Compat warning
        spark_major = spark_v.split(".")[0]
        scala_minor = ".".join(scala_v.split(".")[:2])
        compat_ok = (
            (spark_major == "4" and scala_minor == "2.13") or
            (spark_major == "3" and scala_minor in ("2.12", "2.13"))
        )
        if not compat_ok:
            st.warning(f"⚠ Spark {spark_v} may not be compatible with Scala {scala_v}")
        else:
            st.success(f"✓ Spark {spark_v} ↔ Scala {scala_v} compatible", icon=None)

        versions = VersionConfig(spark=spark_v, scala=scala_v, jdk=jdk_v, hadoop=hadoop_v)

        st.divider()

        # ── LLM Provider ──
        st.markdown("#### 🤖 LLM Provider")
        provider_str = st.radio(
            "LLM Provider",
            options=["Claude Sonnet 4", "Gemini 2.5 Flash"],
            horizontal=True,
            label_visibility="collapsed",
        )
        if provider_str == "Claude Sonnet 4":
            provider = LLMProvider.CLAUDE
            model = "claude-sonnet-4-5-20250929"
            if not settings.has_anthropic:
                st.warning("⚠ ANTHROPIC_API_KEY not set in .env")
        else:
            provider = LLMProvider.GEMINI
            model = "gemini-2.5-flash"
            if not settings.has_google:
                st.warning("⚠ GOOGLE_API_KEY not set in .env")

        st.divider()

        # ── Risk Detection ──
        st.markdown("#### 🛡️ Custom Library Risk Patterns")
        st.caption("Imports / groupIds starting with these prefixes will be flagged.")

        patterns = st.session_state["risk_patterns"]
        updated_patterns = []
        for i, pat in enumerate(patterns):
            cols = st.columns([5, 1])
            val = cols[0].text_input(
                f"Pattern {i+1}", value=pat,
                key=f"pattern_{i}",
                label_visibility="collapsed",
            )
            if cols[1].button("✕", key=f"del_pattern_{i}"):
                patterns.pop(i)
                st.rerun()
            updated_patterns.append(val)

        st.session_state["risk_patterns"] = updated_patterns

        if st.button("+ Add pattern", use_container_width=True):
            st.session_state["risk_patterns"].append("")
            st.rerun()

        risk_action_str = st.radio(
            "On detection",
            ["Warn & Continue", "Block Pipeline", "Info Only"],
            horizontal=False,
        )
        action_map = {
            "Warn & Continue": RiskAction.WARN,
            "Block Pipeline":  RiskAction.BLOCK,
            "Info Only":       RiskAction.INFO,
        }
        risk_cfg = RiskConfig(
            prefixes=[p for p in updated_patterns if p.strip()],
            action=action_map[risk_action_str],
        )

        st.divider()

        # ── Launch ──
        can_launch = (
            (src_cfg.mode == SourceMode.GIT and src_cfg.repo_url and src_cfg.branch) or
            (src_cfg.mode == SourceMode.LOCAL and src_cfg.local_path)
        )

        launch = st.button(
            "🚀 Launch Pipeline",
            disabled=not can_launch or st.session_state["pipeline_running"],
            use_container_width=True,
            type="primary",
        )

        if not can_launch:
            st.caption("⬆ Set source to enable launch")

        if launch:
            return PipelineConfig(
                source=src_cfg,
                versions=versions,
                build_tool=build_tool,
                codegen=CodegenConfig(),
                risk=risk_cfg,
                llm_provider=provider,
                model_name=model,
            )

    return None


# ─────────────────────────────────────────────────────────────────────────────
# Node status icons
# ─────────────────────────────────────────────────────────────────────────────

STATUS_ICON = {
    NodeStatus.PENDING: "○",
    NodeStatus.RUNNING: "⟳",
    NodeStatus.SUCCESS: "✓",
    NodeStatus.FAILED:  "✗",
    NodeStatus.WAITING: "⏸",
    NodeStatus.SKIPPED: "–",
}

NODE_ORDER = [
    "config_validate", "source_resolve",
    "driver_analysis", "schema_extract",
    "jobgraph_infer", "diagram_render",
    "spark_codegen", "risk_scan",
    "project_scaffold", "compile_check",
    "critique_and_patch", "test_check",
    "human_review", "package_export",
]


def render_progress(pipeline_state: PipelineState):
    progress: dict = pipeline_state.get("node_progress", {})
    cols = st.columns(2)
    for i, node in enumerate(NODE_ORDER):
        col = cols[i % 2]
        np = progress.get(node)
        if np:
            status = np.status
            icon = STATUS_ICON.get(status, "?")
            dur = f" · {np.duration_ms}ms" if np.duration_ms else ""
            msg = f" — {np.message}" if np.message else ""
            col.markdown(
                f"<div class='node-card {status.value}'>"
                f"<span>{icon}</span>"
                f"<span><b>{node}</b>{msg}{dur}</span>"
                f"</div>",
                unsafe_allow_html=True,
            )
        else:
            col.markdown(
                f"<div class='node-card pending'>"
                f"<span>○</span><span>{node}</span></div>",
                unsafe_allow_html=True,
            )


# ─────────────────────────────────────────────────────────────────────────────
# Main area tabs
# ─────────────────────────────────────────────────────────────────────────────

def render_main():
    tab_progress, tab_code, tab_risk, tab_report = st.tabs([
        "📊 Pipeline Progress",
        "🧾 Generated Code",
        "🛡️ Risk Report",
        "📋 Migration Report",
    ])

    ps: PipelineState | None = st.session_state.get("pipeline_state")

    # ── Tab 1: Progress ──
    with tab_progress:
        if ps is None:
            st.info("Configure the pipeline in the sidebar and click **Launch Pipeline**.")
            st.markdown("#### Pipeline Nodes")
            dummy_cols = st.columns(2)
            for i, node in enumerate(NODE_ORDER):
                dummy_cols[i % 2].markdown(
                    f"<div class='node-card pending'>○ {node}</div>",
                    unsafe_allow_html=True,
                )
        else:
            config: PipelineConfig = ps.get("config")
            if config:
                st.markdown(
                    f"**Run ID:** `{config.run_id}` &nbsp;|&nbsp; "
                    f"**Source:** `{config.source.display_name}` &nbsp;|&nbsp; "
                    f"**LLM:** `{config.model_name}`"
                )
            render_progress(ps)

            if ps.get("human_review_required"):
                st.error(f"⏸ **Human Review Required** — {ps.get('human_review_message', '')}")
                
                # Show which node failed
                failed_nodes = [name for name, prog in ps.get("node_progress", {}).items() 
                               if prog.status == NodeStatus.FAILED]
                if failed_nodes:
                    st.warning(f"**Failed nodes:** {', '.join(failed_nodes)}")
                    for node_name in failed_nodes:
                        prog = ps["node_progress"][node_name]
                        if prog.error:
                            with st.expander(f"❌ {node_name} error details"):
                                st.code(prog.error)
                
                # Show compile errors if any
                compile_results = ps.get("compile_results", [])
                if compile_results and not compile_results[-1].success:
                    with st.expander("🔧 Compilation errors"):
                        st.code("\n".join(compile_results[-1].error_lines[:10]))
                
                # Show general errors
                if ps.get("errors"):
                    with st.expander("⚠️ Pipeline errors"):
                        for err in ps.get("errors", []):
                            st.error(err)
                
                if st.button("✓ Approve & Resume"):
                    st.info("Manual resume requires re-running the pipeline with fixes applied.")

            if ps.get("artifact_path"):
                st.success(f"✅ Pipeline complete! Artifact: `{ps['artifact_path']}`")

    # ── Tab 2: Generated Code ──
    with tab_code:
        if ps is None or not ps.get("generated_files"):
            st.info("Generated Scala files will appear here after spark_codegen runs.")
        else:
            # Show patched files if they exist, otherwise show original generated files
            patched_files = ps.get("patched_files", {})
            generated_files = ps.get("generated_files", {})
            
            # Merge: patched files override generated files
            files = {**generated_files, **patched_files}
            
            if patched_files:
                st.warning(f"⚠️ Showing {len(patched_files)} patched file(s) (LLM attempted fixes)")
            
            selected = st.selectbox("Select file", sorted(files.keys()))
            if selected:
                # Show if this file was patched
                if selected in patched_files:
                    st.info("🔧 This file was patched by the LLM after compilation errors")
                
                st.code(files[selected], language="scala")
                
                # Add download button
                st.download_button(
                    label="⬇️ Download this file",
                    data=files[selected],
                    file_name=selected.split("/")[-1],
                    mime="text/x-scala",
                )
            
            st.divider()
            
            # Download entire project as ZIP
            import zipfile
            import io
            from pathlib import Path
            
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
                # Add all Scala files
                for path, content in files.items():
                    zip_file.writestr(path, content)
                
                # Add Maven pom.xml if config exists
                config = ps.get("config")
                if config:
                    from src.tools.project_scaffold import _write_pom_xml
                    import tempfile
                    temp_dir = Path(tempfile.mkdtemp())
                    _write_pom_xml(temp_dir, config.versions.model_dump())
                    pom_content = (temp_dir / "pom.xml").read_text()
                    zip_file.writestr("pom.xml", pom_content)
            
            zip_buffer.seek(0)
            
            st.download_button(
                label="📦 Download Entire Project (ZIP)",
                data=zip_buffer.getvalue(),
                file_name=f"spark-migration-{ps.get('config').run_id if ps.get('config') else 'project'}.zip",
                mime="application/zip",
                type="primary",
            )

    # ── Tab 3: Risk Report ──
    with tab_risk:
        if ps is None or not ps.get("risk_items"):
            st.info("Custom library risk scan results will appear here.")
        else:
            risks = ps.get("risk_items", [])
            if not risks:
                st.success("✓ No custom library risks detected.")
            else:
                st.warning(f"⚠ {len(risks)} risk(s) detected")
                for r in risks:
                    with st.expander(f"[{r.matched_prefix}] {r.artifact}"):
                        st.markdown(f"**File:** `{r.location}`")
                        st.markdown(f"**Transitive:** {'Yes' if r.is_transitive else 'No'}")
                        if r.suggestion:
                            st.markdown(f"**Suggestion:** {r.suggestion}")

    # ── Tab 4: Report ──
    with tab_report:
        if ps is None or not ps.get("migration_report"):
            st.info("The migration report will be generated after package_export runs.")
        else:
            st.markdown(ps["migration_report"])
            st.download_button(
                "⬇ Download Report (Markdown)",
                data=ps["migration_report"],
                file_name="migration_report.md",
                mime="text/markdown",
            )


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline runner
# ─────────────────────────────────────────────────────────────────────────────

def run_pipeline(config: PipelineConfig):
    st.session_state["pipeline_running"] = True
    st.session_state["run_log"] = []

    initial_state: PipelineState = {
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

    logger.info("pipeline.start", run_id=config.run_id)

    # Stream node-by-node updates
    progress_placeholder = st.empty()

    try:
        final_state = initial_state
        for step in pipeline_graph.stream(initial_state, stream_mode="updates"):
            for node_name, node_output in step.items():
                # Merge update into running state
                for k, v in node_output.items():
                    final_state[k] = v  # type: ignore[literal-required]
                st.session_state["pipeline_state"] = final_state
                with progress_placeholder.container():
                    render_progress(final_state)
                time.sleep(0.05)   # small delay so Streamlit can redraw

        st.session_state["pipeline_state"] = final_state
        logger.info("pipeline.complete", run_id=config.run_id)

    except Exception as exc:
        logger.exception("pipeline.error", exc=str(exc))
        st.error(f"Pipeline error: {exc}")
    finally:
        st.session_state["pipeline_running"] = False


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    st.title("⚡ MapReduce → Spark / Scala Migration")
    st.caption("Agentic pipeline · Phase 1 · All nodes active with stub tools")

    config = render_sidebar()
    render_main()

    if config is not None:
        run_pipeline(config)
        st.rerun()


if __name__ == "__main__":
    main()
