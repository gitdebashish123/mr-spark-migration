"""
Phase 4 — Build Runner and Test Executor.

Three main callables:
  compile_scala(project_path, build_tool, timeout) → CompileResult
  run_tests(project_path, build_tool, timeout) → TestResult
  critique_and_patch_llm(error_lines, files, config) → {patched_files, explanation}

Compile/test use subprocess to invoke Maven/Gradle. The critique_and_patch
function sends compile errors to the LLM and asks for corrected code.
"""
from __future__ import annotations

import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Optional

from src.models.state import PipelineConfig
from src.utils.logging import get_logger

logger = get_logger(__name__)

MAX_COMPILE_OUTPUT = 2000  # chars to capture from stdout/stderr


# ─────────────────────────────────────────────────────────────────────────────
# Compile Scala via Maven/Gradle
# ─────────────────────────────────────────────────────────────────────────────

def compile_scala(
    project_path: str,
    build_tool: str,
    timeout_seconds: int = 120,
) -> dict[str, Any]:
    """
    Invoke Maven or Gradle compile and return structured result.

    Args:
        project_path:    Path to the scaffolded Spark project.
        build_tool:      "maven" | "gradle".
        timeout_seconds: Max time to wait.

    Returns:
        {"success": bool, "stdout": str, "stderr": str, "error_lines": [str], "attempt": int}
    """
    project_root = Path(project_path)
    if not project_root.exists():
        return {
            "success": False,
            "stdout": "",
            "stderr": f"Project path does not exist: {project_path}",
            "error_lines": [f"Project path does not exist: {project_path}"],
            "attempt": 1,
        }

    if build_tool == "maven":
        cmd = ["mvn", "clean", "compile", "-q"]
    elif build_tool == "gradle":
        cmd = ["./gradlew", "clean", "compileScala", "-q"]
    else:
        raise ValueError(f"Unknown build tool: {build_tool}")

    logger.info("build.compile.start", tool=build_tool, path=project_path)

    try:
        result = subprocess.run(
            cmd,
            cwd=str(project_root),
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
        success = result.returncode == 0
        stdout = result.stdout[-MAX_COMPILE_OUTPUT:] if result.stdout else ""
        stderr = result.stderr[-MAX_COMPILE_OUTPUT:] if result.stderr else ""
        error_lines = _extract_error_lines(stderr if stderr else stdout)

        logger.info(
            "build.compile.done",
            success=success,
            returncode=result.returncode,
            error_count=len(error_lines),
        )

        return {
            "success": success,
            "stdout": stdout,
            "stderr": stderr,
            "error_lines": error_lines,
            "attempt": 1,
        }

    except subprocess.TimeoutExpired:
        logger.error("build.compile.timeout", timeout=timeout_seconds)
        return {
            "success": False,
            "stdout": "",
            "stderr": f"Compilation timed out after {timeout_seconds}s",
            "error_lines": [f"Compilation timed out after {timeout_seconds}s"],
            "attempt": 1,
        }
    except FileNotFoundError as exc:
        logger.error("build.compile.command_not_found", cmd=cmd[0], error=str(exc))
        return {
            "success": False,
            "stdout": "",
            "stderr": f"Build command not found: {cmd[0]}. Ensure {build_tool} is installed.",
            "error_lines": [f"Build command not found: {cmd[0]}"],
            "attempt": 1,
        }


def _extract_error_lines(output: str) -> list[str]:
    """Extract compiler error lines from Maven/Gradle output."""
    # Common error patterns: [ERROR], error:, cannot find symbol, not found
    pattern = re.compile(
        r"(?:\[ERROR\]|error:|cannot find symbol|not found:|value .* is not a member)",
        re.IGNORECASE,
    )
    lines = [line.strip() for line in output.split("\n") if pattern.search(line)]
    return lines[:20]  # cap at 20 errors to avoid huge context


# ─────────────────────────────────────────────────────────────────────────────
# Run Tests (Maven/Gradle)
# ─────────────────────────────────────────────────────────────────────────────

def run_tests(
    project_path: str,
    build_tool: str,
    timeout_seconds: int = 180,
) -> dict[str, Any]:
    """
    Invoke test suite and return pass/fail result.

    Args:
        project_path:    Path to the scaffolded Spark project.
        build_tool:      "maven" | "gradle".
        timeout_seconds: Max time to wait.

    Returns:
        {"passed": bool, "mr_row_count": int | None, "spark_row_count": int | None,
         "checksum_match": bool | None, "details": str}
    """
    project_root = Path(project_path)

    if build_tool == "maven":
        cmd = ["mvn", "test", "-q"]
    elif build_tool == "gradle":
        cmd = ["./gradlew", "test", "-q"]
    else:
        raise ValueError(f"Unknown build tool: {build_tool}")

    logger.info("build.test.start", tool=build_tool, path=project_path)

    try:
        result = subprocess.run(
            cmd,
            cwd=str(project_root),
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
        passed = result.returncode == 0
        details = (result.stdout or "") + (result.stderr or "")

        logger.info("build.test.done", passed=passed)

        # Phase 4: no real MR baseline data yet — these fields stay None
        # Phase 5+ can implement actual row count / checksum comparison
        return {
            "passed": passed,
            "mr_row_count": None,
            "spark_row_count": None,
            "checksum_match": None,
            "details": details[-500:],  # last 500 chars
        }

    except subprocess.TimeoutExpired:
        logger.error("build.test.timeout", timeout=timeout_seconds)
        return {
            "passed": False,
            "mr_row_count": None,
            "spark_row_count": None,
            "checksum_match": None,
            "details": f"Test execution timed out after {timeout_seconds}s",
        }


# ─────────────────────────────────────────────────────────────────────────────
# Critique and Patch (LLM)
# ─────────────────────────────────────────────────────────────────────────────

_CRITIQUE_SYSTEM = """\
You are a Scala compiler expert. You receive:
  - A list of compiler error messages
  - The full source code of the files with errors

Your job: identify the root cause and produce CORRECTED versions of ONLY
the files that need changes. Respond ONLY with a JSON object:

{
  "explanation": "Brief analysis of what went wrong and how you fixed it",
  "patched_files": {
    "relative/path/File.scala": "corrected source code here",
    ...
  }
}

Do NOT include files that have no changes. Only return files you actually modified.
"""


def critique_and_patch_llm(
    error_lines: list[str],
    generated_files: dict[str, str],
    config: PipelineConfig,
) -> dict[str, Any]:
    """
    Send compile errors to the LLM and get back corrected source files.

    Args:
        error_lines:      Extracted error messages from compile output.
        generated_files:  Current {path: source} mapping.
        config:           Pipeline config (LLM provider).

    Returns:
        {"patched_files": {path: corrected_source}, "explanation": str}
    """
    from src.tools.llm_codegen import _call_llm, _parse_json_response

    error_summary = "\n".join(error_lines[:15])  # cap to avoid huge prompts
    file_listing = "\n\n".join(
        f"## {path}\n```scala\n{src}\n```"
        for path, src in list(generated_files.items())[:5]  # cap to 5 files
    )

    user_prompt = f"""\
## Compiler Errors
{error_summary}

## Source Files
{file_listing}

Analyze the errors and produce corrected versions of any files that need changes.
"""

    raw = _call_llm(_CRITIQUE_SYSTEM, user_prompt, config, max_tokens=4096)
    result = _parse_json_response(raw, "critique_and_patch", _validate_patch)
    logger.info(
        "llm.critique.done",
        patched_count=len(result.get("patched_files", {})),
    )
    return result


def _validate_patch(data: dict) -> dict:
    """Ensure patched_files key exists."""
    if "patched_files" not in data:
        raise ValueError("Missing 'patched_files' key in critique response")
    data.setdefault("explanation", "No explanation provided")
    return data
