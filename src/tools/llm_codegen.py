"""
Phase 3 — LLM-assisted Job Graph Inference and Spark Code Generation.

Two main callables:
  infer_job_graph_llm(classes, schemas, build_file, config) → MRJob graph JSON
  generate_spark_code_llm(job_graph, versions, codegen_config, config) → {path: scala_src}

Provider routing:
  LLMProvider.CLAUDE → Anthropic claude-sonnet-4-20250514
  LLMProvider.GEMINI → Google gemini-2.0-flash-exp

Both functions follow a structured-output pattern:
  - System prompt constrains output to JSON only
  - Response is parsed and validated against Pydantic models
  - On JSON parse failure, up to MAX_PARSE_RETRIES with error feedback
"""
from __future__ import annotations

import json
import re
import textwrap
from typing import Any, Optional, Callable

from src.models.state import (
    PipelineConfig, LLMProvider,
    MRJob, JavaClass, DetectedSchema,
)
from src.utils.logging import get_logger

logger = get_logger(__name__)

MAX_PARSE_RETRIES = 3


# ─────────────────────────────────────────────────────────────────────────────
# Enhanced JSON Parsing with Multiple Fallback Strategies
# ─────────────────────────────────────────────────────────────────────────────

def _parse_llm_json(raw: str, context: str, max_retries: int = 3) -> dict[str, Any]:
    """
    Parse JSON from LLM response with multiple fallback strategies.
    
    Args:
        raw: Raw LLM response text
        context: Description for logging (e.g., "job graph", "spark codegen")
        max_retries: Number of cleanup attempts
        
    Returns:
        Parsed JSON dict
        
    Raises:
        RuntimeError: If all parse attempts fail
    """
    
    # Strategy 1: Direct parse
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        logger.warning(f"llm.{context}.parse_direct_fail", error=str(e))
    
    # Strategy 2: Extract JSON from markdown code blocks
    json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', raw, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group(1))
        except json.JSONDecodeError as e:
            logger.warning(f"llm.{context}.parse_markdown_fail", error=str(e))
    
    # Strategy 3: Find first { to last }
    try:
        start = raw.index('{')
        end = raw.rindex('}') + 1
        json_str = raw[start:end]
        return json.loads(json_str)
    except (ValueError, json.JSONDecodeError) as e:
        logger.warning(f"llm.{context}.parse_extract_fail", error=str(e))
    
    # Strategy 4: Clean and retry
    for attempt in range(max_retries):
        try:
            # Remove common issues
            cleaned = raw.strip()
            cleaned = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', cleaned)  # Remove control chars
            cleaned = re.sub(r'\\(?!["\\/bfnrt])', r'\\\\', cleaned)  # Fix backslashes
            
            # Try to extract JSON object
            start = cleaned.find('{')
            end = cleaned.rfind('}') + 1
            if start >= 0 and end > start:
                cleaned = cleaned[start:end]
            
            return json.loads(cleaned)
        except json.JSONDecodeError as e:
            logger.warning(f"llm.{context}.parse_retry", attempt=attempt + 1, error=str(e))
    
    # All strategies failed
    logger.error(
        f"llm.{context}.parse_failed",
        raw_length=len(raw),
        raw_start=raw[:1000],
        raw_end=raw[-1000:] if len(raw) > 1000 else "",
        has_opening_brace="{" in raw,
        has_closing_brace="}" in raw,
    )
    raise RuntimeError(f"LLM JSON parse error for {context}: Failed after {max_retries} attempts")


def _parse_json_response(
    raw: str,
    label: str,
    validator: Callable[[dict], dict],
    attempt: int = 0,
) -> dict[str, Any]:
    """
    Parse JSON from LLM response text with robust error handling.
    
    Args:
        raw: Raw LLM response
        label: Context label for logging
        validator: Function to validate and enrich parsed JSON
        attempt: Current retry attempt (for recursive retry)
        
    Returns:
        Validated JSON dict
        
    Raises:
        RuntimeError: After MAX_PARSE_RETRIES failed attempts
    """
    try:
        data = _parse_llm_json(raw, label.replace(' ', '_'), max_retries=MAX_PARSE_RETRIES)
        validated = validator(data)
        logger.info(f"llm.{label.replace(' ','_')}.parsed_ok")
        return validated
    except (json.JSONDecodeError, ValueError, RuntimeError) as exc:
        if attempt >= MAX_PARSE_RETRIES:
            logger.error(f"llm.{label}.parse_failed", error=str(exc), raw=raw[:300])
            raise RuntimeError(
                f"LLM returned invalid JSON for {label} after "
                f"{MAX_PARSE_RETRIES + 1} attempts: {exc}"
            ) from exc
        
        logger.warning(f"llm.{label}.parse_retry", attempt=attempt + 1, error=str(exc))
        raise RuntimeError(f"LLM JSON parse error for {label}: {exc}") from exc


# ─────────────────────────────────────────────────────────────────────────────
# Job Graph Inference
# ─────────────────────────────────────────────────────────────────────────────

_JOBGRAPH_SYSTEM = textwrap.dedent("""\
    You are a Hadoop MapReduce expert. You receive metadata about Java classes
    and data schemas from a MapReduce project. Your job is to infer the full
    job graph — which classes form which jobs, how jobs chain, and what the
    data flow looks like.

    CRITICAL: Respond ONLY with a single valid JSON object. No prose, no markdown fences,
    no explanation, no code blocks — just the raw JSON object starting with { and ending with }.

    Required top-level keys:
    {
      "jobs": [
        {
          "job_name": "string",
          "driver":   {"class_name": "...", "role": "driver"},
          "mapper":   {"class_name": "...", "role": "mapper"},
          "reducer":  {"class_name": "...", "role": "reducer"},
          "combiner": {"class_name": "...", "role": "combiner"} | null,
          "partitioner": {"class_name": "...", "role": "partitioner"} | null,
          "composite_keys": [{"class_name": "...", "role": "composite_key"}],
          "input_paths":  ["/data/input"],
          "output_paths": ["/data/output"],
          "input_format":  "TextInputFormat",
          "output_format": "TextOutputFormat",
          "chained_after": null | "previous_job_name"
        }
      ],
      "chain_count": 1,
      "complexity": "simple" | "medium" | "complex"
    }
    
    Ensure all strings in the JSON are properly escaped. Do not use newlines inside string values.
""")


def infer_job_graph_llm(
    classes: list[dict],
    schemas: list[dict],
    build_file_content: Optional[str],
    config: PipelineConfig,
) -> dict[str, Any]:
    """
    Call the LLM to infer the MapReduce job graph from class metadata.

    Args:
        classes:            JavaClass dicts from driver_analysis.
        schemas:            DetectedSchema dicts from schema_extract.
        build_file_content: Raw pom.xml / build.gradle content.
        config:             Pipeline config (LLM provider, model name).

    Returns:
        Validated job graph dict matching the schema above.
    """
    user_prompt = _build_jobgraph_prompt(classes, schemas, build_file_content)
    raw = _call_llm(_JOBGRAPH_SYSTEM, user_prompt, config, max_tokens=2048)
    return _parse_json_response(raw, "job graph", _validate_job_graph)


def _build_jobgraph_prompt(
    classes: list[dict],
    schemas: list[dict],
    build_file: Optional[str],
) -> str:
    class_summary = json.dumps(
        [{"class_name": c["class_name"], "role": c["role"],
          "extends": c.get("extends"), "implements": c.get("implements", [])}
         for c in classes],
        indent=2,
    )
    schema_summary = json.dumps(
        [{"source_class": s["source_class"],
          "fields": s.get("fields", []),
          "format": s.get("format")}
         for s in schemas],
        indent=2,
    )
    parts = [
        "## Java Classes",
        class_summary,
        "## Detected Schemas",
        schema_summary,
    ]
    if build_file:
        parts += ["## Build File (first 500 chars)", build_file[:500]]
    
    parts.append("\nRespond with ONLY the JSON object. No other text, no markdown, no explanations.")
    return "\n\n".join(parts)


def _validate_job_graph(data: dict) -> dict:
    """Ensure required keys exist; fill defaults for optional fields."""
    if "jobs" not in data:
        raise ValueError("Missing 'jobs' key in job graph response")
    for job in data["jobs"]:
        job.setdefault("combiner", None)
        job.setdefault("partitioner", None)
        job.setdefault("composite_keys", [])
        job.setdefault("input_paths", ["/data/input"])
        job.setdefault("output_paths", ["/data/output"])
        job.setdefault("input_format", "TextInputFormat")
        job.setdefault("output_format", "TextOutputFormat")
        job.setdefault("chained_after", None)
    data.setdefault("chain_count", len(data["jobs"]))
    data.setdefault("complexity", "medium")
    return data


# ─────────────────────────────────────────────────────────────────────────────
# Spark Code Generation
# ─────────────────────────────────────────────────────────────────────────────

_CODEGEN_SYSTEM = textwrap.dedent("""\
    You are a Spark/Scala expert specializing in migrating Hadoop MapReduce jobs
    to Apache Spark. Generate idiomatic, production-quality Scala code.

    Rules:
    - Use the Dataset/DataFrame API unless explicitly asked for RDD
    - Separate concerns: Driver (main), Job (orchestration), Service (transformations)
    - No business logic in the driver or job — only in services
    - Use SparkSession.builder().getOrCreate() — never create SparkContext directly
    - Prefer spark.implicits._ for encoders
    - Handle edge cases: empty input, null values, schema evolution
    - Include ScalaDoc comments on all public methods

    CRITICAL BUILD FILE RULES:
    - Generate ONLY Scala source files (.scala extension)
    - DO NOT generate build files (no pom.xml, no build.gradle, no build.sbt, no project/build.properties)
    - The build system is handled separately - you only generate application code
    - All files must be under src/main/scala/ or src/test/scala/

    CRITICAL: Respond ONLY with a JSON object mapping relative file paths to file contents.
    No prose, no markdown fences, no code blocks — just the raw JSON object starting with { and ending with }.
    
    All file content strings MUST be properly escaped for JSON (escape quotes, newlines, etc.).

    Format:
    {
      "files": {
        "src/main/scala/com/example/spark/driver/AppNameApp.scala": "package com.example.spark.driver\\n\\nobject AppNameApp {\\n  def main(args: Array[String]): Unit = {\\n    ...\\n  }\\n}",
        "src/main/scala/com/example/spark/jobs/AppNameJob.scala": "package ...",
        "src/main/scala/com/example/spark/service/AppNameService.scala": "package ..."
      },
      "file_count": 3,
      "warnings": []
    }
""")


def generate_spark_code_llm(
    job_graph: dict,
    versions: dict,
    codegen_config: dict,
    config: PipelineConfig,
) -> dict[str, Any]:
    """
    Call the LLM to generate Spark/Scala source files from the job graph.
    Falls back to extracting code from raw text if JSON parsing fails completely.

    Args:
        job_graph:      Validated job graph dict from infer_job_graph_llm.
        versions:       {spark, scala, jdk, hadoop} version strings.
        codegen_config: CodegenConfig.model_dump().
        config:         Pipeline config (LLM provider, model name).

    Returns:
        {"files": {path: content}, "file_count": int, "warnings": [str]}
    """
    user_prompt = _build_codegen_prompt(job_graph, versions, codegen_config)
    raw = _call_llm(_CODEGEN_SYSTEM, user_prompt, config, max_tokens=8192)
    
    try:
        return _parse_json_response(raw, "spark codegen", _validate_codegen)
    except RuntimeError as exc:
        # JSON parsing completely failed - extract Scala code as fallback
        logger.error("spark_codegen.parse_total_failure", error=str(exc))
        
        scala_files = _extract_scala_fallback(raw, job_graph)
        
        if scala_files:
            logger.warning("spark_codegen.fallback_extraction", files_found=len(scala_files))
            return {
                "files": scala_files,
                "file_count": len(scala_files),
                "warnings": [
                    "⚠️ JSON parsing failed - extracted Scala code from raw text",
                    "Code may be incomplete. Review carefully before use.",
                    f"Parse error: {str(exc)[:200]}",
                ],
            }
        
        # Total failure - re-raise
        raise


def _build_codegen_prompt(
    job_graph: dict,
    versions: dict,
    codegen_config: dict,
) -> str:
    jobs = job_graph.get("jobs", [])
    job_names = [j["job_name"] for j in jobs]

    api = codegen_config.get("api_strategy", "dataframe_enforced")
    typed = codegen_config.get("typed_dataset", True)
    gen_tests = codegen_config.get("generate_tests", True)

    return textwrap.dedent(f"""\
        ## Job Graph
        {json.dumps(job_graph, indent=2)}

        ## Target Versions
        - Spark: {versions.get("spark", "4.0.1")}
        - Scala: {versions.get("scala", "2.13.16")}
        - JDK:   {versions.get("jdk", "17")}
        - Hadoop: {versions.get("hadoop", "3.4.2")}

        ## Codegen Options
        - API strategy: {api}  (dataframe_enforced = use Dataset/DataFrame, rdd_only = RDD)
        - Typed Datasets: {typed}
        - Generate unit test stubs: {gen_tests}

        ## Instructions
        Generate complete, compilable Scala files for the following jobs: {job_names}.
        Use the three-layer architecture (Driver / Job / Service) for each job.
        {"Also generate a test stub for each Service class." if gen_tests else ""}
        
        Respond with ONLY the JSON object. No other text, no markdown, no explanations.
        All Scala code must be properly escaped as JSON strings (escape newlines as \\n, quotes as \\", etc.).
    """)


def _validate_codegen(data: dict) -> dict:
    """Ensure files key exists and values are strings. Filter out build files."""
    if "files" not in data:
        raise ValueError("Missing 'files' key in codegen response")
    
    # Filter out build files - LLM sometimes generates these despite instructions
    forbidden_patterns = [
        "pom.xml", "build.gradle", "build.sbt", "build.properties",
        "settings.gradle", "gradle.properties", "project/",
    ]
    
    filtered_files = {}
    warnings = data.get("warnings", [])
    
    for path, content in data["files"].items():
        if not isinstance(content, str):
            raise ValueError(f"File content for {path!r} is not a string")
        
        # Check if this is a forbidden build file
        if any(pattern in path for pattern in forbidden_patterns):
            warnings.append(f"Filtered out build file: {path}")
            continue
        
        # Only keep .scala files
        if not path.endswith(".scala"):
            warnings.append(f"Filtered out non-Scala file: {path}")
            continue
            
        filtered_files[path] = content
    
    data["files"] = filtered_files
    data["file_count"] = len(filtered_files)
    data["warnings"] = warnings
    return data


def _extract_scala_fallback(raw: str, job_graph: dict) -> dict[str, str]:
    """
    Fallback: extract Scala code blocks from malformed LLM response.
    """
    files = {}
    
    # Look for Scala code blocks
    code_blocks = re.findall(r'```(?:scala)?\s*(package\s+[\w.]+.*?)```', raw, re.DOTALL)
    
    jobs = job_graph.get("jobs", [])
    job_name = jobs[0]["job_name"] if jobs else "SparkJob"
    
    for i, code in enumerate(code_blocks):
        # Infer layer from code content
        if "def main" in code:
            path = f"src/main/scala/com/example/spark/driver/{job_name}App.scala"
        elif i == 1 or "def run" in code:
            path = f"src/main/scala/com/example/spark/jobs/{job_name}Job.scala"
        else:
            path = f"src/main/scala/com/example/spark/service/{job_name}Service.scala"
        
        files[path] = code.strip()
    
    # If no code blocks found, create minimal stub
    if not files:
        files[f"src/main/scala/com/example/spark/App.scala"] = (
            f"package com.example.spark\n\n"
            f"object App {{\n"
            f"  def main(args: Array[String]): Unit = {{\n"
            f"    println(\"LLM response was malformed - manual code review required\")\n"
            f"  }}\n"
            f"}}\n"
        )
    
    return files


# ─────────────────────────────────────────────────────────────────────────────
# LLM provider routing
# ─────────────────────────────────────────────────────────────────────────────

def _call_llm(
    system: str,
    user: str,
    config: PipelineConfig,
    max_tokens: int = 4096,
) -> str:
    """Route to the correct LLM provider and return the raw text response."""
    provider = config.llm_provider
    model = config.model_name

    logger.info("llm.call", provider=provider.value, model=model, max_tokens=max_tokens)

    if provider == LLMProvider.CLAUDE:
        return _call_claude(system, user, model, max_tokens)
    elif provider == LLMProvider.GEMINI:
        return _call_gemini(system, user, model, max_tokens)
    else:
        raise ValueError(f"Unknown LLM provider: {provider}")


def _call_claude(system: str, user: str, model: str, max_tokens: int) -> str:
    try:
        import anthropic
    except ImportError:
        raise RuntimeError("anthropic package not installed. Run: uv add anthropic")

    from src.utils.config import settings
    
    logger.info("claude.call", model=model, api_key_set=bool(settings.anthropic_api_key))
    
    client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

    try:
        response = client.messages.create(
            model=model,
            max_tokens=max_tokens,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        return response.content[0].text
    except anthropic.BadRequestError as e:
        logger.error("claude.bad_request", error=str(e), model=model)
        raise RuntimeError(f"Claude API error: {e}") from e
    except anthropic.NotFoundError as e:
        logger.error("claude.not_found", error=str(e), model=model)
        raise RuntimeError(f"Claude model not found: {model}. Use 'claude-sonnet-4-20250514' or 'claude-3-5-sonnet-latest'") from e


def _call_gemini(system: str, user: str, model: str, max_tokens: int) -> str:
    try:
        import google.generativeai as genai
    except ImportError:
        raise RuntimeError("google-generativeai not installed. Run: uv add google-generativeai")

    from src.utils.config import settings
    genai.configure(api_key=settings.google_api_key)

    gemini_model = genai.GenerativeModel(
        model_name=model,
        system_instruction=system,
    )
    response = gemini_model.generate_content(
        user,
        generation_config=genai.GenerationConfig(max_output_tokens=max_tokens),
    )
    return response.text
