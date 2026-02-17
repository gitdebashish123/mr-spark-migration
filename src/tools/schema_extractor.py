"""
Phase 2 — Schema Extractor.

Resolves MapReduce WritableComparable types and Avro schemas to
Spark StructType-compatible field definitions using:
  1. Static Java source analysis (Writable field declarations)
  2. fastavro for .avsc / .avro schema files
  3. Heuristic Writable → Spark type mapping table
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Optional

from src.utils.logging import get_logger

logger = get_logger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Writable → Spark type mapping
# ─────────────────────────────────────────────────────────────────────────────

WRITABLE_TO_SPARK: dict[str, str] = {
    # Hadoop Writables
    "Text":              "StringType",
    "IntWritable":       "IntegerType",
    "LongWritable":      "LongType",
    "FloatWritable":     "FloatType",
    "DoubleWritable":    "DoubleType",
    "BooleanWritable":   "BooleanType",
    "BytesWritable":     "BinaryType",
    "NullWritable":      "NullType",
    "VIntWritable":      "IntegerType",
    "VLongWritable":     "LongType",
    # Java primitives / boxed
    "String":            "StringType",
    "Integer":           "IntegerType",
    "int":               "IntegerType",
    "Long":              "LongType",
    "long":              "LongType",
    "Float":             "FloatType",
    "float":             "FloatType",
    "Double":            "DoubleType",
    "double":            "DoubleType",
    "Boolean":           "BooleanType",
    "boolean":           "BooleanType",
    "byte[]":            "BinaryType",
    "Date":              "DateType",
    "Timestamp":         "TimestampType",
    "BigDecimal":        "DecimalType(38,10)",
}

# Avro primitive → Spark type
AVRO_TO_SPARK: dict[str, str] = {
    "string":  "StringType",
    "int":     "IntegerType",
    "long":    "LongType",
    "float":   "FloatType",
    "double":  "DoubleType",
    "boolean": "BooleanType",
    "bytes":   "BinaryType",
    "null":    "NullType",
}


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def extract_schemas(
    class_metadata: list[dict],
    source_root: str,
    data_paths: Optional[list[str]] = None,
) -> dict:
    """
    Resolve schemas from class metadata and optionally from data files.

    Strategy (in order):
      1. For composite_key classes — parse field declarations from Java source
      2. For Avro format hints — read .avsc schema files with fastavro
      3. Infer mapper/reducer input-output types from extends clause generics

    Args:
        class_metadata: List of JavaClass dicts from driver_analysis.
        source_root:    Root directory for reading source files.
        data_paths:     Optional explicit paths to .avsc / .avro files.

    Returns:
        {"schemas": [DetectedSchema dict, ...], "unresolved": [class_name, ...]}
    """
    schemas = []
    unresolved = []

    for cls in class_metadata:
        role = cls.get("role")
        class_name = cls.get("class_name", "Unknown")

        if role == "composite_key":
            schema = _extract_composite_key_schema(cls, source_root)
            if schema:
                schemas.append(schema)
            else:
                unresolved.append(class_name)

        elif role in ("mapper", "reducer", "combiner"):
            schema = _extract_from_generics(cls)
            if schema:
                schemas.append(schema)

    # Avro schema files if provided or found in source_root
    avro_schemas = _scan_avro_schemas(source_root, data_paths or [])
    schemas.extend(avro_schemas)

    logger.info(
        "schema.extraction_complete",
        resolved=len(schemas),
        unresolved=len(unresolved),
    )

    return {"schemas": schemas, "unresolved": unresolved}


# ─────────────────────────────────────────────────────────────────────────────
# Composite key extraction (WritableComparable field analysis)
# ─────────────────────────────────────────────────────────────────────────────

def _extract_composite_key_schema(cls: dict, source_root: str) -> Optional[dict]:
    """
    Parse field declarations from a WritableComparable class source file.
    Looks for:  private Text fieldName; / private IntWritable count; etc.
    """
    file_path = cls.get("file_path")
    if not file_path:
        return None

    full_path = Path(source_root) / file_path
    if not full_path.exists():
        logger.debug("schema.composite_key.file_not_found", path=str(full_path))
        return _schema_from_implements_hint(cls)

    source = full_path.read_text(encoding="utf-8", errors="replace")
    fields = _parse_writable_fields(source)

    if not fields:
        fields = _infer_fields_from_write_method(source)

    if not fields:
        logger.debug("schema.composite_key.no_fields", class_name=cls["class_name"])
        return None

    return {
        "source_class": cls["class_name"],
        "fields": fields,
        "format": "writable",
    }


def _parse_writable_fields(source: str) -> list[dict]:
    """
    Extract field declarations like:
      private Text groupId;
      private LongWritable timestamp;
      private IntWritable count;
    """
    pattern = re.compile(
        r"(?:private|protected|public)?\s+"
        r"(Text|IntWritable|LongWritable|FloatWritable|DoubleWritable|"
        r"BooleanWritable|BytesWritable|VIntWritable|VLongWritable|"
        r"String|Integer|Long|Float|Double|Boolean|BigDecimal|Date|Timestamp)"
        r"\s+(\w+)"
        r"(?:\s*=\s*[^;]+)?"   # optional initializer e.g. = new Text()
        r"\s*;",
        re.MULTILINE,
    )
    fields = []
    for match in pattern.finditer(source):
        java_type = match.group(1)
        field_name = match.group(2)
        spark_type = WRITABLE_TO_SPARK.get(java_type, "StringType")
        fields.append({
            "name":       field_name,
            "java_type":  java_type,
            "spark_type": spark_type,
        })
    return fields


def _infer_fields_from_write_method(source: str) -> list[dict]:
    """
    Fallback: scan readFields/write method bodies for field accesses like
      this.groupId.readFields(in);
      out.writeLong(timestamp);
    """
    write_pattern = re.compile(
        r"(?:out\.write(?:UTF|Long|Int|Float|Double|Boolean|Bytes)|"
        r"this\.(\w+)\.(?:write|readFields))",
        re.MULTILINE,
    )
    fields = []
    seen = set()
    for match in write_pattern.finditer(source):
        name = match.group(1)
        if name and name not in seen:
            seen.add(name)
            fields.append({
                "name":       name,
                "java_type":  "Unknown",
                "spark_type": "StringType",   # conservative default
            })
    return fields


def _schema_from_implements_hint(cls: dict) -> Optional[dict]:
    """
    Last resort: if we can't read the file, produce a minimal schema
    from the class name itself (e.g. CompositeKey → [{key, StringType}]).
    """
    return {
        "source_class": cls["class_name"],
        "fields": [{"name": "key", "java_type": "Unknown", "spark_type": "StringType"}],
        "format": "writable",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Mapper/Reducer generic type extraction
# ─────────────────────────────────────────────────────────────────────────────

def _extract_from_generics(cls: dict) -> Optional[dict]:
    """
    Extract input/output key-value types from generics:
      Mapper<LongWritable, Text, Text, IntWritable>
        → input_key=LongWritable, input_val=Text, output_key=Text, output_val=IntWritable
    """
    extends_str = cls.get("extends") or ""
    match = re.search(r"<(.+)>", extends_str)
    if not match:
        return None

    raw = match.group(1)
    # Split on commas NOT inside angle brackets
    parts = _split_generics(raw)
    if len(parts) < 4:
        return None

    field_names = ["input_key", "input_value", "output_key", "output_value"]
    fields = []
    for name, java_type in zip(field_names, parts):
        java_type = java_type.strip()
        spark_type = WRITABLE_TO_SPARK.get(java_type, "StringType")
        fields.append({
            "name":       name,
            "java_type":  java_type,
            "spark_type": spark_type,
        })

    return {
        "source_class": cls["class_name"],
        "fields": fields,
        "format": "writable",
    }


def _split_generics(s: str) -> list[str]:
    """Split comma-separated type params, respecting nested angle brackets."""
    parts, depth, current = [], 0, []
    for ch in s:
        if ch == "<":
            depth += 1
            current.append(ch)
        elif ch == ">":
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
    if current:
        parts.append("".join(current).strip())
    return parts


# ─────────────────────────────────────────────────────────────────────────────
# Avro schema file scanning
# ─────────────────────────────────────────────────────────────────────────────

def _scan_avro_schemas(source_root: str, explicit_paths: list[str]) -> list[dict]:
    """Find and parse .avsc schema files in the project."""
    root = Path(source_root)
    schema_paths = list(root.rglob("*.avsc"))

    for p in explicit_paths:
        ep = Path(p)
        if ep.exists() and ep.suffix in (".avsc", ".avro"):
            schema_paths.append(ep)

    schemas = []
    for sp in schema_paths:
        try:
            schema = _parse_avsc_file(sp)
            if schema:
                schemas.append(schema)
        except Exception as exc:
            logger.warning("avro.schema_error", file=str(sp), error=str(exc))

    return schemas


def _parse_avsc_file(path: Path) -> Optional[dict]:
    """Parse an Avro schema (.avsc) file and convert fields to Spark types."""
    # Always load raw JSON first — works without fastavro and gives
    # a consistent dict regardless of fastavro's return type.
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None

    # fastavro resolves $ref and named types — use it if available,
    # but fall back to raw JSON gracefully.
    parsed = raw
    try:
        import fastavro.schema as fschema
        resolved = fschema.load_schema(str(path))
        # load_schema may return the schema dict or a parsed schema object
        if isinstance(resolved, dict):
            parsed = resolved
    except Exception:
        pass  # use raw JSON

    name = parsed.get("name") if isinstance(parsed, dict) else str(path.stem)
    raw_fields = parsed.get("fields", []) if isinstance(parsed, dict) else []

    fields = []
    for f in raw_fields:
        avro_type = f.get("type", "string")
        if isinstance(avro_type, list):
            # union ["null", "string"] — pick non-null
            avro_type = next((t for t in avro_type if t != "null"), "string")
        if isinstance(avro_type, dict):
            avro_type = avro_type.get("type", "string")
        spark_type = AVRO_TO_SPARK.get(str(avro_type), "StringType")
        fields.append({
            "name":       f.get("name", "field"),
            "java_type":  str(avro_type),
            "spark_type": spark_type,
        })

    if not fields:
        return None

    logger.info("avro.schema_loaded", name=name, fields=len(fields))
    return {"source_class": name, "fields": fields, "format": "avro"}
