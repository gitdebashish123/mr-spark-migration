"""
Phase 2 — Java AST Analyzer using javalang.

Parses .java source files and classifies each class by its MapReduce role
by inspecting:
  - extends clauses (Mapper, Reducer, Combiner, Partitioner)
  - implements clauses (WritableComparable → composite key)
  - import statements (hadoop mapreduce imports confirm role)
  - class name heuristics as a fallback
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

from src.utils.logging import get_logger

logger = get_logger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Role detection rules
# ─────────────────────────────────────────────────────────────────────────────

# Ordered — first match wins.
# IMPORTANT: combiner MUST be checked before reducer.
# Hadoop combiners always extend Reducer<K,V,K,V> and never a dedicated
# Combiner base class, so the extends clause alone is ambiguous.
# We use the class name as the primary combiner signal.
_ROLE_RULES: list[tuple[str, list[str]]] = [
    ("mapper",        ["Mapper<", "extends Mapper"]),
    ("combiner",      ["Combiner<", "extends Combiner"]),   # before reducer
    ("reducer",       ["Reducer<", "extends Reducer"]),
    ("partitioner",   ["Partitioner<", "extends Partitioner", "HashPartitioner"]),
    ("composite_key", ["WritableComparable", "implements WritableComparable"]),
]

_DRIVER_IMPORTS = {
    "org.apache.hadoop.mapreduce.Job",
    "org.apache.hadoop.mapred.JobConf",
    "org.apache.hadoop.mapreduce.lib.input.FileInputFormat",
}

_DRIVER_NAME_PATTERNS = re.compile(
    r"(driver|main|runner|app|launcher|entry)", re.IGNORECASE
)

# Combiner name heuristic — checked BEFORE the extends-clause rules
_COMBINER_NAME_RE = re.compile(r"combiner", re.IGNORECASE)


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def analyze_files(
    file_paths: list[str],
    source_root: str,
) -> dict:
    """
    Parse a list of .java files and classify each class by MR role.

    Args:
        file_paths:  List of absolute or relative paths to .java files.
        source_root: Root directory (used to resolve relative paths).

    Returns:
        {
          "classes": [JavaClass dict, ...],
          "drivers": int, "mappers": int, "reducers": int,
          "combiners": int, "partitioners": int,
          "composite_keys": int, "unclassified": int,
          "parse_errors": [{"file": str, "error": str}]
        }
    """
    try:
        import javalang
    except ImportError:
        raise RuntimeError("javalang is not installed. Run: uv add javalang")

    root = Path(source_root)
    classes = []
    parse_errors = []
    counters = {
        "drivers": 0, "mappers": 0, "reducers": 0,
        "combiners": 0, "partitioners": 0,
        "composite_keys": 0, "unclassified": 0,
    }

    for file_path in file_paths:
        path = Path(file_path) if Path(file_path).is_absolute() else root / file_path
        if not path.exists() or path.suffix != ".java":
            continue

        try:
            source = path.read_text(encoding="utf-8", errors="replace")
            parsed = _parse_java_file(source, str(path.relative_to(root)), javalang)
            for cls in parsed:
                role = cls.get("role", "unclassified")
                key = f"{role}s" if role != "composite_key" else "composite_keys"
                if key in counters:
                    counters[key] += 1
                else:
                    counters["unclassified"] += 1
                classes.append(cls)
        except Exception as exc:
            rel = str(path.relative_to(root)) if path.is_relative_to(root) else str(path)
            parse_errors.append({"file": rel, "error": str(exc)})
            logger.warning("java.parse_error", file=rel, error=str(exc))

    logger.info(
        "java.analysis_complete",
        total=len(classes),
        **counters,
        parse_errors=len(parse_errors),
    )

    return {"classes": classes, "parse_errors": parse_errors, **counters}


# ─────────────────────────────────────────────────────────────────────────────
# Internal parsing
# ─────────────────────────────────────────────────────────────────────────────

def _parse_java_file(source: str, rel_path: str, javalang) -> list[dict]:
    """Parse a single .java source file and return a list of class dicts."""
    try:
        tree = javalang.parse.parse(source)
    except Exception:
        # javalang can fail on generics / newer Java syntax — fall back to regex
        return _regex_fallback(source, rel_path)

    package = tree.package.name if tree.package else None
    imports = [imp.path for imp in (tree.imports or [])]
    classes = []

    for _, node in tree.filter(javalang.tree.ClassDeclaration):
        extends_str  = _resolve_extends(node)
        implements   = _resolve_implements(node)
        role         = _detect_role(node.name, extends_str, implements, imports, source)

        classes.append({
            "class_name":  node.name,
            "file_path":   rel_path,
            "package":     package,
            "role":        role,
            "imports":     imports,
            "extends":     extends_str,
            "implements":  implements,
            "raw_source":  None,   # omit raw source from state to keep it lean
        })

    return classes


def _resolve_extends(node) -> Optional[str]:
    """Extract the extends clause as a string."""
    if node.extends is None:
        return None
    ext = node.extends
    name = ext.name if hasattr(ext, "name") else str(ext)
    # Include type args if present (e.g. Mapper<LongWritable, Text, Text, IntWritable>)
    if hasattr(ext, "arguments") and ext.arguments:
        args = ", ".join(_type_arg_str(a) for a in ext.arguments)
        return f"{name}<{args}>"
    return name


def _resolve_implements(node) -> list[str]:
    """Extract implements clauses as a list of strings."""
    if not node.implements:
        return []
    result = []
    for iface in node.implements:
        name = iface.name if hasattr(iface, "name") else str(iface)
        if hasattr(iface, "arguments") and iface.arguments:
            args = ", ".join(_type_arg_str(a) for a in iface.arguments)
            result.append(f"{name}<{args}>")
        else:
            result.append(name)
    return result


def _type_arg_str(arg) -> str:
    """Stringify a type argument node."""
    if hasattr(arg, "name"):
        return arg.name
    if hasattr(arg, "pattern_type"):
        return "?"
    return str(arg)


def _detect_role(
    class_name: str,
    extends_str: Optional[str],
    implements: list[str],
    imports: list[str],
    source: str,
) -> str:
    """
    Determine the MR role of a class using a priority-ordered rule set:
    1. extends/implements clause (most reliable)
    2. import-based heuristics
    3. class name patterns (least reliable — last resort)
    """
    # Combiner name check FIRST — combiners extend Reducer in Hadoop,
    # so the extends clause alone cannot distinguish them from reducers.
    # A class named *Combiner* that extends Reducer is always a combiner.
    if _COMBINER_NAME_RE.search(class_name) and (
        extends_str and "Reducer" in extends_str
    ):
        return "combiner"

    combined = " ".join(filter(None, [extends_str or ""] + implements))

    for role, patterns in _ROLE_RULES:
        if any(p in combined for p in patterns):
            return role

    # Import-based driver detection
    if any(imp in _DRIVER_IMPORTS for imp in imports):
        return "driver"

    # Source-body heuristics (e.g. static void main)
    if "public static void main" in source and "Job" in source:
        return "driver"

    # Name-based fallback
    if _DRIVER_NAME_PATTERNS.search(class_name):
        return "driver"

    return "unclassified"


def _regex_fallback(source: str, rel_path: str) -> list[dict]:
    """
    Minimal regex-based parser for files javalang cannot parse
    (e.g. files using newer Java syntax like records, sealed classes).
    Extracts class name, package, imports, and guesses role.
    """
    logger.debug("java.regex_fallback", file=rel_path)

    package_match = re.search(r"^package\s+([\w.]+)\s*;", source, re.MULTILINE)
    package = package_match.group(1) if package_match else None

    imports = re.findall(r"^import\s+([\w.*]+)\s*;", source, re.MULTILINE)

    class_matches = re.findall(
        r"(?:public\s+)?(?:abstract\s+)?class\s+(\w+)"
        r"(?:\s+extends\s+([\w<>, ]+?))?(?:\s+implements\s+([\w<>, ]+?))?\s*\{",
        source,
    )

    classes = []
    for class_name, extends_raw, implements_raw in class_matches:
        extends_str  = extends_raw.strip()  if extends_raw  else None
        implements   = [i.strip() for i in implements_raw.split(",")] if implements_raw else []
        role         = _detect_role(class_name, extends_str, implements, imports, source)
        classes.append({
            "class_name":  class_name,
            "file_path":   rel_path,
            "package":     package,
            "role":        role,
            "imports":     imports,
            "extends":     extends_str,
            "implements":  implements,
            "raw_source":  None,
        })
    return classes
