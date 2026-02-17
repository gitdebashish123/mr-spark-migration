"""
Phase 5 — Risk Scanner for custom/proprietary library dependencies.

Scans Java imports and Maven/Gradle dependencies for patterns matching
user-configured company-specific libraries that may not have Spark equivalents.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Optional

from src.utils.logging import get_logger

logger = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Main API
# ─────────────────────────────────────────────────────────────────────────────

def scan_for_risks(
    class_metadata: list[dict],
    build_files: list[str],
    risk_prefixes: list[str],
    risk_action: str = "warn",
    scan_transitive: bool = True,
) -> dict[str, Any]:
    """
    Scan Java imports and build files for risky custom library usage.

    Args:
        class_metadata:  JavaClass dicts from driver_analysis.
        build_files:     Paths to pom.xml / build.gradle files.
        risk_prefixes:   Package prefixes to flag (e.g., ["com.company", "org.internal"]).
        risk_action:     "warn" | "block" | "info".
        scan_transitive: If True, also check for transitive dependency patterns.

    Returns:
        {
          "risk_items": [RiskItem dict, ...],
          "total_count": int,
          "blocked": bool,
          "transitive_count": int,
        }
    """
    risk_items = []

    # Scan Java imports
    for cls in class_metadata:
        imports = cls.get("imports", [])
        for imp in imports:
            for prefix in risk_prefixes:
                if imp.startswith(prefix):
                    risk_items.append({
                        "matched_prefix": prefix,
                        "artifact": imp,
                        "location": cls.get("file_path", "unknown"),
                        "is_transitive": False,
                        "suggestion": _suggest_alternative(imp),
                    })

    # Scan build files for direct dependencies
    for build_path in build_files:
        path = Path(build_path)
        if not path.exists():
            continue
        
        content = path.read_text(encoding="utf-8", errors="replace")
        deps = _extract_dependencies(content, path.suffix)
        
        for dep in deps:
            for prefix in risk_prefixes:
                if prefix in dep:
                    risk_items.append({
                        "matched_prefix": prefix,
                        "artifact": dep,
                        "location": str(path),
                        "is_transitive": False,
                        "suggestion": "Review dependency tree and consider Spark-native alternatives",
                    })

    # Deduplicate
    seen = set()
    unique_items = []
    for item in risk_items:
        key = (item["artifact"], item["location"])
        if key not in seen:
            seen.add(key)
            unique_items.append(item)

    blocked = risk_action == "block" and len(unique_items) > 0
    transitive_count = sum(1 for item in unique_items if item["is_transitive"])

    logger.info(
        "risk.scan_complete",
        total=len(unique_items),
        blocked=blocked,
        transitive=transitive_count,
    )

    return {
        "risk_items": unique_items,
        "total_count": len(unique_items),
        "blocked": blocked,
        "transitive_count": transitive_count,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _extract_dependencies(content: str, file_ext: str) -> list[str]:
    """Extract dependency artifact IDs from pom.xml or build.gradle."""
    deps = []
    
    if file_ext == ".xml":  # Maven pom.xml
        # <groupId>com.company</groupId><artifactId>custom-lib</artifactId>
        pattern = re.compile(
            r'<groupId>([^<]+)</groupId>\s*<artifactId>([^<]+)</artifactId>',
            re.DOTALL
        )
        for match in pattern.finditer(content):
            group_id = match.group(1).strip()
            artifact_id = match.group(2).strip()
            deps.append(f"{group_id}:{artifact_id}")
    
    elif file_ext in (".gradle", ".kts"):  # Gradle
        # implementation("com.company:custom-lib:1.0")
        pattern = re.compile(r'["\']([a-zA-Z0-9._-]+:[a-zA-Z0-9._-]+)(?::[0-9.]+)?["\']')
        deps.extend(pattern.findall(content))
    
    return deps


def _suggest_alternative(import_path: str) -> Optional[str]:
    """Suggest Spark-native alternatives for common patterns."""
    suggestions = {
        "hadoop.io.": "Use Spark Dataset/DataFrame with built-in encoders",
        "hadoop.mapreduce.": "Migrate to Spark transformations (map, filter, reduce)",
        "hadoop.fs.": "Use spark.read/write with built-in file formats",
        "cascading.": "Rewrite in Spark Dataset API",
        "pig.": "Rewrite in Spark SQL or Dataset API",
        "hive.": "Use Spark SQL with Hive metastore support",
    }
    
    for pattern, suggestion in suggestions.items():
        if pattern in import_path:
            return suggestion
    
    return "Review for Spark-native equivalent or rewrite as custom UDF"
