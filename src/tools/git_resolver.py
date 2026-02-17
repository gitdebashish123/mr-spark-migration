"""
Phase 2 — Git / Local source resolver.

Handles:
  - Git clone (HTTPS + SSH, PAT token, sparse-checkout for subdir)
  - Local path validation
  - File system scanning with glob + build-dir exclusion
"""
from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path
from typing import Optional

from src.utils.logging import get_logger

logger = get_logger(__name__)

# Build output dirs to skip when scanning
_SKIP_DIRS = frozenset({"target", "build", ".gradle", ".mvn", "out", "__pycache__", ".git"})


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def resolve_git_source(
    repo_url: str,
    branch: str,
    run_id: str,
    token: Optional[str] = None,
    subdir: Optional[str] = None,
    commit_tag: Optional[str] = None,
) -> str:
    """
    Clone a git repository and return the local path.

    Args:
        repo_url:   HTTPS or SSH git URL.
        branch:     Branch name to checkout.
        run_id:     Unique pipeline run ID (used for temp dir naming).
        token:      Optional PAT/token for private HTTPS repos.
        subdir:     If set, return path to this subdirectory within the clone.
        commit_tag: If set, checkout this specific commit or tag after clone.

    Returns:
        Absolute path to the cloned (and possibly subdir-scoped) directory.
    """
    try:
        import git as gitpython
    except ImportError:
        raise RuntimeError("gitpython is not installed. Run: uv add gitpython")

    clone_dir = Path(tempfile.gettempdir()) / "mr_migration" / run_id / "repo"
    clone_dir.mkdir(parents=True, exist_ok=True)

    # Inject token into HTTPS URL if provided
    effective_url = _inject_token(repo_url, token)

    logger.info("git.clone.start", url=repo_url, branch=branch, target=str(clone_dir))

    try:
        repo = gitpython.Repo.clone_from(
            effective_url,
            str(clone_dir),
            branch=branch,
            depth=1,           # shallow clone — we only need source, not history
            single_branch=True,
        )

        if commit_tag:
            logger.info("git.checkout", ref=commit_tag)
            repo.git.checkout(commit_tag)

    except gitpython.exc.GitCommandError as e:
        raise RuntimeError(f"Git clone failed for {repo_url}@{branch}: {e}") from e

    resolved = clone_dir / subdir if subdir else clone_dir
    if not resolved.exists():
        raise RuntimeError(
            f"Subdir '{subdir}' not found in cloned repo. "
            f"Available: {[p.name for p in clone_dir.iterdir() if p.is_dir()]}"
        )

    logger.info("git.clone.done", path=str(resolved))
    return str(resolved)


def resolve_local_source(
    local_path: str,
    subdir: Optional[str] = None,
) -> str:
    """
    Validate a local directory path and return it (with optional subdir).

    Args:
        local_path: Absolute path to the project root.
        subdir:     Optional subdirectory within root.

    Returns:
        Validated absolute path string.
    """
    root = Path(local_path)
    if not root.exists():
        raise RuntimeError(f"Local path does not exist: {local_path}")
    if not root.is_dir():
        raise RuntimeError(f"Local path is not a directory: {local_path}")

    resolved = root / subdir if subdir else root
    if not resolved.exists():
        raise RuntimeError(f"Subdir '{subdir}' not found under {local_path}")

    logger.info("local.resolved", path=str(resolved))
    return str(resolved)


def scan_directory(
    source_path: str,
    pattern: str = "**/*.java",
    skip_build_dirs: bool = True,
) -> dict:
    """
    Walk a directory and collect Java source files + build descriptors.

    Args:
        source_path:     Root directory to scan.
        pattern:         Glob pattern for source files.
        skip_build_dirs: Skip target/, build/, .gradle/ etc.

    Returns:
        {
          "files": [{"path": str, "abs_path": str, "size_bytes": int}],
          "total_count": int,
          "java_count": int,
          "build_files": [str],
          "root": str
        }
    """
    root = Path(source_path)
    if not root.exists():
        raise RuntimeError(f"Source path does not exist: {source_path}")

    all_files = []
    build_files = []

    for path in root.rglob("*"):
        if not path.is_file():
            continue

        # Skip build output dirs
        if skip_build_dirs and _is_in_skip_dir(path, root):
            continue

        rel = path.relative_to(root)
        entry = {
            "path": str(rel),
            "abs_path": str(path),
            "size_bytes": path.stat().st_size,
        }

        if path.suffix == ".java":
            all_files.append(entry)
        elif path.name in {"pom.xml", "build.gradle", "build.gradle.kts", "settings.gradle"}:
            build_files.append(str(rel))

    logger.info(
        "scan.complete",
        source=source_path,
        java_count=len(all_files),
        build_files=len(build_files),
    )

    return {
        "files": all_files,
        "total_count": len(all_files) + len(build_files),
        "java_count": len(all_files),
        "build_files": build_files,
        "root": source_path,
    }


def cleanup_clone(run_id: str) -> None:
    """Remove a cloned repo temp directory after the pipeline completes."""
    clone_dir = Path(tempfile.gettempdir()) / "mr_migration" / run_id
    if clone_dir.exists():
        shutil.rmtree(clone_dir, ignore_errors=True)
        logger.info("git.cleanup", run_id=run_id)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _inject_token(url: str, token: Optional[str]) -> str:
    """Embed a PAT token into an HTTPS URL for authentication."""
    if not token or not url.startswith("https://"):
        return url
    # https://token@github.com/org/repo.git
    without_scheme = url[len("https://"):]
    return f"https://{token}@{without_scheme}"


def _is_in_skip_dir(path: Path, root: Path) -> bool:
    """Return True if any path component is in the skip list."""
    try:
        rel = path.relative_to(root)
        return any(part in _SKIP_DIRS for part in rel.parts)
    except ValueError:
        return False
