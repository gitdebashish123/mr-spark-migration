#!/usr/bin/env python3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.tools.java_analyzer import analyze_files
import json

source_root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("/tmp/demo")
java_files = [str(f.relative_to(source_root)) for f in source_root.rglob("*.java")]

result = analyze_files(java_files, str(source_root))
print(json.dumps(result, indent=2))