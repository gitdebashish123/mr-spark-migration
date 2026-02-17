# MapReduce → Spark/Scala Migration Pipeline

**Automated migration of Hadoop MapReduce jobs to Apache Spark using LLM-assisted code generation.**

Complete production-ready implementation with static analysis, LLM code generation, compile/test/patch loop, risk scanning, and comprehensive reporting.

---

## Features

✅ **Source Analysis** (Phase 2)
- Git clone with authentication (HTTPS + SSH, PAT tokens)
- Java AST parsing via `javalang` with combiner/reducer/mapper role detection
- Schema extraction from WritableComparable fields + Avro `.avsc` files
- Automatic Writable → Spark type mapping

✅ **Job Graph Inference** (Phase 3)
- LLM-powered job graph construction from class metadata
- Multi-job pipeline detection with chaining support
- Provider routing: Claude Sonnet 4 or Gemini 2.0 Flash

✅ **Spark Code Generation** (Phase 3)
- Three-layer architecture (Driver / Job / Service)
- Dataset/DataFrame API (configurable RDD fallback)
- Production-quality Scala with ScalaDoc comments
- Robust multi-strategy JSON parsing (handles unescaped quotes)

✅ **Quality Gate** (Phase 4)
- Real Maven/Gradle compilation via subprocess
- LLM-assisted patching on compile errors (up to 3 retries)
- Circuit breaker prevents infinite loops
- Test execution and result validation

✅ **Risk Assessment** (Phase 5)
- Custom library dependency scanning
- Import pattern matching (company-specific prefixes)
- Maven/Gradle dependency extraction
- Spark-native alternative suggestions

✅ **Final Packaging** (Phase 5)
- JAR assembly via `mvn package` or `gradle build`
- Comprehensive markdown migration report
- Human review escalation on failures

---

## UV command
```bash
cd <project-dir>
uv venv --python 3.12
source .venv/bin/activate
uv sync 
sh scripts/run_with_tracing.sh ( with tracing)
uv run streamlit run ui/app.py ( without tracing)

python3 scripts/demo_analyzer.py <local-code-dir>
```

## Quick Start

```bash
# 1. Setup (auto-installs uv + Python 3.12 if needed)
bash scripts/setup.sh

# 2. Configure API key
cp .env.example .env
# Edit .env and add:
#   ANTHROPIC_API_KEY=sk-ant-...
# OR
#   GOOGLE_API_KEY=AIza...

# 3. Launch UI
uv run streamlit run ui/app.py
```

Then open **http://localhost:8501** and:
1. Enter your MapReduce git repo URL (or local path)
2. Configure versions (Spark 4.0, Scala 2.13, etc.)
3. Select LLM provider (Claude or Gemini)
4. Click **Launch Migration**

---

## CLI Usage (Programmatic)

```python
from src.agents.graph import pipeline_graph
from src.models.state import PipelineConfig, SourceConfig, SourceMode

config = PipelineConfig(
    source=SourceConfig(
        mode=SourceMode.GIT,
        repo_url="https://github.com/apache/hadoop",
        branch="trunk",
        subdir="hadoop-mapreduce-project/hadoop-mapreduce-examples",
    ),
)

state = pipeline_graph.invoke({"config": config})

# Access results
print(state["migration_report"])
print(state["generated_files"])  # {path: scala_source}
print(state["artifact_path"])    # path to compiled JAR
```

---

## Architecture

### Pipeline Nodes (LangGraph)

```
config_validate → source_resolve → ⎧ driver_analysis ⎫ → jobgraph_infer → 
                                    ⎩ schema_extract  ⎭                     
                                    
diagram_render (parallel) → spark_codegen → risk_scan → project_scaffold →

compile_check → ⎧ test_check → package_export → END
                ⎩ critique_and_patch (retry loop, max 3) → compile_check
```

### Three-Layer Scala Architecture

Generated code follows strict separation of concerns:

- **Driver** (`*App.scala`) — Entry point, SparkSession setup, no business logic
- **Job** (`*Job.scala`) — Orchestration, calls services, no transformations
- **Service** (`*Service.scala`) — Pure transformation logic (DataFrame → DataFrame)

Example:
```scala
// Driver
object WordCountApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    WordCountJob.run(spark, args)
  }
}

// Job
object WordCountJob {
  def run(spark: SparkSession, args: Array[String]): Unit = {
    val input = spark.read.text(args(0))
    val result = WordCountService.count(spark, input)
    result.write.text(args(1))
  }
}

// Service
object WordCountService {
  def count(spark: SparkSession, input: DataFrame): DataFrame = {
    import spark.implicits._
    input.flatMap(_.getString(0).split("\\s+"))
         .groupBy($"value").count()
  }
}
```

---

## Configuration

### Source Options

**Git Mode:**
```python
SourceConfig(
    mode=SourceMode.GIT,
    repo_url="https://github.com/org/repo",
    branch="main",
    git_token="ghp_...",        # optional PAT for private repos
    subdir="mr-jobs",            # optional subdirectory
    commit_tag="v1.0.0",         # optional specific commit/tag
)
```

**Local Mode:**
```python
SourceConfig(
    mode=SourceMode.LOCAL,
    local_path="/Users/me/mr-project",
    local_pattern="**/*.java",
    skip_build_dirs=True,
)
```

### Build Tools

- **Maven** — generates `pom.xml` with scala-maven-plugin + maven-shade-plugin
- **Gradle** — generates `build.gradle.kts` with Scala plugin

Both include:
- Spark SQL (provided scope)
- Hadoop client (provided scope)
- Scala library + ScalaTest

### LLM Providers

**Claude (Anthropic):**
```python
llm_provider=LLMProvider.CLAUDE,
model_name="claude-sonnet-4-20250514",
```

**Gemini (Google):**
```python
llm_provider=LLMProvider.GEMINI,
model_name="gemini-2.0-flash-exp",
```

### Risk Scanning

```python
RiskConfig(
    prefixes=["com.company", "org.internal"],
    action=RiskAction.WARN,  # or BLOCK
    transitive_dep_scan=True,
)
```

---

## Testing

```bash
# All tests (Phase 1-5, ~100 tests, all mocked, no API keys required)
uv run pytest tests/ -v

# Specific phase
uv run pytest tests/test_phase2.py -v
uv run pytest tests/test_phase3.py -v

# With coverage
uv run pytest tests/ --cov=src --cov-report=html
```

**Test Structure:**
- `test_phase1.py` — Graph smoke tests (14 tests)
- `test_phase2.py` — Real static analysis with fixtures (28 tests)
- `test_phase3.py` — LLM codegen with mocked calls (27 tests)
- `test_phase4.py` — Build/test/patch with mocked subprocess (21 tests)
- `test_phase5.py` — Risk scanning + report generation (20 tests)

---

## Output Files

After a successful run:

```
/tmp/mr_migration/{run_id}/
├── spark-migration-output/
│   ├── src/main/scala/com/example/spark/
│   │   ├── driver/
│   │   │   └── WordCountApp.scala
│   │   ├── jobs/
│   │   │   └── WordCountJob.scala
│   │   └── service/
│   │       └── WordCountService.scala
│   ├── pom.xml  (or build.gradle.kts)
│   └── target/
│       └── spark-migration-1.0.0-SNAPSHOT.jar
└── migration_report.md
```

---

## Troubleshooting

### "LLM JSON parse error"
The pipeline uses a 4-strategy fallback parser that handles:
- Direct JSON parsing
- Markdown fence extraction
- Bracket-bounded extraction
- Deep cleaning (control chars, backslashes)

If all strategies fail, check the logs for the raw LLM response preview.

### Compilation Failures
The pipeline automatically retries with LLM patching (up to 3 attempts). If it still fails:
1. Check `compile_results[-1].error_lines` in state
2. Review `patched_files` to see what the LLM tried
3. Manually fix and re-run, or increase `MAX_PATCH_RETRIES`

### Missing Dependencies
```bash
# Maven/Gradle not installed?
brew install maven gradle  # macOS
apt install maven gradle   # Ubuntu

# Python dependencies?
uv sync  # re-syncs from pyproject.toml
```

### Git Clone Failures
- Private repo? Add `git_token` (GitHub PAT)
- SSH URL? Ensure SSH keys are configured
- Subdir not found? Check `subdir` path is correct

---

## Project Structure

```
mr_spark_migration/
├── src/
│   ├── agents/
│   │   ├── graph.py           # LangGraph pipeline definition
│   │   └── nodes.py           # 14 node implementations
│   ├── models/
│   │   └── state.py           # TypedDicts + Pydantic models
│   ├── tools/
│   │   ├── base_tools.py      # FastMCP tool server
│   │   ├── git_resolver.py    # Git clone + directory scanning
│   │   ├── java_analyzer.py   # AST parsing + role detection
│   │   ├── schema_extractor.py # Writable/Avro → Spark types
│   │   ├── llm_codegen.py     # Job graph + code generation
│   │   ├── build_runner.py    # Maven/Gradle compile/test
│   │   ├── project_scaffold.py # Write files + build descriptor
│   │   ├── risk_scanner.py    # Custom library scanning
│   │   └── report_generator.py # Markdown report
│   ├── utils/
│   │   ├── config.py          # Settings (API keys, env vars)
│   │   └── logging.py         # Structured JSON logging
│   └── ui/
│       └── app.py             # Streamlit dashboard
├── tests/
│   ├── fixtures/
│   │   └── wordcount/         # Sample MapReduce project
│   ├── test_phase1.py
│   ├── test_phase2.py
│   ├── test_phase3.py
│   ├── test_phase4.py
│   └── test_phase5.py
├── scripts/
│   └── setup.sh               # One-shot setup
├── pyproject.toml
├── .env.example
└── README.md
```

---

## Dependencies

**Runtime:**
- Python 3.12+
- Streamlit 1.28.1
- LangGraph 0.2.61
- FastMCP 0.2.5
- javalang, fastavro
- anthropic or google-generativeai

**Build Tools (optional, for compile/test):**
- Maven 3.8+ or Gradle 8.0+
- Java/Scala compiler

**Install:**
```bash
bash scripts/setup.sh  # auto-installs everything
```

---

## License

MIT

---

## Credits

Built with:
- [LangGraph](https://github.com/langchain-ai/langgraph) for pipeline orchestration
- [FastMCP](https://github.com/jlowin/fastmcp) for LLM tool server
- [Streamlit](https://streamlit.io) for UI
- Claude Sonnet 4 / Gemini 2.0 Flash for code generation

---

## LangSmith Tracing (Debugging LLM Calls)

To see detailed traces of all LLM calls (prompts, responses, token usage):

1. **Get API key** from [smith.langchain.com](https://smith.langchain.com)

2. **Add to `.env`:**
```bash
LANGCHAIN_TRACING_V2=true
LANGCHAIN_API_KEY=lsv2_pt_...
LANGCHAIN_PROJECT=mr-spark-migration
```

3. **Launch with tracing:**
```bash
bash scripts/run_with_tracing.sh
```

4. **View traces** at https://smith.langchain.com/o/{your-org}/projects/p/mr-spark-migration

**What you'll see:**
- Full prompt sent to LLM (system + user)
- LLM response (raw text before JSON parsing)
- Token counts and latency
- Parse errors and retry attempts
- Nested tool calls (if using MCP in Phase 3+)

**Sidebar indicator:**
- 🟢 "LangSmith tracing: Enabled" → traces are being sent
- 🔵 "LangSmith tracing: Disabled" → no tracing (click to see setup steps)

