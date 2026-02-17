"""
Phase 4 — Project Scaffolding.

Writes the generated Spark/Scala files to disk along with a complete build
descriptor (pom.xml or build.gradle.kts) with all necessary dependencies.
"""
from __future__ import annotations

import textwrap
from pathlib import Path
from typing import Any

from src.utils.logging import get_logger

logger = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Main API
# ─────────────────────────────────────────────────────────────────────────────

def scaffold_spark_project(
    generated_files: dict[str, str],
    build_tool: str,
    versions: dict,
    output_dir: str,
) -> dict[str, Any]:
    """
    Write all Scala files and build descriptor to disk.

    Args:
        generated_files: {relative_path: source_content}
        build_tool:      "maven" | "gradle"
        versions:        {spark, scala, jdk, hadoop}
        output_dir:      Base directory (files written under output_dir/project/)

    Returns:
        {"project_path": str, "files_written": int, "build_file": str}
    """
    output_root = Path(output_dir)
    project_path = output_root / "spark-migration-output"
    project_path.mkdir(parents=True, exist_ok=True)

    files_written = 0

    # Write Scala source files
    for rel_path, content in generated_files.items():
        file_path = project_path / rel_path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(content, encoding="utf-8")
        files_written += 1
        logger.debug("scaffold.file_written", path=rel_path)

    # Write build descriptor
    if build_tool == "maven":
        build_file = "pom.xml"
        _write_pom_xml(project_path, versions)
    elif build_tool == "gradle":
        build_file = "build.gradle.kts"
        _write_gradle_build(project_path, versions)
    else:
        raise ValueError(f"Unknown build tool: {build_tool}")

    files_written += 1

    logger.info(
        "scaffold.complete",
        project_path=str(project_path),
        files_written=files_written,
    )

    return {
        "project_path": str(project_path),
        "files_written": files_written,
        "build_file": build_file,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Maven pom.xml
# ─────────────────────────────────────────────────────────────────────────────

def _write_pom_xml(project_path: Path, versions: dict) -> None:
    """Generate a production-ready pom.xml with all Spark dependencies."""
    spark = versions.get("spark", "4.0.1")
    scala_full = versions.get("scala", "2.13.16")
    scala_compat = ".".join(scala_full.split(".")[:2])  # 2.13.16 → 2.13
    jdk = versions.get("jdk", "17")
    hadoop = versions.get("hadoop", "3.4.2")

    pom = textwrap.dedent(f"""\
        <?xml version="1.0" encoding="UTF-8"?>
        <project xmlns="http://maven.apache.org/POM/4.0.0"
                 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                 xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                                     http://maven.apache.org/xsd/maven-4.0.0.xsd">
            <modelVersion>4.0.0</modelVersion>

            <groupId>com.example</groupId>
            <artifactId>spark-migration</artifactId>
            <version>1.0.0-SNAPSHOT</version>
            <packaging>jar</packaging>

            <properties>
                <maven.compiler.source>{jdk}</maven.compiler.source>
                <maven.compiler.target>{jdk}</maven.compiler.target>
                <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
                <spark.version>{spark}</spark.version>
                <scala.version>{scala_full}</scala.version>
                <scala.compat.version>{scala_compat}</scala.compat.version>
                <hadoop.version>{hadoop}</hadoop.version>
            </properties>

            <dependencies>
                <!-- Spark SQL -->
                <dependency>
                    <groupId>org.apache.spark</groupId>
                    <artifactId>spark-sql_${{scala.compat.version}}</artifactId>
                    <version>${{spark.version}}</version>
                    <scope>provided</scope>
                </dependency>

                <!-- Hadoop Client (for HDFS/S3 access) -->
                <dependency>
                    <groupId>org.apache.hadoop</groupId>
                    <artifactId>hadoop-client</artifactId>
                    <version>${{hadoop.version}}</version>
                    <scope>provided</scope>
                </dependency>

                <!-- Scala Library -->
                <dependency>
                    <groupId>org.scala-lang</groupId>
                    <artifactId>scala-library</artifactId>
                    <version>${{scala.version}}</version>
                </dependency>

                <!-- ScalaTest -->
                <dependency>
                    <groupId>org.scalatest</groupId>
                    <artifactId>scalatest_${{scala.compat.version}}</artifactId>
                    <version>3.2.15</version>
                    <scope>test</scope>
                </dependency>
            </dependencies>

            <build>
                <sourceDirectory>src/main/scala</sourceDirectory>
                <testSourceDirectory>src/test/scala</testSourceDirectory>

                <plugins>
                    <!-- Scala Maven Plugin -->
                    <plugin>
                        <groupId>net.alchim31.maven</groupId>
                        <artifactId>scala-maven-plugin</artifactId>
                        <version>4.8.1</version>
                        <executions>
                            <execution>
                                <goals>
                                    <goal>compile</goal>
                                    <goal>testCompile</goal>
                                </goals>
                            </execution>
                        </executions>
                        <configuration>
                            <scalaVersion>${{scala.version}}</scalaVersion>
                            <args>
                                <arg>-deprecation</arg>
                                <arg>-feature</arg>
                            </args>
                        </configuration>
                    </plugin>

                    <!-- Shade Plugin for fat JAR -->
                    <plugin>
                        <groupId>org.apache.maven.plugins</groupId>
                        <artifactId>maven-shade-plugin</artifactId>
                        <version>3.5.0</version>
                        <executions>
                            <execution>
                                <phase>package</phase>
                                <goals>
                                    <goal>shade</goal>
                                </goals>
                                <configuration>
                                    <transformers>
                                        <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                            <mainClass>com.example.spark.driver.Main</mainClass>
                                        </transformer>
                                    </transformers>
                                </configuration>
                            </execution>
                        </executions>
                    </plugin>
                </plugins>
            </build>
        </project>
    """)

    (project_path / "pom.xml").write_text(pom, encoding="utf-8")


# ─────────────────────────────────────────────────────────────────────────────
# Gradle build.gradle.kts
# ─────────────────────────────────────────────────────────────────────────────

def _write_gradle_build(project_path: Path, versions: dict) -> None:
    """Generate a production-ready build.gradle.kts."""
    spark = versions.get("spark", "4.0.1")
    scala_full = versions.get("scala", "2.13.16")
    scala_compat = ".".join(scala_full.split(".")[:2])
    jdk = versions.get("jdk", "17")
    hadoop = versions.get("hadoop", "3.4.2")

    gradle = textwrap.dedent(f"""\
        plugins {{
            scala
            application
        }}

        group = "com.example"
        version = "1.0.0-SNAPSHOT"

        repositories {{
            mavenCentral()
        }}

        dependencies {{
            implementation("org.scala-lang:scala-library:{scala_full}")
            compileOnly("org.apache.spark:spark-sql_{scala_compat}:{spark}")
            compileOnly("org.apache.hadoop:hadoop-client:{hadoop}")
            testImplementation("org.scalatest:scalatest_{scala_compat}:3.2.15")
        }}

        java {{
            toolchain {{
                languageVersion.set(JavaLanguageVersion.of({jdk}))
            }}
        }}

        application {{
            mainClass.set("com.example.spark.driver.Main")
        }}
    """)

    (project_path / "build.gradle.kts").write_text(gradle, encoding="utf-8")
