# Astronomer Cosmos Architecture

This document provides a comprehensive overview of the Astronomer Cosmos architecture, design principles, and key implementation concepts.

**Version:** 1.12.x | **Airflow Compatibility:** 2.4+ and 3.x

## Table of Contents

1. [Overview](#overview)
2. [High-Level Architecture](#high-level-architecture)
3. [Core Components](#core-components)
4. [Design Patterns](#design-patterns)
5. [Configuration System](#configuration-system)
6. [Operator Architecture](#operator-architecture)
7. [Execution Modes](#execution-modes)
8. [Watcher Mode (New)](#watcher-mode)
9. [Async Execution Mode (New)](#async-execution-mode)
10. [dbt Integration](#dbt-integration)
11. [Profile Mapping System](#profile-mapping-system)
12. [Airflow 2 vs 3 Compatibility](#airflow-2-vs-3-compatibility)
13. [Telemetry & Observability](#telemetry--observability)
14. [Data Flow](#data-flow)
15. [Extension Points](#extension-points)

---

## Overview

**Astronomer Cosmos** is an Apache Airflow provider package that orchestrates dbt (data build tool) projects within Airflow. It transforms dbt workflows into native Airflow DAGs and task groups, enabling seamless integration between the two ecosystems.

### Key Capabilities

- Convert dbt projects to Airflow DAGs automatically
- Support 10 execution modes (Local, Docker, Kubernetes, Watcher, Async, etc.)
- Map Airflow connections to dbt profiles automatically (18+ databases)
- Integrate with Airflow's scheduling, monitoring, and alerting
- Provide lineage tracking via OpenLineage and Airflow Datasets
- Full compatibility with both Airflow 2.x and 3.x

---

## High-Level Architecture

```mermaid
graph TB
    subgraph "User Configuration"
        PC[ProjectConfig]
        PFC[ProfileConfig]
        EC[ExecutionConfig]
        RC[RenderConfig]
    end

    subgraph "Cosmos Core"
        DD[DbtDag / DbtTaskGroup]
        CONV[DbtToAirflowConverter]
        DG[DbtGraph]
        GB[Graph Builder]
    end

    subgraph "dbt Project"
        PROJ[dbt_project.yml]
        MODELS[models/]
        MANIFEST[manifest.json]
    end

    subgraph "Airflow Integration"
        DAG[Airflow DAG]
        TG[Task Groups]
        TASKS[Airflow Tasks]
    end

    subgraph "Execution Layer"
        LOCAL[Local]
        DOCKER[Docker]
        K8S[Kubernetes]
        WATCHER[Watcher]
        ASYNC[Async]
        CLOUD[Cloud Operators]
    end

    subgraph "Observability"
        LISTENERS[Event Listeners]
        TELEMETRY[Telemetry]
        LINEAGE[OpenLineage]
    end

    subgraph "Database Profiles"
        PM[Profile Mappings]
        CONN[Airflow Connections]
        PROFILES[profiles.yml]
    end

    PC --> DD
    PFC --> DD
    EC --> DD
    RC --> DD

    DD --> CONV
    CONV --> DG
    DG --> |Parse| PROJ
    DG --> |Parse| MANIFEST
    DG --> |Parse| MODELS

    CONV --> GB
    GB --> DAG
    GB --> TG
    GB --> TASKS

    TASKS --> LOCAL
    TASKS --> DOCKER
    TASKS --> K8S
    TASKS --> WATCHER
    TASKS --> ASYNC
    TASKS --> CLOUD

    PFC --> PM
    CONN --> PM
    PM --> PROFILES

    TASKS --> LISTENERS
    LISTENERS --> TELEMETRY
    TASKS --> LINEAGE
```

---

## Core Components

### Component Overview

```mermaid
graph LR
    subgraph "Entry Points"
        A1[DbtDag]
        A2[DbtTaskGroup]
        A3[Individual Operators]
    end

    subgraph "Conversion Layer"
        B1[DbtToAirflowConverter]
        B2[DbtGraph]
        B3[DbtNode]
    end

    subgraph "Operator Layer"
        C1[AbstractDbtBaseOperator]
        C2[Command Mixins]
        C3[Execution Operators]
    end

    subgraph "Profile Layer"
        D1[ProfileConfig]
        D2[BaseProfileMapping]
        D3[Database Mappings]
    end

    subgraph "Observability Layer"
        E1[TaskInstanceListener]
        E2[DagRunListener]
        E3[Telemetry]
    end

    A1 --> B1
    A2 --> B1
    B1 --> B2
    B2 --> B3
    B1 --> C1
    C2 --> C3
    C1 --> C3
    A3 --> C3
    D1 --> D2
    D2 --> D3
    C3 --> D3
    C3 --> E1
    E1 --> E3
    E2 --> E3
```

### Directory Structure

```
cosmos/
├── __init__.py                    # Public API exports
├── config.py                      # Configuration classes
├── constants.py                   # ExecutionMode, LoadMode, TestBehavior enums
├── converter.py                   # DbtToAirflowConverter
├── versioning.py                  # Content-based hashing for caching (NEW)
│
├── airflow/                       # High-level DAG/TaskGroup APIs
│   ├── dag.py                    # DbtDag class
│   ├── task_group.py             # DbtTaskGroup class
│   └── graph.py                  # Graph building logic
│
├── core/                          # Core abstractions
│   ├── airflow.py                # get_airflow_task() function
│   └── graph/
│       └── entities.py           # Task, Group, CosmosEntity models
│
├── dbt/                           # dbt integration
│   ├── graph.py                  # DbtNode, DbtGraph
│   ├── runner.py                 # dbtRunner wrapper (NEW)
│   ├── selector.py               # Node selection logic
│   ├── executable.py             # dbt executable detection
│   ├── project.py                # Project utilities
│   └── parser/                   # Manifest/output parsing
│
├── operators/                     # Execution operators (60+)
│   ├── base.py                   # AbstractDbtBaseOperator + Mixins
│   ├── local.py                  # Local execution (17 operators)
│   ├── docker.py                 # Docker execution
│   ├── kubernetes.py             # Kubernetes execution
│   ├── virtualenv.py             # Virtualenv execution
│   ├── watcher.py                # Watcher mode operators (NEW)
│   ├── airflow_async.py          # Async execution operators (NEW)
│   ├── aws_ecs.py                # AWS ECS operators (NEW)
│   ├── aws_eks.py                # AWS EKS operators
│   ├── azure_container_instance.py
│   ├── gcp_cloud_run_job.py
│   │
│   ├── _watcher/                 # Watcher internals (NEW)
│   │   ├── state.py              # AF2/AF3 state management
│   │   └── triggerer.py          # WatcherTrigger for deferrable sensors
│   │
│   └── _asynchronous/            # Async internals (NEW)
│       ├── base.py               # Factory operator
│       ├── bigquery.py           # BigQuery async operator
│       └── databricks.py         # Databricks async (stub)
│
├── profiles/                      # Database profile mappings (18+)
│   ├── base.py                   # BaseProfileMapping
│   ├── postgres/
│   ├── snowflake/
│   ├── bigquery/
│   ├── redshift/
│   ├── databricks/
│   ├── duckdb/                   # NEW
│   ├── mysql/                    # NEW
│   ├── sqlserver/                # NEW
│   ├── trino/
│   ├── spark/
│   ├── athena/
│   ├── oracle/
│   ├── teradata/
│   ├── vertica/
│   ├── clickhouse/
│   └── exasol/
│
├── plugin/                        # Airflow plugin
│   ├── __init__.py               # Plugin registration
│   ├── airflow2.py               # Flask-based UI (AF2)
│   ├── airflow3.py               # FastAPI-based UI (AF3) (NEW)
│   ├── storage.py                # Storage type detection
│   ├── snippets.py               # UI snippets
│   └── templates/
│
├── listeners/                     # Event listeners (NEW)
│   ├── dag_run_listener.py       # DAG run telemetry
│   └── task_instance_listener.py # Task instance telemetry (NEW)
│
├── hooks/                         # Airflow hooks
│   └── subprocess.py             # Subprocess execution hook
│
├── cache.py                       # Caching mechanisms
├── dataset.py                     # Airflow Dataset/Asset support
├── io.py                          # File I/O utilities
├── settings.py                    # Application settings
└── telemetry.py                   # Telemetry emission
```

---

## Design Patterns

### 1. Mixin Composition Pattern

The operator system uses mixin classes to compose command functionality with execution modes.

```mermaid
classDiagram
    class AbstractDbtBaseOperator {
        +build_cmd()
        +add_global_flags()
        +execute()
    }

    class DbtRunMixin {
        +base_cmd = ["run"]
        +ui_color = "#4C8EBD"
    }

    class DbtTestMixin {
        +base_cmd = ["test"]
        +ui_color = "#F0EAD6"
    }

    class DbtBuildMixin {
        +base_cmd = ["build"]
        +ui_color = "#8194E0"
    }

    class DbtLocalBaseOperator {
        +run_subprocess()
        +run_dbt_runner()
    }

    class DbtDockerBaseOperator {
        +build_and_run_cmd()
    }

    class DbtKubernetesBaseOperator {
        +build_and_run_cmd()
    }

    class DbtRunLocalOperator
    class DbtRunDockerOperator
    class DbtRunKubernetesOperator

    AbstractDbtBaseOperator <|-- DbtLocalBaseOperator
    AbstractDbtBaseOperator <|-- DbtDockerBaseOperator
    AbstractDbtBaseOperator <|-- DbtKubernetesBaseOperator

    DbtRunMixin <|-- DbtRunLocalOperator
    DbtLocalBaseOperator <|-- DbtRunLocalOperator

    DbtRunMixin <|-- DbtRunDockerOperator
    DbtDockerBaseOperator <|-- DbtRunDockerOperator

    DbtRunMixin <|-- DbtRunKubernetesOperator
    DbtKubernetesBaseOperator <|-- DbtRunKubernetesOperator
```

### 2. Producer-Consumer Pattern (Watcher Mode)

```mermaid
flowchart TD
    subgraph "Producer (Single Task)"
        PROD[DbtProducerWatcherOperator]
        PROD --> |"dbt build"| DBT[dbt process]
        DBT --> |"Stream events"| XCOM[XCom Storage]
    end

    subgraph "Consumers (Multiple Sensors)"
        XCOM --> C1[DbtConsumerWatcherSensor model_1]
        XCOM --> C2[DbtConsumerWatcherSensor model_2]
        XCOM --> C3[DbtConsumerWatcherSensor model_3]
        XCOM --> CN[DbtConsumerWatcherSensor model_N]
    end

    subgraph "Fallback"
        C1 --> |"On failure"| FB1[DbtRunLocalOperator]
        C2 --> |"On failure"| FB2[DbtRunLocalOperator]
    end
```

### 3. Factory Pattern (Dynamic Operator Creation)

```mermaid
sequenceDiagram
    participant GB as GraphBuilder
    participant CM as calculate_operator_class()
    participant IL as importlib
    participant OP as Operator Instance

    GB->>CM: execution_mode=WATCHER, command=run
    CM->>CM: Build class: "DbtConsumerWatcherSensor"
    CM->>CM: Build module: "cosmos.operators.watcher"
    CM->>IL: import_module(module)
    IL-->>CM: module
    CM->>CM: getattr(module, class_name)
    CM-->>GB: Operator class
    GB->>OP: Operator(task_id=..., **kwargs)
    OP-->>GB: Airflow Task
```

### 4. Registry Pattern (Profile Mappings)

```mermaid
flowchart TD
    CONN[Airflow Connection] --> REG[Profile Registry]

    subgraph "Registry Loop"
        REG --> PM1[PostgresMapping]
        REG --> PM2[SnowflakeMapping]
        REG --> PM3[BigQueryMapping]
        REG --> PM4[DuckDBMapping]
        REG --> PM5[MySQLMapping]
        REG --> PMN[...18+ Mappings]
    end

    PM1 --> |can_claim?| CHECK{Match?}
    PM2 --> |can_claim?| CHECK
    PM3 --> |can_claim?| CHECK

    CHECK --> |Yes| MATCH[Return Mapping]
    CHECK --> |No| NEXT[Try Next]
    NEXT --> REG

    MATCH --> PROFILE[Generate profiles.yml]
```

### 5. Strategy Pattern (Invocation Modes)

```mermaid
flowchart LR
    EXEC[execute] --> CHECK{invocation_mode?}

    CHECK --> |SUBPROCESS| SUB[run_subprocess]
    CHECK --> |DBT_RUNNER| RUNNER[run_dbt_runner]

    SUB --> |"subprocess.Popen()"| CMD[dbt run ...]
    RUNNER --> |"dbtRunner().invoke()"| API[Python API]

    CMD --> RESULT[Parse Output]
    API --> |"Event callbacks"| STREAM[Real-time Events]
    API --> RESULT
```

---

## Configuration System

### Configuration Classes

```mermaid
classDiagram
    class ProjectConfig {
        +dbt_project_path: Path
        +manifest_path: Path
        +project_name: str
        +env_vars: dict
        +dbt_vars: dict
        +partial_parse: bool
    }

    class ProfileConfig {
        +profile_name: str
        +target_name: str
        +profiles_yml_filepath: Path
        +profile_mapping: BaseProfileMapping
    }

    class ExecutionConfig {
        +execution_mode: ExecutionMode
        +invocation_mode: InvocationMode
        +dbt_executable_path: Path
        +virtualenv_dir: Path
    }

    class RenderConfig {
        +emit_datasets: bool
        +test_behavior: TestBehavior
        +load_method: LoadMode
        +select: list
        +exclude: list
    }

    class DbtToAirflowConverter {
        +project_config: ProjectConfig
        +profile_config: ProfileConfig
        +execution_config: ExecutionConfig
        +render_config: RenderConfig
    }

    ProjectConfig --> DbtToAirflowConverter
    ProfileConfig --> DbtToAirflowConverter
    ExecutionConfig --> DbtToAirflowConverter
    RenderConfig --> DbtToAirflowConverter
```

### Invocation Modes

```mermaid
graph TB
    subgraph "InvocationMode Enum"
        SUB[SUBPROCESS]
        RUNNER[DBT_RUNNER]
    end

    subgraph "Characteristics"
        SUB --> S1[Spawns subprocess]
        SUB --> S2[Captures stdout/stderr]
        SUB --> S3[Works with any dbt version]

        RUNNER --> R1[In-process execution]
        RUNNER --> R2[Event callbacks]
        RUNNER --> R3[Real-time streaming]
        RUNNER --> R4[Required for WATCHER mode]
    end
```

---

## Execution Modes

### Complete Execution Mode Matrix

```mermaid
graph TB
    subgraph "ExecutionMode Enum (10 modes)"
        LOCAL[LOCAL]
        DOCKER[DOCKER]
        K8S[KUBERNETES]
        VENV[VIRTUALENV]
        WATCHER[WATCHER - NEW]
        ASYNC[AIRFLOW_ASYNC - NEW]
        ECS[AWS_ECS - NEW]
        EKS[AWS_EKS]
        ACI[AZURE_CONTAINER_INSTANCE]
        GCR[GCP_CLOUD_RUN_JOB]
    end

    subgraph "Use Cases"
        LOCAL --> UC1[Development / Simple deployments]
        DOCKER --> UC2[Containerized execution]
        K8S --> UC3[Kubernetes-native]
        VENV --> UC4[Isolated Python envs]
        WATCHER --> UC5[Real-time monitoring]
        ASYNC --> UC6[Cloud-native async]
        ECS --> UC7[AWS ECS tasks]
        EKS --> UC8[AWS EKS pods]
        ACI --> UC9[Azure containers]
        GCR --> UC10[GCP Cloud Run]
    end
```

### Valid Mode Combinations

| ExecutionMode | InvocationMode.SUBPROCESS | InvocationMode.DBT_RUNNER |
|---------------|---------------------------|---------------------------|
| LOCAL | ✅ Default | ✅ Supported |
| VIRTUALENV | ✅ Only option | ❌ Not implemented |
| WATCHER | ✅ Fallback | ✅ Preferred (events) |
| DOCKER | N/A | N/A |
| KUBERNETES | N/A | N/A |
| AWS_ECS | N/A | N/A |
| AIRFLOW_ASYNC | N/A (profile-specific) | N/A |

---

## Watcher Mode

### Overview

The WATCHER execution mode implements a **producer-consumer pattern** for efficient, near real-time dbt execution monitoring.

### Architecture

```mermaid
sequenceDiagram
    participant DAG as Airflow DAG
    participant PROD as DbtProducerWatcherOperator
    participant DBT as dbt build
    participant XCOM as XCom
    participant CONS as DbtConsumerWatcherSensor
    participant TRIG as WatcherTrigger

    DAG->>PROD: Start producer task
    PROD->>DBT: dbt build (all models)

    loop For each model completion
        DBT->>PROD: NodeFinished event
        PROD->>XCOM: Push nodefinished_{model_id}
    end

    par Consumer sensors (deferred)
        CONS->>TRIG: Defer to trigger
        TRIG->>XCOM: Poll for model event
        XCOM-->>TRIG: Event data
        TRIG-->>CONS: TriggerEvent
        CONS->>CONS: Process result
    end
```

### Key Classes

```mermaid
classDiagram
    class DbtProducerWatcherOperator {
        +template_fields: tuple
        +_process_log_line_callable()
        +_handle_startup_event()
        +_handle_node_finished()
        +execute()
    }

    class DbtConsumerWatcherSensor {
        +model_unique_id: str
        +producer_task_id: str
        +deferrable: bool = True
        +poke_interval: int = 10
        +poke()
        +execute_complete()
    }

    class WatcherTrigger {
        +model_unique_id: str
        +producer_task_id: str
        +run()
        +get_xcom_val_af2()
        +get_xcom_val_af3()
    }

    class ProducerStateFetcher {
        <<interface>>
        +fetch_state()
    }

    DbtBuildMixin <|-- DbtProducerWatcherOperator
    DbtLocalBaseOperator <|-- DbtProducerWatcherOperator
    BaseSensorOperator <|-- DbtConsumerWatcherSensor
    BaseTrigger <|-- WatcherTrigger
    DbtConsumerWatcherSensor --> WatcherTrigger
```

### XCom Data Flow

```mermaid
flowchart LR
    subgraph "Producer XCom Keys"
        X1[dbt_startup_events]
        X2[nodefinished_model.project.model_a]
        X3[nodefinished_model.project.model_b]
        X4[run_results - fallback]
    end

    subgraph "Data Format"
        X2 --> |"zlib + base64"| COMP[Compressed JSON]
        COMP --> |"Contains"| DATA[node_status, timing, compiled_sql]
    end
```

---

## Async Execution Mode

### Overview

The AIRFLOW_ASYNC execution mode enables **deferrable, cloud-native execution** using database-specific async operators.

### Architecture

```mermaid
flowchart TD
    subgraph "Factory Pattern"
        FACTORY[DbtRunAirflowAsyncFactoryOperator]
        FACTORY --> |"profile_type"| DETECT{Detect Database}

        DETECT --> |"bigquery"| BQ[DbtRunAirflowAsyncBigqueryOperator]
        DETECT --> |"databricks"| DB[DbtRunAirflowAsyncDatabricksOperator]
        DETECT --> |"other"| STUB[NotImplementedError]
    end

    subgraph "BigQuery Async"
        BQ --> |"Inherits"| BQIJ[BigQueryInsertJobOperator]
        BQ --> |"Inherits"| ADLB[AbstractDbtLocalBase]
        BQ --> EXEC[Execute compiled SQL as BQ job]
    end
```

### Supported Databases

| Database | Status | Operator Class |
|----------|--------|----------------|
| BigQuery | ✅ Implemented | `DbtRunAirflowAsyncBigqueryOperator` |
| Databricks | 🚧 Stub | `DbtRunAirflowAsyncDatabricksOperator` |
| Others | ❌ Not supported | N/A |

### Class Hierarchy

```mermaid
classDiagram
    class DbtRunAirflowAsyncFactoryOperator {
        +profile_config: ProfileConfig
        +__init__()
    }

    class DbtRunAirflowAsyncBigqueryOperator {
        +compiled_sql: str
        +gcp_project: str
        +dataset: str
        +execute()
    }

    class BigQueryInsertJobOperator {
        +configuration: dict
        +deferrable: bool
    }

    class AbstractDbtLocalBase {
        +build_cmd()
        +add_global_flags()
    }

    DbtRunLocalOperator <|-- DbtRunAirflowAsyncFactoryOperator
    BigQueryInsertJobOperator <|-- DbtRunAirflowAsyncBigqueryOperator
    AbstractDbtLocalBase <|-- DbtRunAirflowAsyncBigqueryOperator
```

---

## dbt Integration

### dbt Runner Module

The `cosmos/dbt/runner.py` module provides a wrapper around `dbt.cli.main.dbtRunner` for in-process execution.

```mermaid
flowchart TD
    subgraph "dbt Runner API"
        CHECK[is_available] --> |"Check import"| AVAIL{dbt-core installed?}
        AVAIL --> |Yes| RUNNER[get_runner]
        AVAIL --> |No| FALLBACK[Use subprocess]

        RUNNER --> CMD[run_command]
        CMD --> |"callbacks"| EVENTS[Real-time events]
        CMD --> RESULT[dbtRunnerResult]
    end

    subgraph "Event Types"
        EVENTS --> E1[MainReportVersion]
        EVENTS --> E2[AdapterRegistered]
        EVENTS --> E3[NodeStart]
        EVENTS --> E4[NodeFinished]
        EVENTS --> E5[RunResult]
    end
```

### Project Parsing Flow

```mermaid
sequenceDiagram
    participant DD as DbtDag
    participant CONV as DbtToAirflowConverter
    participant DG as DbtGraph
    participant MAN as manifest.json
    participant LS as dbt ls

    DD->>CONV: Initialize with configs
    CONV->>DG: Create DbtGraph

    alt LoadMode.DBT_MANIFEST
        DG->>MAN: Read manifest.json
        MAN-->>DG: JSON nodes data
    else LoadMode.DBT_LS
        DG->>LS: Execute "dbt ls --output json"
        LS-->>DG: JSON lines output
    else LoadMode.AUTOMATIC
        DG->>MAN: Check if manifest exists
        alt manifest exists
            MAN-->>DG: Read manifest
        else
            DG->>LS: Run dbt ls
            LS-->>DG: JSON output
        end
    end

    DG->>DG: Create DbtNode objects
    DG->>DG: Build dependency graph
    DG-->>CONV: dict[unique_id, DbtNode]
```

### Node Selection (Graph Operators)

```mermaid
flowchart LR
    subgraph "dbt Selector Syntax"
        AT["@model_a"]
        PLUS_PRE["+model_b"]
        PLUS_POST["model_c+"]
        PATH["path:models/staging"]
        TAG["tag:nightly"]
        CONFIG["config.materialized:view"]
    end

    subgraph "Selection Result"
        AT --> ALL[Ancestors + Descendants]
        PLUS_PRE --> UP[Upstream nodes]
        PLUS_POST --> DOWN[Downstream nodes]
        PATH --> FILES[Matching files]
        TAG --> TAGGED[Tagged nodes]
        CONFIG --> CONFIGURED[Matching config]
    end
```

---

## Profile Mapping System

### Supported Databases (18+)

```mermaid
graph TB
    subgraph "Standard Databases"
        POSTGRES[PostgreSQL]
        MYSQL[MySQL - NEW]
        SQLITE[SQLite]
        SQLSERVER[SQL Server - NEW]
    end

    subgraph "Cloud Data Warehouses"
        SNOWFLAKE[Snowflake]
        BIGQUERY[BigQuery]
        REDSHIFT[Redshift]
        DATABRICKS[Databricks]
    end

    subgraph "Analytics Engines"
        SPARK[Apache Spark]
        TRINO[Trino/Presto]
        ATHENA[AWS Athena]
        CLICKHOUSE[ClickHouse]
        DUCKDB[DuckDB - NEW]
    end

    subgraph "Enterprise Databases"
        ORACLE[Oracle]
        TERADATA[Teradata]
        VERTICA[Vertica]
        EXASOL[Exasol]
    end
```

### Profile Mapping Architecture

```mermaid
classDiagram
    class BaseProfileMapping {
        <<abstract>>
        +conn_id: str
        +profile_args: dict
        +airflow_connection_type: str
        +dbt_profile_type: str
        +can_claim_connection(): bool
        +profile: dict
        +mock_profile: dict
        +env_vars: dict
        +version(): str
    }

    class PostgresUserPasswordProfileMapping {
        +dbt_profile_type = "postgres"
        +default_port = 5432
    }

    class DuckDBUserPasswordProfileMapping {
        +dbt_profile_type = "duckdb"
    }

    class MysqlUserPasswordProfileMapping {
        +dbt_profile_type = "mysql"
    }

    BaseProfileMapping <|-- PostgresUserPasswordProfileMapping
    BaseProfileMapping <|-- DuckDBUserPasswordProfileMapping
    BaseProfileMapping <|-- MysqlUserPasswordProfileMapping
```

---

## Airflow 2 vs 3 Compatibility

### Version Abstraction Layer

```mermaid
flowchart TD
    subgraph "Import Abstraction"
        CHECK{Airflow Version?}
        CHECK --> |"< 3.0"| AF2[Airflow 2 imports]
        CHECK --> |">= 3.0"| AF3[Airflow 3 imports]
    end

    subgraph "Airflow 2"
        AF2 --> BO2[airflow.models.BaseOperator]
        AF2 --> DS2[airflow.datasets.Dataset]
        AF2 --> TG2[airflow.utils.task_group.TaskGroup]
        AF2 --> CTX2[airflow.utils.context.Context]
    end

    subgraph "Airflow 3"
        AF3 --> BO3[airflow.sdk.bases.operator.BaseOperator]
        AF3 --> DS3[airflow.sdk.definitions.asset.Asset]
        AF3 --> TG3[airflow.sdk.TaskGroup]
        AF3 --> CTX3[airflow.sdk.definitions.context.Context]
    end
```

### Plugin Architecture

```mermaid
flowchart LR
    subgraph "Airflow 2 Plugin"
        AF2_PLUGIN[CosmosPlugin]
        AF2_PLUGIN --> FLASK[Flask-based UI]
        AF2_PLUGIN --> ABV[AirflowBaseView]
        FLASK --> DOCS2[/cosmos/dbt_docs]
    end

    subgraph "Airflow 3 Plugin"
        AF3_PLUGIN[CosmosAF3Plugin]
        AF3_PLUGIN --> FASTAPI[FastAPI-based UI]
        AF3_PLUGIN --> EXT[External Views]
        FASTAPI --> DOCS3[/cosmos/dbt_docs]
    end
```

### State Management (Watcher Mode)

```mermaid
flowchart TD
    BUILD[build_producer_state_fetcher]
    BUILD --> VER{airflow_version}

    VER --> |"< 3.0"| AF2_STATE[Airflow 2 State Fetcher]
    VER --> |">= 3.0"| AF3_STATE[Airflow 3 State Fetcher]

    AF2_STATE --> SQL[TaskInstance.query with SQL session]
    AF3_STATE --> SDK[RuntimeTaskInstance.get_task_states]
```

---

## Telemetry & Observability

### Listener Architecture

```mermaid
flowchart TD
    subgraph "Event Listeners"
        TIL[task_instance_listener]
        DRL[dag_run_listener]
    end

    subgraph "Hooks"
        TIL --> ON_RUN[@on_task_instance_running]
        TIL --> ON_SUCCESS[@on_task_instance_success]
        TIL --> ON_FAIL[@on_task_instance_failed]

        DRL --> DAG_SUCCESS[@on_dag_run_success]
        DRL --> DAG_FAIL[@on_dag_run_failed]
    end

    subgraph "Metrics Collected"
        ON_RUN --> M1[execution_mode]
        ON_RUN --> M2[invocation_mode]
        ON_RUN --> M3[dbt_command]
        ON_SUCCESS --> M4[duration]
        ON_FAIL --> M5[error_type]
    end

    M1 --> EMIT[emit_usage_metrics]
    M2 --> EMIT
    M3 --> EMIT
    M4 --> EMIT
    M5 --> EMIT
```

### Task Detection

```python
# Detection functions in task_instance_listener.py
_is_cosmos_task()      # Check if task uses Cosmos operators
_execution_mode()      # Extract execution mode from module path
_invocation_mode()     # Extract DBT_RUNNER or SUBPROCESS
_dbt_command()         # Extract run, build, test, seed, etc.
```

---

## Data Flow

### Complete Request Flow

```mermaid
sequenceDiagram
    participant USER as User
    participant DD as DbtDag
    participant CONV as Converter
    participant DG as DbtGraph
    participant GB as GraphBuilder
    participant AF as Airflow
    participant OP as Operator
    participant DBT as dbt
    participant TEL as Telemetry

    USER->>DD: Define DbtDag(configs)
    DD->>CONV: Initialize converter
    CONV->>DG: Parse dbt project
    DG-->>CONV: DbtNode dictionary

    CONV->>GB: build_airflow_graph()
    GB->>GB: Create tasks for each node
    GB-->>CONV: Airflow DAG with tasks

    CONV-->>DD: Complete DAG
    DD-->>AF: Register DAG

    Note over AF: DAG execution triggered

    AF->>OP: Execute task
    OP->>TEL: on_task_instance_running
    OP->>DBT: dbt command
    DBT-->>OP: Execution result
    OP->>TEL: on_task_instance_success/failed
    OP-->>AF: Task complete
```

### Caching Architecture

```mermaid
flowchart TD
    subgraph "Cache Layers"
        L1[Profile Cache - SHA256 versioned]
        L2[dbt ls Cache]
        L3[Manifest Cache]
        L4[Package Lock Cache]
        L5[Remote Cache - S3/GCS/Azure]
    end

    subgraph "Versioning"
        VER[versioning.py]
        VER --> |"MD5 hash"| L1
        VER --> |"Content-based"| L3
    end
```

---

## Extension Points

### Adding New Execution Mode

```mermaid
flowchart TD
    NEW[New Execution Mode] --> CONST[Add to ExecutionMode enum]
    CONST --> BASE[Create base operator class]
    BASE --> |"Extend AbstractDbtBaseOperator"| IMPL[Implement build_and_run_cmd]

    IMPL --> MIX1[DbtRunNewModeOperator]
    IMPL --> MIX2[DbtTestNewModeOperator]
    IMPL --> MIXN[...]

    MIX1 --> REG[Register in operators/__init__.py]
    REG --> GRAPH[Update graph.py calculate_operator_class]
    GRAPH --> EXPORT[Export in cosmos/__init__.py]
```

### Adding New Database Profile

```mermaid
flowchart TD
    NEW[New Database] --> CREATE[Create profiles/newdb/]
    CREATE --> IMPL[Implement BaseProfileMapping subclass]

    IMPL --> CAN[can_claim_connection]
    IMPL --> PROFILE[profile property]
    IMPL --> MOCK[mock_profile property]
    IMPL --> ENV[env_vars property]

    PROFILE --> REG[Add to profile_mappings list]
    REG --> EXPORT[Export in profiles/__init__.py]
```

---

## Operator Hierarchy

### Complete Operator Matrix

| Command | Local | Docker | K8s | Virtualenv | Watcher | Async | AWS ECS | AWS EKS | Azure ACI | GCP CR |
|---------|-------|--------|-----|------------|---------|-------|---------|---------|-----------|--------|
| run | ✅ | ✅ | ✅ | ✅ | Consumer | ✅ | ✅ | ✅ | ✅ | ✅ |
| test | ✅ | ✅ | ✅ | ✅ | Consumer | - | ✅ | ✅ | ✅ | ✅ |
| build | ✅ | ✅ | ✅ | ✅ | Producer | - | ✅ | ✅ | ✅ | ✅ |
| seed | ✅ | ✅ | ✅ | ✅ | - | - | ✅ | ✅ | ✅ | ✅ |
| snapshot | ✅ | ✅ | ✅ | ✅ | - | - | ✅ | ✅ | ✅ | ✅ |
| source | ✅ | ✅ | ✅ | - | Consumer | - | ✅ | ✅ | ✅ | ✅ |
| compile | ✅ | - | - | - | - | - | ✅ | - | - | - |
| docs | ✅ | - | - | - | - | - | - | - | - | - |
| clone | ✅ | - | - | - | - | - | ✅ | - | - | - |

---

## Key Design Principles

### 1. Airflow-First Design
- Uses native Airflow patterns (DAGs, Operators, Hooks, Plugins, Listeners)
- Leverages Airflow's scheduling, monitoring, and alerting
- Integrates with Airflow Datasets/Assets for lineage
- Full compatibility with Airflow 2.x and 3.x

### 2. Configuration Over Code
- Four configuration classes cover all use cases
- Sensible defaults with full customization
- Automatic profile mapping reduces boilerplate

### 3. Composition Over Inheritance
- Mixin-based operator design
- Easy to add new commands or execution modes
- Clear separation of concerns

### 4. Performance Optimization
- Multi-level caching (profiles, manifests, dbt ls output)
- Lazy loading of optional dependencies
- dbt-runner mode for faster in-process execution
- Watcher mode for efficient multi-model execution

### 5. Extensibility
- Registry pattern for profile mappings
- Factory pattern for operator instantiation
- Clear extension points for new databases and execution modes

### 6. Observability
- Built-in telemetry via listeners
- OpenLineage integration
- Comprehensive logging

---

## Summary

| Aspect | Details |
|--------|---------|
| **Entry Points** | DbtDag, DbtTaskGroup, Individual Operators |
| **Execution Modes** | 10 (Local, Docker, K8s, Virtualenv, Watcher, Async, AWS ECS/EKS, Azure ACI, GCP CR) |
| **Operators** | 60+ (8 commands × multiple execution modes) |
| **Databases** | 18+ with 20+ profile mapping classes |
| **Design Patterns** | Mixin, Factory, Registry, Producer-Consumer, Template Method, Strategy |
| **Caching** | Profile (versioned), Manifest, dbt ls, Package lock, Remote |
| **Airflow Compat** | Full support for Airflow 2.4+ and 3.x |
| **Observability** | Task/DAG listeners, Telemetry, OpenLineage |

The architecture enables flexible dbt orchestration while maintaining Airflow-native patterns, supporting diverse execution environments, and providing comprehensive observability.
