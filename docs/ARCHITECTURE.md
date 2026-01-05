# Astronomer Cosmos Architecture

This document provides a comprehensive overview of the Astronomer Cosmos architecture, design principles, and key implementation concepts.

## Table of Contents

1. [Overview](#overview)
2. [High-Level Architecture](#high-level-architecture)
3. [Core Components](#core-components)
4. [Design Patterns](#design-patterns)
5. [Configuration System](#configuration-system)
6. [Operator Architecture](#operator-architecture)
7. [dbt Integration](#dbt-integration)
8. [Profile Mapping System](#profile-mapping-system)
9. [Data Flow](#data-flow)
10. [Extension Points](#extension-points)

---

## Overview

**Astronomer Cosmos** is an Apache Airflow provider package that orchestrates dbt (data build tool) projects within Airflow. It transforms dbt workflows into native Airflow DAGs and task groups, enabling seamless integration between the two ecosystems.

### Key Capabilities

- Convert dbt projects to Airflow DAGs automatically
- Support multiple execution modes (Local, Docker, Kubernetes, etc.)
- Map Airflow connections to dbt profiles automatically
- Integrate with Airflow's scheduling, monitoring, and alerting
- Provide lineage tracking via OpenLineage and Airflow Datasets

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
        LOCAL[Local Operator]
        DOCKER[Docker Operator]
        K8S[Kubernetes Operator]
        VENV[Virtualenv Operator]
        CLOUD[Cloud Operators]
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
    TASKS --> VENV
    TASKS --> CLOUD

    PFC --> PM
    CONN --> PM
    PM --> PROFILES

    LOCAL --> PROFILES
    DOCKER --> PROFILES
    K8S --> PROFILES
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
```

### Directory Structure

```
cosmos/
├── __init__.py              # Public API exports
├── airflow/                 # High-level DAG/TaskGroup APIs
│   ├── dag.py              # DbtDag class
│   ├── task_group.py       # DbtTaskGroup class
│   └── graph.py            # Graph building logic
├── core/                    # Core abstractions
│   └── graph/
│       └── entities.py     # Task, Group, CosmosEntity models
├── dbt/                     # dbt integration
│   ├── graph.py            # DbtNode, DbtGraph
│   ├── selector.py         # Node selection logic
│   └── parser/             # Manifest/output parsing
├── operators/              # Execution operators (50+)
│   ├── base.py             # AbstractDbtBaseOperator
│   ├── local.py            # Local execution
│   ├── docker.py           # Docker execution
│   ├── kubernetes.py       # Kubernetes execution
│   └── virtualenv.py       # Virtualenv execution
├── profiles/               # Database profile mappings (15+)
│   ├── base.py             # BaseProfileMapping
│   ├── postgres/
│   ├── snowflake/
│   └── bigquery/
├── plugin/                 # Airflow plugin
├── config.py               # Configuration classes
├── converter.py            # Core conversion logic
├── cache.py                # Caching mechanisms
└── constants.py            # Enums and constants
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
        +add_cmd_flags()
    }

    class DbtTestMixin {
        +base_cmd = ["test"]
        +ui_color = "#F0EAD6"
        +add_cmd_flags()
    }

    class DbtSeedMixin {
        +base_cmd = ["seed"]
        +ui_color = "#F58D7E"
    }

    class DbtLocalBaseOperator {
        +run_subprocess()
        +run_dbt_runner()
        +build_and_run_cmd()
    }

    class DbtDockerBaseOperator {
        +build_and_run_cmd()
    }

    class DbtKubernetesBaseOperator {
        +build_and_run_cmd()
    }

    class DbtRunLocalOperator {
    }

    class DbtTestLocalOperator {
    }

    class DbtRunDockerOperator {
    }

    AbstractDbtBaseOperator <|-- DbtLocalBaseOperator
    AbstractDbtBaseOperator <|-- DbtDockerBaseOperator
    AbstractDbtBaseOperator <|-- DbtKubernetesBaseOperator

    DbtRunMixin <|-- DbtRunLocalOperator
    DbtLocalBaseOperator <|-- DbtRunLocalOperator

    DbtTestMixin <|-- DbtTestLocalOperator
    DbtLocalBaseOperator <|-- DbtTestLocalOperator

    DbtRunMixin <|-- DbtRunDockerOperator
    DbtDockerBaseOperator <|-- DbtRunDockerOperator
```

**Benefits:**
- Reusable command logic across 7+ execution modes
- Clean separation of concerns
- Easy to add new commands or execution modes

### 2. Registry Pattern

Profile mappings are registered in a central registry for automatic detection.

```mermaid
flowchart TD
    CONN[Airflow Connection] --> REG[Profile Registry]

    subgraph "Registry Loop"
        REG --> PM1[PostgresMapping]
        REG --> PM2[SnowflakeMapping]
        REG --> PM3[BigQueryMapping]
        REG --> PM4[RedshiftMapping]
        REG --> PMN[...20+ Mappings]
    end

    PM1 --> |can_claim?| CHECK{Connection Type Match?}
    PM2 --> |can_claim?| CHECK
    PM3 --> |can_claim?| CHECK
    PM4 --> |can_claim?| CHECK
    PMN --> |can_claim?| CHECK

    CHECK --> |Yes| MATCH[Return Mapping Instance]
    CHECK --> |No| NEXT[Try Next Mapping]
    NEXT --> REG

    MATCH --> PROFILE[Generate profiles.yml]
```

### 3. Factory Pattern

Operators are dynamically instantiated based on configuration.

```mermaid
sequenceDiagram
    participant GB as GraphBuilder
    participant CM as calculate_operator_class()
    participant IL as importlib
    participant OP as Operator Instance

    GB->>CM: execution_mode=DOCKER, command=run
    CM->>CM: Build class name: "DbtRunDockerOperator"
    CM->>CM: Build module: "cosmos.operators.docker"
    CM->>IL: import_module("cosmos.operators.docker")
    IL-->>CM: module
    CM->>CM: getattr(module, "DbtRunDockerOperator")
    CM-->>GB: Operator class
    GB->>OP: Operator(task_id=..., **kwargs)
    OP-->>GB: Airflow Task
```

### 4. Template Method Pattern

Base operator defines the command building process; subclasses implement specific steps.

```mermaid
flowchart TD
    subgraph "AbstractDbtBaseOperator"
        BUILD[build_cmd]
        BUILD --> EXE[dbt_executable_path]
        BUILD --> GF[add_global_flags]
        BUILD --> CF[add_cmd_flags]
        BUILD --> FINAL[Final Command]
    end

    subgraph "Mixin Override"
        CF --> |DbtRunMixin| RUN["--select", model]
        CF --> |DbtTestMixin| TEST["--select", test]
        CF --> |DbtSeedMixin| SEED["--full-refresh"]
    end

    subgraph "Execution Override"
        FINAL --> LOCAL[DbtLocalBaseOperator.execute]
        FINAL --> DOCKER[DbtDockerBaseOperator.execute]
        FINAL --> K8S[DbtKubernetesBaseOperator.execute]
    end
```

### 5. Strategy Pattern

Different invocation strategies for dbt execution.

```mermaid
flowchart LR
    EXEC[execute] --> CHECK{invocation_mode?}

    CHECK --> |SUBPROCESS| SUB[run_subprocess]
    CHECK --> |DBT_RUNNER| RUNNER[run_dbt_runner]

    SUB --> |"subprocess.run()"| CMD[dbt run ...]
    RUNNER --> |"dbtRunner().invoke()"| API[Python API]

    CMD --> RESULT[Parse Output]
    API --> RESULT
```

---

## Configuration System

### Configuration Classes Hierarchy

```mermaid
classDiagram
    class ProjectConfig {
        +dbt_project_path: Path
        +manifest_path: Path
        +project_name: str
        +env_vars: dict
        +dbt_vars: dict
        +partial_parse: bool
        +validate_project()
    }

    class ProfileConfig {
        +profile_name: str
        +target_name: str
        +profiles_yml_filepath: Path
        +profile_mapping: BaseProfileMapping
        +get_profile_file_contents()
    }

    class ExecutionConfig {
        +execution_mode: ExecutionMode
        +invocation_mode: InvocationMode
        +dbt_executable_path: Path
        +virtualenv_dir: Path
        +validate()
    }

    class RenderConfig {
        +emit_datasets: bool
        +test_behavior: TestBehavior
        +load_method: LoadMode
        +select: list
        +exclude: list
        +source_rendering_behavior: SourceRenderingBehavior
        +validate()
    }

    class DbtToAirflowConverter {
        +project_config: ProjectConfig
        +profile_config: ProfileConfig
        +execution_config: ExecutionConfig
        +render_config: RenderConfig
        +dbt_graph: DbtGraph
        +build_airflow_graph()
    }

    ProjectConfig --> DbtToAirflowConverter
    ProfileConfig --> DbtToAirflowConverter
    ExecutionConfig --> DbtToAirflowConverter
    RenderConfig --> DbtToAirflowConverter
```

### Execution Modes

```mermaid
graph TB
    subgraph "ExecutionMode Enum"
        LOCAL[LOCAL]
        DOCKER[DOCKER]
        K8S[KUBERNETES]
        VENV[VIRTUALENV]
        EKS[AWS_EKS]
        ACI[AZURE_CONTAINER_INSTANCE]
        GCR[GCP_CLOUD_RUN_JOB]
        ASYNC[AIRFLOW_ASYNC]
    end

    subgraph "Use Cases"
        LOCAL --> UC1[Development/Simple deployments]
        DOCKER --> UC2[Containerized execution]
        K8S --> UC3[Kubernetes-native deployments]
        VENV --> UC4[Isolated Python environments]
        EKS --> UC5[AWS EKS clusters]
        ACI --> UC6[Azure container instances]
        GCR --> UC7[GCP Cloud Run jobs]
        ASYNC --> UC8[Async task execution]
    end
```

### Load Methods

```mermaid
flowchart TD
    AUTO[AUTOMATIC] --> CHECK{manifest.json exists?}

    CHECK --> |Yes| MANIFEST[DBT_MANIFEST]
    CHECK --> |No| LS[DBT_LS]

    MANIFEST --> |"Parse manifest.json"| FAST[Fastest - No dbt execution]
    LS --> |"Run dbt ls --output json"| PARSE[Parse JSON output]

    LS_FILE[DBT_LS_FILE] --> |"Read pre-computed file"| PARSE
    CUSTOM[CUSTOM] --> |"User-provided loader"| NODES[DbtNode objects]

    FAST --> NODES
    PARSE --> NODES
```

---

## Operator Architecture

### Complete Operator Hierarchy

```mermaid
graph TB
    subgraph "Airflow Base"
        BO[BaseOperator]
    end

    subgraph "Cosmos Base"
        ADBO[AbstractDbtBaseOperator]
    end

    subgraph "Command Mixins"
        RUN[DbtRunMixin]
        TEST[DbtTestMixin]
        SEED[DbtSeedMixin]
        SNAP[DbtSnapshotMixin]
        BUILD[DbtBuildMixin]
        LS[DbtLSMixin]
        SOURCE[DbtSourceMixin]
        COMPILE[DbtCompileMixin]
    end

    subgraph "Execution Base Classes"
        LOCAL[DbtLocalBaseOperator]
        DOCKER[DbtDockerBaseOperator]
        K8S[DbtKubernetesBaseOperator]
        VENV[DbtVirtualenvBaseOperator]
    end

    subgraph "Concrete Operators (Examples)"
        RLO[DbtRunLocalOperator]
        TLO[DbtTestLocalOperator]
        RDO[DbtRunDockerOperator]
        RKO[DbtRunKubernetesOperator]
    end

    BO --> ADBO
    ADBO --> LOCAL
    ADBO --> DOCKER
    ADBO --> K8S
    LOCAL --> VENV

    RUN --> RLO
    LOCAL --> RLO

    TEST --> TLO
    LOCAL --> TLO

    RUN --> RDO
    DOCKER --> RDO

    RUN --> RKO
    K8S --> RKO
```

### Operator Composition Matrix

| Command | Local | Docker | Kubernetes | Virtualenv | AWS EKS | Azure ACI | GCP Cloud Run |
|---------|-------|--------|------------|------------|---------|-----------|---------------|
| run | DbtRunLocalOperator | DbtRunDockerOperator | DbtRunKubernetesOperator | DbtRunVirtualenvOperator | DbtRunAwsEksOperator | DbtRunAzureContainerInstanceOperator | DbtRunGcpCloudRunJobOperator |
| test | DbtTestLocalOperator | DbtTestDockerOperator | DbtTestKubernetesOperator | DbtTestVirtualenvOperator | ... | ... | ... |
| seed | DbtSeedLocalOperator | DbtSeedDockerOperator | DbtSeedKubernetesOperator | ... | ... | ... | ... |
| snapshot | DbtSnapshotLocalOperator | ... | ... | ... | ... | ... | ... |
| build | DbtBuildLocalOperator | ... | ... | ... | ... | ... | ... |
| compile | DbtCompileLocalOperator | ... | ... | ... | ... | ... | ... |
| source | DbtSourceLocalOperator | ... | ... | ... | ... | ... | ... |
| docs | DbtDocsLocalOperator | ... | ... | ... | ... | ... | ... |

---

## dbt Integration

### dbt Project Parsing Flow

```mermaid
sequenceDiagram
    participant DD as DbtDag
    participant CONV as DbtToAirflowConverter
    participant DG as DbtGraph
    participant PROJ as dbt_project.yml
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
    CONV->>CONV: build_airflow_graph()
```

### DbtNode Structure

```mermaid
classDiagram
    class DbtNode {
        +unique_id: str
        +resource_type: DbtResourceType
        +depends_on: list[str]
        +file_path: Path
        +tags: list[str]
        +config: dict
        +has_test: bool
        +has_freshness: bool
        +dbt_name: str
        +airflow_task_config: dict
    }

    class DbtResourceType {
        <<enumeration>>
        MODEL
        TEST
        SEED
        SNAPSHOT
        SOURCE
    }

    class DbtGraph {
        +nodes: dict[str, DbtNode]
        +filtered_nodes: dict[str, DbtNode]
        +load()
        +select_nodes()
        +update_node_dependency()
    }

    DbtNode --> DbtResourceType
    DbtGraph --> DbtNode
```

### Node Selection (Graph Operators)

```mermaid
flowchart LR
    subgraph "dbt Selector Syntax"
        AT["@model_a"]
        PLUS_PRE["+model_b"]
        PLUS_POST["model_c+"]
        PLUS_BOTH["+model_d+"]
        N_PLUS["2+model_e"]
        PATH["path:models/staging"]
        TAG["tag:nightly"]
        CONFIG["config.materialized:view"]
    end

    subgraph "Selection Result"
        AT --> |"Ancestors + Descendants"| ALL[All related nodes]
        PLUS_PRE --> |"Parents"| UP[Upstream nodes]
        PLUS_POST --> |"Children"| DOWN[Downstream nodes]
        PLUS_BOTH --> |"Both directions"| BOTH[All connected]
        N_PLUS --> |"N generations"| NGEN[N levels up]
        PATH --> |"File path match"| FILES[Matching files]
        TAG --> |"Tag filter"| TAGGED[Tagged nodes]
        CONFIG --> |"Config filter"| CONFIGURED[Matching config]
    end
```

### Test Behavior Modes

```mermaid
flowchart TD
    subgraph "TestBehavior.AFTER_EACH"
        AE_M1[model_1_run] --> AE_T1[model_1_test]
        AE_M2[model_2_run] --> AE_T2[model_2_test]
        AE_M3[model_3_run] --> AE_T3[model_3_test]
    end

    subgraph "TestBehavior.AFTER_ALL"
        AA_M1[model_1_run] --> AA_TA[all_tests]
        AA_M2[model_2_run] --> AA_TA
        AA_M3[model_3_run] --> AA_TA
    end

    subgraph "TestBehavior.BUILD"
        BUILD_ALL[dbt_build] --> |"models + tests"| DONE[Complete]
    end

    subgraph "TestBehavior.NONE"
        NONE_M1[model_1_run]
        NONE_M2[model_2_run]
        NONE_M3[model_3_run]
    end
```

---

## Profile Mapping System

### Profile Mapping Architecture

```mermaid
classDiagram
    class BaseProfileMapping {
        <<abstract>>
        +conn_id: str
        +profile_args: dict
        +can_claim_connection(): bool
        +profile: dict
        +mock_profile: dict
        +env_vars: dict
        +get_profile_file_contents(): str
    }

    class PostgresUserPasswordProfileMapping {
        +dbt_profile_type = "postgres"
        +default_port = 5432
        +airflow_param_mapping: dict
    }

    class SnowflakeUserPasswordProfileMapping {
        +dbt_profile_type = "snowflake"
        +airflow_param_mapping: dict
    }

    class GoogleCloudServiceAccountFileProfileMapping {
        +dbt_profile_type = "bigquery"
        +airflow_param_mapping: dict
    }

    class DatabricksTokenProfileMapping {
        +dbt_profile_type = "databricks"
    }

    BaseProfileMapping <|-- PostgresUserPasswordProfileMapping
    BaseProfileMapping <|-- SnowflakeUserPasswordProfileMapping
    BaseProfileMapping <|-- GoogleCloudServiceAccountFileProfileMapping
    BaseProfileMapping <|-- DatabricksTokenProfileMapping
```

### Connection to Profile Transformation

```mermaid
flowchart LR
    subgraph "Airflow Connection"
        AC_HOST[host: db.example.com]
        AC_PORT[port: 5432]
        AC_USER[login: dbt_user]
        AC_PASS[password: ****]
        AC_SCHEMA[schema: analytics]
    end

    MAPPER[Profile Mapping]

    subgraph "dbt Profile"
        DBT_TYPE[type: postgres]
        DBT_HOST[host: db.example.com]
        DBT_PORT[port: 5432]
        DBT_USER[user: dbt_user]
        DBT_PASS[password: env_var]
        DBT_DB[dbname: analytics]
        DBT_THREADS[threads: 4]
    end

    AC_HOST --> MAPPER
    AC_PORT --> MAPPER
    AC_USER --> MAPPER
    AC_PASS --> MAPPER
    AC_SCHEMA --> MAPPER

    MAPPER --> DBT_TYPE
    MAPPER --> DBT_HOST
    MAPPER --> DBT_PORT
    MAPPER --> DBT_USER
    MAPPER --> DBT_PASS
    MAPPER --> DBT_DB
    MAPPER --> DBT_THREADS
```

### Supported Database Profiles

```mermaid
graph TB
    subgraph "Standard Databases"
        POSTGRES[PostgreSQL]
        MYSQL[MySQL]
        SQLITE[SQLite]
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
    end

    subgraph "Enterprise Databases"
        ORACLE[Oracle]
        TERADATA[Teradata]
        VERTICA[Vertica]
        EXASOL[Exasol]
    end
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
    participant DBT as dbt CLI

    USER->>DD: Define DbtDag(project_config, ...)
    DD->>CONV: Initialize converter
    CONV->>DG: Parse dbt project

    alt Using Manifest
        DG->>DG: Load manifest.json
    else Using dbt ls
        DG->>DBT: dbt ls --output json
        DBT-->>DG: JSON nodes
    end

    DG-->>CONV: DbtNode dictionary

    CONV->>GB: build_airflow_graph()
    GB->>GB: Create tasks for each node
    GB->>GB: Set up dependencies
    GB-->>CONV: Airflow DAG with tasks

    CONV-->>DD: Complete DAG
    DD-->>AF: Register DAG

    Note over AF: DAG execution triggered

    AF->>OP: Execute task
    OP->>OP: Build dbt command
    OP->>OP: Setup environment
    OP->>DBT: dbt run --select model
    DBT-->>OP: Execution result
    OP-->>AF: Task success/failure
```

### Caching Architecture

```mermaid
flowchart TD
    subgraph "Cache Layers"
        L1[Profile Cache]
        L2[dbt ls Cache]
        L3[Manifest Cache]
        L4[Package Lock Cache]
        L5[Remote Cache]
    end

    subgraph "Cache Storage"
        LOCAL_FS[Local Filesystem]
        AIRFLOW_VAR[Airflow Variables]
        REMOTE[S3 / GCS / Azure]
    end

    L1 --> |"profiles.yml + SHA256"| LOCAL_FS
    L2 --> |"dbt ls output"| LOCAL_FS
    L3 --> |"manifest.json"| LOCAL_FS
    L4 --> |"package-lock.yml"| LOCAL_FS
    L5 --> |"Distributed cache"| REMOTE

    AIRFLOW_VAR --> |"Cache invalidation"| L1
    AIRFLOW_VAR --> |"Cache invalidation"| L2
```

---

## Extension Points

### Adding New Execution Mode

```mermaid
flowchart TD
    NEW[New Execution Mode] --> BASE[Create Base Operator]
    BASE --> |"Extend AbstractDbtBaseOperator"| IMPL[Implement build_and_run_cmd]

    IMPL --> MIX1[Create DbtRunNewModeOperator]
    IMPL --> MIX2[Create DbtTestNewModeOperator]
    IMPL --> MIX3[Create DbtSeedNewModeOperator]

    MIX1 --> |"DbtRunMixin + NewModeBaseOperator"| REG[Register in operators/__init__.py]
    MIX2 --> REG
    MIX3 --> REG

    REG --> CONST[Add to ExecutionMode enum]
    CONST --> EXPORT[Export in cosmos/__init__.py]
```

### Adding New Database Profile

```mermaid
flowchart TD
    NEW[New Database] --> CREATE[Create profiles/newdb/]
    CREATE --> IMPL[Implement BaseProfileMapping]

    IMPL --> METHODS[Define Methods]
    METHODS --> CAN[can_claim_connection]
    METHODS --> PROFILE[profile property]
    METHODS --> MOCK[mock_profile property]
    METHODS --> ENV[env_vars property]

    CAN --> REG[Add to profile_mappings list]
    PROFILE --> REG
    MOCK --> REG
    ENV --> REG

    REG --> EXPORT[Export in profiles/__init__.py]
```

---

## Plugin Architecture

### Airflow Plugin Integration

```mermaid
flowchart TD
    subgraph "CosmosPlugin"
        PLUGIN[CosmosPlugin class]
        VIEWS[Flask Views]
        LISTENERS[Event Listeners]
    end

    subgraph "Airflow UI"
        MENU[Menu Items]
        DOCS_VIEW[dbt Docs View]
    end

    subgraph "Event System"
        DAG_RUN[DAG Run Events]
        TELEMETRY[Telemetry Collection]
    end

    PLUGIN --> VIEWS
    PLUGIN --> LISTENERS

    VIEWS --> MENU
    VIEWS --> DOCS_VIEW

    LISTENERS --> DAG_RUN
    DAG_RUN --> TELEMETRY
```

---

## Key Design Principles

### 1. Airflow-First Design
- Uses native Airflow patterns (DAGs, Operators, Hooks, Plugins)
- Leverages Airflow's scheduling, monitoring, and alerting
- Integrates with Airflow Datasets for lineage

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
- dbt-runner mode for faster execution

### 5. Extensibility
- Registry pattern for profile mappings
- Factory pattern for operator instantiation
- Clear extension points for new databases and execution modes

### 6. Developer Experience
- Comprehensive error messages with validation
- Logging and telemetry for debugging
- Support for mock profiles in CI/CD

---

## Summary

Astronomer Cosmos provides a robust bridge between dbt and Airflow through:

| Aspect | Implementation |
|--------|----------------|
| **Entry Points** | DbtDag, DbtTaskGroup, Individual Operators |
| **Operators** | 50+ (8 commands × 7 execution modes) |
| **Databases** | 15+ with 20+ profile mapping classes |
| **Design Patterns** | Mixin, Factory, Registry, Template Method, Strategy |
| **Caching** | Profile, Manifest, dbt ls, Package lock, Remote |
| **Integration** | Airflow Datasets, OpenLineage, Plugins, Listeners |

The architecture enables flexible dbt orchestration while maintaining Airflow-native patterns and supporting diverse execution environments.
