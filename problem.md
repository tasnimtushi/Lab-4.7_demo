# Analysis of Issues and Flaws in Lab 4.7 Flow

This document details all bugs, logic errors, architectural issues, and environment incompatibilities identified in the **Lab 4.7 --- Streaming Feast: Real-Time Feature Updates with Kafka and Faust** guide.

---

## Table of Issues

| ID | Issue Category | Severity | Description |
| :--- | :--- | :--- | :--- |
| **1** | Feast API / Schema | **Critical (Showstopper)** | `PushSource` requires `batch_source` when used in a `FeatureView`; `feast apply` crashes. |
| **2** | Lab Logic / State Flaw | **High** | Chapter 10 instructs adding an event to `produce_events.py` and rerunning, which resends all past events and causes incorrect state (`count: 5, spend: 1100.0` instead of `3` and `700.0`). |
| **3** | Output Discrepancy | **Medium** | `read_features.py` produces unprefixed keys (`purchase_count`), while lab guide expects `user_activity_fv__purchase_count`. |
| **4** | Package Dependencies | **High** | `feast>=0.53,<0.54` restricts `pyarrow<=17.0.0`, which fails to build on Python 3.12+ (`No module named 'pkg_resources'`). |
| **5** | Shell / OS Portability | **Medium** | Linux/Bash-specific commands (`mkdir -p {...}`, `touch`, `source`, `python3`) fail on Windows / PowerShell. |
| **6** | Infrastructure Dependency | **Medium** | Hard dependency on Docker daemon; no fallback or local broker guidance if Docker is unavailable. |
| **7** | Deprecation Warning | **Low** | Feast `Entity` definition omits `value_type`, triggering deprecation warnings. |
| **8** | Stream State Fault-Tolerance | **Medium** | Faust `Table` lacks durable storage configuration / RocksDB on Windows and changelog recovery. |
| **9** | Data Type Precision | **Low** | `Float32` defined in schema while pandas creates `float64` by default; may cause issues on strict online stores. |

---

## Detailed Issue Analysis & Recommendations

### 1. [CRITICAL] Missing `batch_source` in `PushSource` (`feature_definitions.py`)

#### Problem
In **Chapter 4**, `feature_definitions.py` defines:
```python
user_activity_push_source = PushSource(
    name="user_activity_push_source",
)

user_activity_fv = FeatureView(
    name="user_activity_fv",
    entities=[user],
    ttl=timedelta(days=1),
    schema=[
        Field(name="purchase_count", dtype=Int64),
        Field(name="total_spend", dtype=Float32),
        Field(name="last_purchase_amount", dtype=Float32),
    ],
    source=user_activity_push_source,
    online=True,
)
```

When executing `feast apply` in **Chapter 5**, Feast halts immediately with:
```text
ValueError: A batch_source needs to be specified for stream source `user_activity_push_source`
```

#### Cause
In Feast's `FeatureView` implementation (`feast/feature_view.py`), any streaming source (`PushSource`, `KafkaSource`, etc.) assigned to a `FeatureView` **must** specify a `batch_source`. Feast uses the batch source for offline schema validation, historical feature retrieval, and materialization fallbacks:
```python
if (
    isinstance(self.data_source, PushSource)
    or isinstance(self.data_source, KafkaSource)
    ...
):
    self.stream_source = self.data_source
    if not self.data_source.batch_source:
        raise ValueError(
            f"A batch_source needs to be specified for stream source `{self.data_source.name}`"
        )
```

#### Fix
Add a `batch_source` (e.g. `FileSource`) to `PushSource` in `feature_repo/feature_definitions.py`:
```python
from datetime import timedelta
from feast import Entity, FeatureView, Field, FileSource, PushSource, ValueType
from feast.types import Float32, Int64

user = Entity(
    name="user",
    join_keys=["user_id"],
    value_type=ValueType.STRING,
)

batch_source = FileSource(
    name="user_activity_batch_source",
    path="data/user_activity.parquet",
    timestamp_field="event_timestamp",
)

user_activity_push_source = PushSource(
    name="user_activity_push_source",
    batch_source=batch_source,
)

user_activity_fv = FeatureView(
    name="user_activity_fv",
    entities=[user],
    ttl=timedelta(days=1),
    schema=[
        Field(name="purchase_count", dtype=Int64),
        Field(name="total_spend", dtype=Float32),
        Field(name="last_purchase_amount", dtype=Float32),
    ],
    source=user_activity_push_source,
    online=True,
)
```

---

### 2. [HIGH] Chapter 10 Event Duplication Causes Incorrect State

#### Problem
In **Chapter 10**, the guide instructs:
> Add another event to the `events` list in `scripts/produce_events.py`:
> ```python
> {"user_id": "user-1001", "purchase_amount": 300.0},
> ```
> Run the producer again:
> ```bash
> python scripts/produce_events.py
> ```
> Expected values for `user-1001`, assuming the same worker remains running and retains its state:
> ```text
> user_id: ['user-1001']
> user_activity_fv__purchase_count: [3]
> user_activity_fv__total_spend: [700.0]
> user_activity_fv__last_purchase_amount: [300.0]
> ```

#### Cause
If the user appends the event to the existing list:
```python
events = [
    {"user_id": "user-1001", "purchase_amount": 250.0},
    {"user_id": "user-1002", "purchase_amount": 450.0},
    {"user_id": "user-1001", "purchase_amount": 150.0},
    {"user_id": "user-1003", "purchase_amount": 700.0},
    {"user_id": "user-1002", "purchase_amount": 200.0},
    {"user_id": "user-1001", "purchase_amount": 300.0}, # Appended
]
```
Running `python scripts/produce_events.py` **re-publishes all 5 previous events** to Kafka in addition to the 6th event.
Because the Faust worker is still running and listening to `user-events`, it will process all 6 messages again:
- Initial state for `user-1001`: `count = 2`, `total_spend = 400.0`
- Re-sent events:
  - Event 1: `user-1001` +$250 -> `count = 3`, `total_spend = 650.0`
  - Event 3: `user-1001` +$150 -> `count = 4`, `total_spend = 800.0`
  - Event 6: `user-1001` +$300 -> `count = 5`, `total_spend = 1100.0`

**Actual Result:**
- `purchase_count`: `5` (Expected: `3`)
- `total_spend`: `1100.0` (Expected: `700.0`)

#### Fix
The guide should instruct the user to **replace** the `events` list in Chapter 10 with only the new event:
```python
events = [
    {"user_id": "user-1001", "purchase_amount": 300.0},
]
```
Or create a separate script (e.g., `scripts/produce_single_event.py`).

---

### 3. [MEDIUM] Output Key Name Discrepancy in `read_features.py`

#### Problem
In **Chapter 9** and **Chapter 10**, the guide lists the expected output as:
```text
user_id: ['user-1001']
user_activity_fv__purchase_count: [2]
user_activity_fv__total_spend: [400.0]
user_activity_fv__last_purchase_amount: [150.0]
```

However, running `scripts/read_features.py` outputs:
```text
user_id: ['user-1001']
purchase_count: [2]
total_spend: [400.0]
last_purchase_amount: [150.0]
```

#### Cause
In Feast, the method signature for `get_online_features` has default `full_feature_names=False`:
```python
def get_online_features(
    self,
    features: List[str] | FeatureService,
    entity_rows: List[Dict[str, Any]],
    full_feature_names: bool = False,
    ...
)
```
When `full_feature_names=False`, Feast returns raw feature column names (`purchase_count`), not prefixed names (`user_activity_fv__purchase_count`).

#### Fix
Either:
1. Update `scripts/read_features.py` to specify `full_feature_names=True`:
   ```python
   features = store.get_online_features(
       features=[
           "user_activity_fv:purchase_count",
           "user_activity_fv:total_spend",
           "user_activity_fv:last_purchase_amount",
       ],
       entity_rows=[
           {"user_id": "user-1001"},
       ],
       full_feature_names=True,
   ).to_dict()
   ```
2. Or update the expected output in the Markdown document to match `full_feature_names=False`.

---

### 4. [HIGH] Dependency Constraint `feast>=0.53,<0.54` Breaks on Python 3.12+

#### Problem
In **Chapter 1**, `requirements.txt` specifies:
```text
feast>=0.53,<0.54
faust-streaming>=0.11,<0.12
kafka-python>=2.2,<3
pandas>=2.2,<3
```
Running `pip install -r requirements.txt` on Python 3.12 or newer (including Python 3.14) fails with:
```text
ERROR: Failed to build 'pyarrow' when getting requirements to build wheel
ModuleNotFoundError: No module named 'pkg_resources'
```

#### Cause
- `feast 0.53.0` has a strict dependency `pyarrow<=17.0.0`.
- `pyarrow<=17.0.0` does not provide pre-compiled wheels for newer Python versions, forcing pip to build from source.
- Building pyarrow 17 from source requires legacy `pkg_resources` (removed in setuptools/Python 3.12+) and C++ CMake tools.
- Modern feast releases (e.g. `feast>=0.54` or `>=0.60`) loosen this requirement to `pyarrow>=17.0.0`, allowing pre-compiled wheels.

#### Fix
Update `requirements.txt` to:
```text
feast>=0.54
faust-streaming>=0.11
kafka-python>=2.2
pandas>=2.2,<3
```

---

### 5. [MEDIUM] Shell / OS Command Incompatibilities

#### Problem
The lab instructions assume a Linux Bash shell:
- `mkdir -p streaming-feast/{feature_repo/data,scripts,src}` uses Bash brace expansion. On PowerShell, this creates a literal folder name `streaming-feast\{feature_repo/data,scripts,src}`.
- `touch src/__init__.py`: `touch` is not recognized natively on Windows CMD or PowerShell.
- `source .venv/bin/activate`: `source` and `.venv/bin/activate` are Unix-only. On Windows PowerShell, the command is `.venv\Scripts\Activate.ps1`, or on CMD `.venv\Scripts\activate.bat`.
- `python3 -m venv .venv`: `python3` frequently redirects to the Microsoft Store on Windows unless configured.

#### Fix
Provide cross-platform instructions or specify PowerShell/Windows alternatives:
```powershell
# PowerShell:
New-Item -ItemType Directory -Force -Path streaming-feast/feature_repo/data, streaming-feast/scripts, streaming-feast/src
New-Item -ItemType File -Force -Path streaming-feast/src/__init__.py
python -m venv .venv
.venv\Scripts\Activate.ps1
```

---

### 6. [MEDIUM] Docker Daemon Dependency & Local Kafka Provisioning

#### Problem
In **Chapter 2 & 3**, the lab relies on `docker compose up -d` and `docker exec datahive-kafka ...`.
If Docker is not running or not installed, Kafka cannot be started, and:
- `produce_events.py` throws `kafka.errors.KafkaTimeoutError: Unable to bootstrap from localhost:9092`.
- `streaming.py` hangs or repeatedly retries broker connection.

#### Fix
Add a prerequisite check for Docker running, or document how to run a local Kafka binary without Docker.

---

### 7. [LOW] Feast Entity Deprecation Warning

#### Problem
Defining `Entity(name="user", join_keys=["user_id"])` generates:
```text
DeprecationWarning: Entity value_type will be mandatory in the next release. Please specify a value_type for entity 'user'.
```

#### Fix
Add `value_type=ValueType.STRING`:
```python
from feast import Entity, ValueType

user = Entity(
    name="user",
    join_keys=["user_id"],
    value_type=ValueType.STRING,
)
```

---

### 8. [MEDIUM] Faust Stream State Durability

#### Problem
In `src/streaming.py`:
```python
user_stats = app.Table(
    "user_stats",
    default=dict,
)
```
- By default, Faust's Table uses a local in-memory or RocksDB store.
- On Windows, RocksDB (`python-rocksdb`) is unsupported, so Faust defaults to in-memory tables.
- If the Faust worker is restarted, all cumulative user stats (`purchase_count`, `total_spend`) are wiped out in memory, causing subsequent purchases to start counting from 1 again, while Feast online store already contains the previous counts.

#### Fix
For a production or reliable pipeline, either configure Faust changelog topic replication or fetch current stats from the Feast online store upon cold start.

---

### 9. [LOW] Floating-Point Type Precision in DataFrame

#### Problem
In `feature_definitions.py`:
```python
Field(name="total_spend", dtype=Float32),
Field(name="last_purchase_amount", dtype=Float32),
```
In `src/streaming.py`:
```python
amount = float(event["purchase_amount"])
```
Python standard `float` becomes `float64` in Pandas DataFrames.
While SQLite online store coerces float types flexibly, strict online store providers (like Redis, Snowflake, BigQuery) may reject precision mismatches or warn on conversion.

#### Fix
Cast DataFrame fields explicitly:
```python
feature_data["total_spend"] = feature_data["total_spend"].astype("float32")
feature_data["last_purchase_amount"] = feature_data["last_purchase_amount"].astype("float32")
```

---

## Summary of Necessary Action Items to Fix the Lab

1. **Fix `feature_definitions.py`**: Add a `batch_source` (such as `FileSource`) to `PushSource` so `feast apply` succeeds.
2. **Fix `requirements.txt`**: Unpin or upgrade `feast>=0.54` to support modern Python environments.
3. **Fix Chapter 10 in R&D**: Change the instructions to send only the new event in `produce_events.py` rather than appending and re-executing all past events.
4. **Fix `read_features.py`**: Pass `full_feature_names=True` if `user_activity_fv__*` keys are expected, or update documentation expected output.
5. **Add Windows / PowerShell command notes** for users outside Linux environments.
