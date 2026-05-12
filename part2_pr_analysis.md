# aiokafka PR Analysis

---

## PR Selection

| PR | Commit Hash | Era | Selected | Reason |
|----|-------------|-----|----------|--------|
| PR115 | `5bf26b2` | 2024 | ✓ | Restores `api_version` auto-negotiation to both `AIOKafkaClient` and `AIOKafkaAdminClient`, introduces a `metrics` module—clear scope, modern codebase |
| PR143 | `0e39ec7` | 2017 | ✓ | Introduces `NoGroupCoordinator` / `BaseCoordinator` hierarchy and fixes stale fetcher records—tightly scoped, well-tested |

---

## PR115 — Restoring `api_version` Negotiation and Adding a Metrics Module

### PR Summary

The master branch had completely removed the `api_version` parameter from `AIOKafkaClient` and `AIOKafkaAdminClient`, marking it deprecated (a no-op) since v0.13.0. This PR reverses that direction: it restores `api_version` as a first-class, functional configuration option with a default of `"auto"`. In `"auto"` mode the client probes the broker at connect time by sending an `ApiVersionRequest_v0` and caches the broker's supported API key ranges. In manual mode the caller passes a version string like `"0.9"` which is parsed into a tuple and used as a hint to skip version lookup on older brokers. The PR also introduces a self-contained `aiokafka/metrics/` package (ported from `kafka-python`) providing a sensor/metric registry with pluggable reporters.

### Technical Changes

- **`aiokafka/client.py`**
  - `api_version` parameter default changed from `None` (deprecated no-op) → `"auto"` (functional)
  - Deprecation warning block removed
  - New `api_version` property: returns parsed tuple or `(0, 9, 0)` as safe fallback
  - Bootstrap now sends `MetadataRequest[0]` (v0) when version is `"auto"` or `< (0, 10)` to avoid assuming broker capabilities
  - After bootstrap, calls `self.check_version()` when in `"auto"` mode to finalize `self._api_version`
  - `version_hint` propagated to each new `AIOKafkaConnection` so connections know whether to skip `ApiVersionRequest` lookup
  - `ConnectionGroup` and `CoordinationType` changed from `IntEnum` → plain class (cosmetic/compatibility)

- **`aiokafka/conn.py`**
  - New `VersionInfo` class (`conn.py`) wraps the broker's `{api_key: (min, max)}` dict and exposes a `pick_best(request_versions)` method that walks the versioned request list in reverse to find the highest mutually supported version
  - `_versions` dict replaced by `self._version_info = VersionInfo({})`
  - `_do_version_lookup` now sends `ApiVersionRequest[0]()` explicitly (version-pinned) rather than the unversioned alias
  - SASL handshake now uses `version_info.pick_best(SaslHandShakeRequest)` instead of always sending a fixed version; falls back to no-handshake for v0.9 GSSAPI
  - `version_hint` constructor parameter added; version lookup is skipped when `version_hint < (0, 10)`

- **`aiokafka/admin/client.py`**
  - `api_version` parameter restored as `str = "auto"` (was `None = None` + deprecation warning)
  - `_get_version_info()` async method added: sends `ApiVersionRequest_v0` and populates `self._version_info`
  - `_matching_api_version(operation)` method added: resolves the highest API version mutually supported by client and broker, raising `IncompatibleBrokerVersion` on mismatch
  - All admin operations (`create_topics`, `delete_topics`, `describe_configs`, `alter_configs`, `create_partitions`, `describe_groups`, `list_groups`, `find_coordinator`, `list_offsets`, `delete_records`) now call `_matching_api_version(...)` to pick the right request version instead of hardcoding version indices
  - Type annotations updated from Python 3.10+ union syntax (`X | Y`) to `Union[X, Y]` for older Python compatibility

- **`aiokafka/helpers.py`**
  - Same union-syntax downgrade (`X | Y` → `Union[X, Y]`)

- **`aiokafka/metrics/`** *(new package)*
  - `metrics.py` — `Metrics` registry: manages `Sensor` objects and `KafkaMetric` instances; supports expiration of idle sensors
  - `kafka_metric.py` — `KafkaMetric`: a named metric backed by a `Measurable`
  - `metric_name.py` / `metric_config.py` / `quota.py` — name/config/quota value objects
  - `measurable.py` / `measurable_stat.py` / `compound_stat.py` — abstract stat interfaces
  - `metrics_reporter.py` — `AbstractMetricsReporter` ABC
  - `dict_reporter.py` — `DictReporter`: stores metrics in a two-level `{category: {name: metric}}` dict; `snapshot()` returns current values
  - `stats/` — concrete stat implementations: `Avg`, `Max`, `Min`, `Count`, `Rate`, `Total`, `Percentiles`, `Sensor`

### Implementation Approach

The core idea is a two-phase connection strategy. During bootstrap, the client deliberately uses the lowest-common-denominator `MetadataRequest[0]` so it can talk to any broker ≥ 0.9. Once the TCP connection is established, the client sends an `ApiVersionRequest_v0` (a lightweight introspection call all modern Kafka brokers answer) to learn exactly which API keys and version ranges the broker supports. This response is stored in a `VersionInfo` object on each `AIOKafkaConnection`. Subsequent requests — metadata, fetch, produce, admin — call `VersionInfo.pick_best()` or `_matching_api_version()` to select the highest request version that both the library and the broker understand, rather than guessing or hardcoding.

When the user provides an explicit version string like `"0.9"`, it is parsed into a tuple once at startup and passed as `version_hint` to connections. Connections use this hint to decide whether even to bother sending `ApiVersionRequest` (brokers older than 0.10 don't support it reliably), gracefully degrading to minimal-version requests.

The `metrics` module is ported verbatim from `kafka-python` as a standalone registry. It is not yet wired into `AIOKafkaProducer` or `AIOKafkaConsumer` in this PR — it is a foundation layer exposed via `aiokafka/__init__.py` for users and future internal instrumentation.

### Potential Impact

Every outbound Kafka request in both producer and consumer paths is affected, since they all route through `AIOKafkaConnection` and `AIOKafkaClient`. Admin clients gain dynamic protocol version selection, removing the need to manually keep version indices in sync with broker upgrades. Operators running older Kafka clusters (pre-0.10) can now pin `api_version="0.9"` to restore the prior no-lookup behavior. The metrics module has no runtime impact yet but establishes the import surface.

---

## PR143 — `NoGroupCoordinator` and Stale Fetcher Record Cleanup

### PR Summary

This PR addresses two related problems that arose when a consumer operates without a `group_id`. Previously, `AIOKafkaConsumer` used `GroupCoordinator` for all consumers, but a coordinator backed by a Kafka group requires broker interaction for offset commits, heartbeats, and partition assignment — none of which apply to a `group_id=None` consumer. The consumer also lacked any mechanism to respond to partition metadata changes (e.g., new topics matching a subscribed pattern). Separately, the fetcher pre-fetches records per-partition into a `_records` dict; when the consumer resubscribes mid-flight, records for the old partitions were left in the cache and could be incorrectly delivered after the subscription changed. This PR fixes both issues.

### Technical Changes

- **`aiokafka/group_coordinator.py`**
  - New `BaseCoordinator` base class extracted from `GroupCoordinator`, containing shared metadata-handling logic: `_handle_metadata_update`, `_metadata_changed`, `_get_metadata_snapshot`, `_on_change_assignment`
  - New `NoGroupCoordinator(BaseCoordinator)` class — a lightweight coordinator for `group_id=None` consumers:
    - Overrides `_handle_metadata_update` to call `assign_all_partitions()` whenever metadata changes
    - `assign_all_partitions(check_unknown=False)` assigns all partitions from subscribed topics directly to this consumer, bypassing broker-side group management
    - `close()` is a no-op coroutine (no broker resources to release)
  - `GroupCoordinator` now extends `BaseCoordinator` and delegates init to `super().__init__(...)`
  - New `assignment_changed_cb` parameter added to both `BaseCoordinator` and `GroupCoordinator` constructors (replaces the separate `on_group_rebalanced(cb)` call pattern)

- **`aiokafka/consumer.py`**
  - `NoGroupCoordinator` imported alongside `GroupCoordinator`
  - `start()` branch for no-`group_id` now instantiates `NoGroupCoordinator` instead of doing ad-hoc partition assignment inline
  - If the subscription `needs_partition_assignment` at start, calls `coordinator.assign_all_partitions(check_unknown=True)` (clean, single place)
  - `subscription()` return value guarded: `frozenset(self._subscription.subscription or [])` prevents `TypeError` when subscription is `None`

- **`aiokafka/fetcher.py`**
  - In `getone()` (`_fetched_records` path): after pulling a record batch for partition `tp`, checks `if not self._subscriptions.is_assigned(tp): del self._records[tp]` — stale cached records are evicted immediately
  - Same guard added in `getmany()` path
  - `[x.result() for x in done]` list comprehension replaced with an explicit `for` loop — avoids constructing a throw-away list just for side effects
  - `_wait_empty_future` exception propagation comment added for clarity

- **`tests/test_consumer.py`**
  - `test_consumer_subscribe_pattern_autocreate_no_group_id`: integration test verifying that a `group_id=None` consumer with a pattern subscription picks up newly-created topics as metadata updates arrive
  - `test_consumer_cleanup_unassigned_data_getone`: verifies that `_records` cache entries for old partitions are evicted after a `subscribe()` call, using `getone()`
  - `test_consumer_cleanup_unassigned_data_getmany`: same verification using `getmany()`

- **`tests/_testutil.py`** / **`tests/conftest.py`** — minor test-harness updates

### Implementation Approach

The central design move is the extraction of `BaseCoordinator`. The shared logic — registering a metadata listener with the cluster, diffing the current metadata snapshot against the last-assigned snapshot, notifying upstream code of changes — belongs to all coordinator types, not just group-aware ones. By pulling this into a base class, `NoGroupCoordinator` gets pattern subscription and metadata-change awareness for free, without any Kafka group-management overhead (no heartbeat task, no JoinGroup/SyncGroup RPCs, no offset commit).

`NoGroupCoordinator.assign_all_partitions()` is the key method: whenever `_handle_metadata_update` detects a change (new partitions or new topics matching the pattern), it computes the full set of `TopicPartition` objects from current cluster metadata and calls `subscription.assign_from_subscribed(partitions)` directly — the same call that `GroupCoordinator` uses after a rebalance, ensuring consistent downstream behavior.

The fetcher fix is a simple but important guard: the `_records` dict is keyed by `TopicPartition`. After a subscription change, partitions are unassigned before the next fetch cycle completes. The added `is_assigned(tp)` check in both `getone` and `getmany` ensures stale entries are dropped on the spot rather than surfaced to the caller.

### Potential Impact

The `NoGroupCoordinator` change affects every consumer instantiated with `group_id=None` — standalone consumers and those using manual partition assignment via topic subscription. These consumers now automatically react to partition count changes and pattern-matched topic creation, behavior that was silently absent before. The `GroupCoordinator` itself is structurally changed (now a subclass) but its external behavior is unchanged. The fetcher fix affects all consumers: any mid-flight subscription change that leaves stale records in `_records` will now be cleaned up deterministically, preventing stale-message delivery bugs.