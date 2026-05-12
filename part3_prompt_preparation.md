# Prompt Preparation Document
## aiokafka — PR #115: Fix Serialization for Batch API

---

## 3.1.1 Repository Context

**aiokafka** is an asynchronous Python client library for Apache Kafka built on top of `asyncio`. It provides non-blocking, coroutine-based interfaces for producing and consuming messages to and from Kafka brokers.

**Intended users** are Python developers building high-throughput, event-driven applications, microservices, or data pipelines where Kafka is the messaging backbone and performance/non-blocking I/O are critical requirements. Users are typically comfortable with `asyncio` and familiar with Kafka concepts (topics, partitions, consumer groups, offsets).

**Problem domain:** The library addresses the challenge of integrating Apache Kafka — inherently a networked, I/O-bound system — with Python's `asyncio` event loop. Without this library, developers would either block the event loop waiting on Kafka network calls, or resort to thread pools. aiokafka handles connection management, partition leadership tracking, consumer group coordination, producer batching, message serialization/deserialization, and transactional semantics — all asynchronously.

The repository includes:
- [`aiokafka/producer/`](aiokafka/producer/) — `AIOKafkaProducer` with batching, transactions, and the `BatchBuilder` public API
- [`aiokafka/consumer/`](aiokafka/consumer/) — `AIOKafkaConsumer` with group coordination and offset management
- [`aiokafka/admin/`](aiokafka/admin/) — `AIOKafkaAdminClient` for topic/partition administration
- [`aiokafka/record/`](aiokafka/record/) — Low-level Kafka record format encoding (legacy v0/v1, and default v2)
- [`tests/`](tests/) — Integration and unit tests requiring a live Kafka broker

**Kafka versions supported** range from 0.8.x through current, with the API version negotiated at runtime. Python 3.8+ is required.

---

## 3.1.2 Pull Request Description

**PR #115** fixes a bug reported in **issue #886**: when a producer is configured with `key_serializer` and/or `value_serializer`, those serializers are **not applied** when using the batch API (`create_batch()` + `send_batch()`), even though they are correctly applied when using the regular `send()` API.

**Previous behavior:** `AIOKafkaProducer.create_batch()` called `self._message_accumulator.create_builder()` with **no arguments**, meaning the `BatchBuilder` was constructed without `key_serializer` or `value_serializer`. When a user called `batch.append(key="my_key", value={"data": 1})`, the raw Python objects were passed directly to the record builder, causing either a `TypeError` (bytes required) or silently storing unserialised values.

**New behavior:** `AIOKafkaProducer.create_batch()` now passes `key_serializer=self._key_serializer` and `value_serializer=self._value_serializer` to `create_builder()`. The `BatchBuilder` class, which already holds `_key_serializer` and `_value_serializer` attributes and has a working `_serialize()` method at lines 51–61 of [`aiokafka/producer/message_accumulator.py`](aiokafka/producer/message_accumulator.py), then correctly applies those serializers inside its `append()` method before passing bytes to the underlying record builder.

The fix makes the batch API and the `send()` API consistent: a producer configured with serializers will serialize messages regardless of which send path is used. A new integration test `test_producer_send_batch_with_serializer` (in [`tests/test_producer.py`](tests/test_producer.py) at line 390) validates this end-to-end against a live Kafka broker.

**Files changed:**
- [`aiokafka/producer/producer.py`](aiokafka/producer/producer.py) — line ~557: pass serializers to `create_builder()`
- [`tests/test_producer.py`](tests/test_producer.py) — new integration test at line 390

---

## 3.1.3 Acceptance Criteria

✓ **When** a producer is configured with a `key_serializer` and `value_serializer`, **the system should** apply both serializers to keys and values appended via `batch.append()`, producing the same bytes that `producer.send()` would produce for the same inputs.

✓ **When** `producer.create_batch()` is called on a producer that has a `key_serializer` configured, **the returned `BatchBuilder` instance should** have a non-`None` `_key_serializer` attribute equal to the producer's `_key_serializer`.

✓ **When** `producer.create_batch()` is called on a producer with **no serializers configured** (default `None`), **the system should** produce a `BatchBuilder` with `key_serializer=None` and `value_serializer=None`, preserving backwards compatibility — raw `bytes` inputs must still work exactly as before.

✓ **When** a batch built with serialization is submitted via `send_batch()` and consumed by an `AIOKafkaConsumer`, **the consumer should** receive messages where `msg.key` and `msg.value` are the serialized byte representations (e.g., `b"KEY1"` for a key serializer that upper-cases and encodes, and `b'{"value":111}'` for a JSON value serializer).

✓ **When** only `key_serializer` is configured (and `value_serializer` is `None`), **the implementation should** serialize only the key while leaving the value as raw bytes — and vice versa for `value_serializer` only — matching the partial-serialization behaviour of `producer.send()`.

✓ **When** `batch.append()` is called with a `None` key or `None` value, **the system should** not call the serializer on the `None` value and should pass `None` directly to the record builder, consistent with the existing behaviour in `BatchBuilder._serialize()` lines 52–60.

✓ **The implementation should** pass the existing test suite in [`tests/test_message_accumulator.py`](tests/test_message_accumulator.py) without modification, confirming that internal `create_builder()` calls (which pass no serializer arguments) still produce valid `BatchBuilder` instances with `None` serializers.

---

## 3.1.4 Edge Cases

**Edge Case 1 — Serializer returns `None`**
If a user supplies a `value_serializer` that returns `None` for a given input (e.g., a serializer that filters messages), the `BatchBuilder._serialize()` path must not raise a `TypeError`. The record builder already handles `None` values (tombstone/delete semantics in compacted topics), so the serializer result of `None` must be passed through cleanly. Verify that `batch.append(key=b"k", value=some_value_that_serializes_to_None)` produces a valid metadata result rather than crashing.

**Edge Case 2 — Serializer raises an exception**
If `key_serializer` or `value_serializer` raises during `batch.append()` (e.g., a JSON serializer given a non-serializable object), the exception must propagate synchronously out of `batch.append()` to the caller. Because `BatchBuilder.append()` is not a coroutine, there is no `asyncio` error handling layer here — the exception must bubble up directly. Ensure no batch state corruption occurs (e.g., `_relative_offset` should not have been incremented before the error).

**Edge Case 3 — Mixed usage: batch API alongside `send()` in the same producer session**
A producer configured with serializers may use both `producer.send(topic, key="k", value={"a":1})` and `batch = producer.create_batch(); batch.append(key="k", value={"a":1}); producer.send_batch(batch, topic, partition=0)` in the same session. Both paths must produce byte-identical messages on the broker. Any regression where one path serializes and the other does not would silently produce corrupt data that is difficult to debug downstream.

**Edge Case 4 — Transactional producer with batch API**
When `transactional_id` is configured, `create_builder()` sets `is_transactional=True` and uses the `DefaultRecordBatchBuilder` (magic=2). The fix must not disturb this: both `is_transactional` and serializers must be correctly forwarded to `BatchBuilder`. Verify that a transactional producer can build a serialized batch and commit it in a transaction without errors.

**Edge Case 5 — Explicitly closed batch submitted after serialization**
If a user calls `batch.close()` before `send_batch()`, subsequent `batch.append()` calls must return `None` (batch is closed) regardless of whether serializers are configured. The close state check in `BatchBuilder.append()` (line 80 of `message_accumulator.py`) must short-circuit before `_serialize()` is called, avoiding unnecessary serializer invocations on a closed batch.

---

## 3.1.5 Initial Prompt

```
You are implementing a bug fix for the aiokafka library, an asyncio-based 
Apache Kafka client for Python.

## Background

aiokafka's `AIOKafkaProducer` supports two message-sending interfaces:

1. **Regular send**: `await producer.send(topic, key=k, value=v)` — 
   serializes key/value using the producer's configured `key_serializer` 
   and `value_serializer` before enqueueing.

2. **Batch send**: `batch = producer.create_batch()`, then 
   `batch.append(key=k, value=v, timestamp=None)`, then 
   `await producer.send_batch(batch, topic, partition=p)` — intended to 
   give callers fine-grained control over batching.

## The Bug (Issue #886)

When `AIOKafkaProducer` is initialized with `key_serializer` and/or 
`value_serializer`, the batch API does NOT apply those serializers. 
This is because `create_batch()` in `producer.py` calls 
`self._message_accumulator.create_builder()` without passing the 
serializer arguments:

```python
# producer.py ~line 557 — BUGGY
def create_batch(self):
    return self._message_accumulator.create_builder()  # no serializers!
```

The `BatchBuilder` class (in `aiokafka/producer/message_accumulator.py`) 
already has full serialization support: it stores `_key_serializer` and 
`_value_serializer`, and its `_serialize()` method (lines 51–61) applies 
them during `append()`. The infrastructure is there — it just isn't 
wired up from `create_batch()`.

## Your Task

**Fix `AIOKafkaProducer.create_batch()`** in 
`aiokafka/producer/producer.py` to pass the producer's serializers to 
`create_builder()`:

```python
def create_batch(self):
    return self._message_accumulator.create_builder(
        key_serializer=self._key_serializer,
        value_serializer=self._value_serializer,
    )
```

**Add an integration test** `test_producer_send_batch_with_serializer` 
in `tests/test_producer.py` that:
- Creates a producer with a `key_serializer` (e.g., upper-cases + encodes 
  a string) and a `value_serializer` (e.g., JSON-encodes a dict)
- Calls `producer.create_batch()` and appends messages with non-bytes keys 
  and values
- Submits the batch via `send_batch()`
- Reads back the messages with an `AIOKafkaConsumer` and asserts that 
  `msg.key` and `msg.value` are the expected serialized bytes

## Acceptance Criteria to Satisfy

1. `producer.create_batch()` on a producer with serializers returns a 
   `BatchBuilder` whose `_key_serializer` and `_value_serializer` match 
   the producer's.
2. Messages appended via `batch.append()` are serialized identically to 
   those sent via `producer.send()`.
3. Producers with NO serializers configured continue to work as before 
   (raw bytes pass through).
4. Partial serializer configuration (only key or only value) works 
   correctly.
5. The new integration test passes end-to-end against a live Kafka broker.

## Edge Cases to Handle

- `batch.append()` with `None` key or value must not invoke the serializer 
  on `None` (the existing `_serialize()` already handles this correctly — 
  confirm your changes don't break it).
- If the serializer raises, the exception should propagate from 
  `batch.append()` without corrupting the batch's `_relative_offset`.
- Transactional producers (with `transactional_id` set) must still produce 
  correctly formatted batches — the `is_transactional` flag and magic 
  version selection in `create_builder()` must be unaffected by this 
  change.
- Existing tests in `tests/test_message_accumulator.py` that call 
  `create_builder()` with no arguments must continue to pass.

## Relevant Files

- `aiokafka/producer/producer.py` — `create_batch()` at ~line 548, 
  `_serialize()` at ~line 403 (reference for how serializers are applied 
  in the regular send path)
- `aiokafka/producer/message_accumulator.py` — `BatchBuilder` class 
  (lines 18–137), `BatchBuilder._serialize()` (lines 51–61), 
  `MessageAccumulator.create_builder()` (line 502)
- `tests/test_producer.py` — existing `test_producer_send_batch` at ~line 
  354, `test_producer_send_with_serializer` at ~line 194 (reference for 
  serializer test patterns)
- `tests/test_message_accumulator.py` — `test_add_batch_builder` at line 
  215 (must remain passing)

The fix itself is a one-line change. The integration test is the 
substantive deliverable. Make sure the test verifies consumed message 
content byte-for-byte, not just that no exception was raised.
```