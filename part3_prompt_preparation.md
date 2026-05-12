# Prompt Preparation — aiokafka PR #115: Fix Serialization for the Batch API

---

## What is aiokafka?

aiokafka is an async Python client for Apache Kafka. It wraps Kafka's networking in Python's `asyncio` so your application never has to block the event loop waiting on a broker. If you've ever tried integrating Kafka with an async Python service the naive way — threads, blocking calls, the whole mess — you'll immediately understand why this library exists.

The people who use it are Python developers building event-driven systems, microservices, or data pipelines on top of Kafka. They're already comfortable with `asyncio` and they know their way around Kafka concepts like partitions, consumer groups, and offsets. For them, aiokafka handles all the plumbing: connection management, partition leader tracking, group coordination, producer batching, serialization, and transactional semantics — all without blocking.

The codebase is organized roughly like this:

- `aiokafka/producer/` — the `AIOKafkaProducer`, including batching, transactions, and the public `BatchBuilder` API
- `aiokafka/consumer/` — the `AIOKafkaConsumer` with group coordination and offset tracking
- `aiokafka/admin/` — admin tools for managing topics and partitions
- `aiokafka/record/` — low-level record format encoding (both the legacy v0/v1 format and the current v2)
- `tests/` — integration and unit tests that need a live Kafka broker to run

Supported Kafka versions go all the way back to 0.8.x, with the API version negotiated at runtime. Python 3.8+ is required.

---

## What's the bug, and what does this PR fix?

This PR fixes **issue #886**: when you configure a producer with a `key_serializer` and/or `value_serializer`, those serializers are silently ignored if you use the batch API (`create_batch()` + `send_batch()`). They work fine with the regular `send()` API — just not with batches.

**What was happening before:** `create_batch()` called `self._message_accumulator.create_builder()` with no arguments. That meant the `BatchBuilder` was created with no knowledge of any serializers. So when you called `batch.append(key="my_key", value={"data": 1})`, your raw Python objects got passed straight to the record builder — which either blew up with a `TypeError` (it expected bytes) or, worse, silently stored the unserialized data.

**What happens now:** `create_batch()` now passes `key_serializer=self._key_serializer` and `value_serializer=self._value_serializer` along to `create_builder()`. The `BatchBuilder` already had the plumbing for this — `_key_serializer`, `_value_serializer`, and a working `_serialize()` method (lines 51–61 of `message_accumulator.py`). It just wasn't being wired up from `create_batch()`. Now it is.

The end result: the batch API and `send()` are now consistent. A producer configured with serializers will serialize your messages no matter which send path you use.

A new integration test — `test_producer_send_batch_with_serializer` in `tests/test_producer.py` at line 390 — validates this end-to-end against a live broker.

**Files touched:**
- `aiokafka/producer/producer.py` (~line 557) — the one-line fix: pass serializers to `create_builder()`
- `tests/test_producer.py` — the new integration test at line 390

---

## What "done" looks like

Here's what this fix needs to get right:

- A producer configured with `key_serializer` and `value_serializer` should apply both when you call `batch.append()`. The output bytes should be identical to what `producer.send()` would have produced for the same inputs.
- Calling `producer.create_batch()` on a serializer-configured producer should give you a `BatchBuilder` whose `_key_serializer` attribute matches the producer's — not `None`.
- If you create a producer *without* any serializers (the default), nothing changes. Raw `bytes` inputs still work exactly as before. No regressions.
- Partial configuration — only a `key_serializer`, or only a `value_serializer` — should work correctly, serializing just the configured side and leaving the other as-is. That's how `producer.send()` already behaves.
- When the batch gets sent via `send_batch()` and consumed by an `AIOKafkaConsumer`, the consumer should see the serialized bytes — e.g., `b"KEY1"` for an uppercase-and-encode key serializer, `b'{"value":111}'` for a JSON value serializer.
- `batch.append(key=None, value=None)` should not try to run `None` through a serializer. The existing `_serialize()` logic already handles this — just make sure the fix doesn't accidentally break it.
- The existing tests in `tests/test_message_accumulator.py` must keep passing. Internal calls to `create_builder()` that don't pass serializer arguments should still produce valid `BatchBuilder` instances with `None` serializers.

---

## Edge cases worth thinking about

**What if the serializer returns `None`?**
Some serializers might return `None` for certain inputs — for example, a filtering serializer that drops messages it doesn't recognize. That `None` needs to pass through to the record builder cleanly. Kafka's record format already handles `None` values (they're tombstones in compacted topics), so this shouldn't crash — but it's worth verifying that `batch.append(key=b"k", value=something_that_serializes_to_None)` produces a valid metadata result rather than a `TypeError`.

**What if the serializer throws?**
If your `key_serializer` or `value_serializer` raises an exception (say, you passed a non-JSON-serializable object to a JSON serializer), that exception should propagate straight out of `batch.append()` to the caller. There's no `asyncio` exception-handling layer here — `append()` is synchronous — so the error will just bubble up directly. The important thing is that the batch's internal `_relative_offset` counter shouldn't have been incremented yet when the error happens. No corrupted state.

**Mixing the batch API with `send()` in the same session**
It's perfectly valid for code to use both paths on the same producer — sometimes calling `producer.send()`, other times building a batch manually. With serializers configured, both paths now serialize consistently. Before this fix, mixing the two would silently produce inconsistent data on the broker. That was hard to debug and easy to miss.

**Transactional producers**
When `transactional_id` is set, `create_builder()` sets `is_transactional=True` and uses the `DefaultRecordBatchBuilder` (magic=2). The serializer fix must not interfere with this. Both `is_transactional` and the serializer arguments need to be forwarded correctly to `BatchBuilder`. A transactional producer building a serialized batch and committing it in a transaction should work without issues.

**Appending to a closed batch**
If someone calls `batch.close()` and then tries to `batch.append()` anyway, the return value should be `None` (the batch is closed, nothing was added). This should happen *before* `_serialize()` is ever called — the close-state check at line 80 of `message_accumulator.py` already short-circuits early. Just confirm the fix doesn't accidentally move that check somewhere it no longer fires first.

---

## The prompt

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
