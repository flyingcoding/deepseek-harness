# Agent Note: Complete projections for bounded cold history

Status: implemented

English | [中文](2026-09-06-bounded-history-projection-replay.zh.md)

## Problem

A cold history window can omit the events that select a model or establish a turn outline. A missing or version-mismatched projection cache then leaves the model picker loading indefinitely even though the visible messages have loaded. Activating an Agent to recover these values would also retain the complete Session log merely for browsing.

## Decision

Persistence window reads can supply a visitor for the complete validated prefix behind the returned window. JSONL establishes the exact inherited cut before traversing the same input bytes again; the second traversal retains one event and never opens a full-log handle. The history controller folds all registered projections through the existing restore operation in page-sized batches and publishes their complete baseline with the opening snapshot.

The visitor does not publish partial results. Cancellation, invalid stored data, or a projection failure rejects the read. Full-prefix replay also avoids treating stale checkpoint values as current selections or deriving state from an incomplete visible tail. Small or interrupted logs retain their ordinary prepared-observation path.

## Alternatives considered

Activating an Agent to populate projections retains the complete Session for a browsing operation. Folding only the visible tail or trusting an old checkpoint can omit a pending model selection and misstate turn navigation. Full-prefix replay preserves both read-only activation policy and complete projection semantics.

## Consequences

A projection-bearing JSONL window decodes its input twice. Its event retention is bounded by the visible window, a projection batch, and one decoder event; compressed input bytes, individual event payloads, and projection state have independent sizes. This cost preserves complete projection semantics without Agent activation or a second filesystem snapshot that could race an append.

## Verification

Storage tests cover plaintext and Zstandard visitors, events outside the returned window, exact fork metadata, cancellation, and caller failures. Controller tests rebuild a model selection outside the tail with missing and stale caches while refusing full observation or promotion. The recorded `seeded-history` browser case verifies model readiness, a bounded opening snapshot, and backwards paging without a live Agent or attached Session.
