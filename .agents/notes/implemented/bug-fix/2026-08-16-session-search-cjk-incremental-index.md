# Agent Note: Incremental session-search indexing with CJK substring support

Status: implemented

English | [中文](2026-08-16-session-search-cjk-incremental-index.zh.md)

## Problem

`dsh web` crashed with V8 heap exhaustion, and workspace search returned no results for Chinese content. Every `searchSessions`/`searchEvents` call re-observed the complete corpus: it `structuredClone`d every event of every live session, built one search document per event, recomputed a whole-log fingerprint, and re-inserted every document of any changed session. Long-running sessions therefore made each search allocate memory proportional to the entire corpus, and one giant matched document could materialize multi-hundred-megabyte strings (the `Array.from` snippet path and `JSON.stringify` fingerprint path) on top of an already full heap. Separately, FTS5 `unicode61` treats consecutive CJK characters as one token and the backend quoted the whole query as a single phrase, so multi-term and Chinese queries (e.g. `内存` inside `内存溢出`) matched nothing.

## Decision

The [SQLite session-query provider](../../../../packages/session-query/session-query-sqlite/README.md) owns extraction, reconciliation, and schema safety. This note records literal-AND queries, CJK substring indexing, and bounded query retention.

The SQLite backend observes and indexes incrementally. Each live `Session` owns a WeakMap cache holding an incremental SHA-256 stream, the cached surface fold, and the fingerprint and event seq last written; events are deep-frozen and the public snapshot array is replaced on append, so the stream only ever hashes events appended since the last observation and fingerprints need no clones. Reconcile writes only documents appended since the last indexed seq, updates only newly shadowed older documents (`surface = 'shadowed'`, chunked `IN` lists), and applies cache bookkeeping only after the transaction commits. Persisted logs are read through persistence handles without a second event clone, and documents are inserted at write time; each loaded source is released after its rows are written. Stable observation still retains all changed cold sources until the transaction starts, so rebuilding a large cold corpus can require substantial memory. The inherited observation cache separately limits retained Sessions with `preparedSessionCacheSize` and logical events with `preparedSessionCacheMaxEvents`; active leases remain pinned. The query SQL windows `highlight()` output around the first match marker with `instr`/`substr`, so per-row JavaScript memory is bounded by the snippet window instead of the full document, and `makeSnippet` works on that bounded window.

CJK text is searchable at any length: each CJK run is stored as a unigram+bigram token stream separated by zero-width spaces (schema version 10 resets earlier derived indexes in place), and queries expand their CJK runs into the same bigrams (or the lone unigram). Whitespace-separated query terms are ANDed as individually quoted literals, so caller MATCH syntax stays inert data and adjacency is no longer required. Snippets decode the token stream back into readable text: a complete run is read from its unigram prefix, and a window fragment is anchored at the first marked token so giant-run windows still show exact characters from the match on. The keyless `session-query-cjk` snapshot searches fixed-time Session evidence through the shipped headless profile and pins the returned original Chinese text.

The stability gate that re-runs reconciliation when snapshot listings change no longer treats live-owned revision churn as instability: an attached session's write-behind flush updates its persisted file continuously, and its indexed rows are shadowed by the TEMP overlay anyway, so comparing those revisions retried into the next flush forever and starved every search on a corpus whose rebuild outlasts the flush interval. Non-live snapshot churn still retries.

Inherited batch title reads default to one concurrent persisted read (`persistedReadConcurrency` defaults to 1), reducing simultaneous cold-log retention compared with four concurrent reads. Cold observations retain immutable cuts through leases and evict oversized unpinned entries after use. Full-log exact reads such as `readSession` and `listEvents` retain the upstream detached-copy semantics and still allocate complete outputs; the incremental search index does not change those guarantees.

## Alternatives considered

**FTS5 `trigram` tokenizer.** Rejected because queries shorter than three characters — both Chinese words like `内存` and latin tokens like `AI` — silently match nothing.

**External-content or contentless FTS tables with a separate original-text table.** Rejected because `highlight()` positioning is lost or unavailable, requiring a second synced table and hand-rolled snippet matching.

**Keep full per-search rebuilds and only fix the fingerprint stringification.** Rejected because observation, cloning, and re-insertion would still cost O(corpus) per search while a long session is active, leaving the OOM intact.

**Reuse the session's internal incremental `SurfaceManager`.** Rejected because the engine only sees the public session API; replaying `foldSurface` per changed snapshot is cheap and stays on exported contracts.

## Consequences

Every recognized earlier derived index resets once on the next open; the derived index is disposable by design. Indexed CJK text roughly doubles in bytes and `match_count` counts matched bigrams, which inflates multi-character CJK match counts relative to latin tokens. Degenerate single-run CJK documents lose exact pre-match context in snippets (the snippet anchors at the match) rather than memory bounds. Latin substring behavior is unchanged (`AI` still does not match `BRAID`); the keyless snapshot and both READMEs document the new AND and CJK semantics. Batch title reads over a large corpus are now serial, so a cold-title pass over many giant logs takes longer than the old four-way parallel pass; deployments may raise `persistedReadConcurrency` when their logs are small enough. Exact reads that return whole logs by contract (`readSession`, `listEvents`) still allocate their full output, including the detached event copies their APIs return.
