---
kind: upgrade-guide
description: "SQLite session search ANDs literal terms and restores CJK character and bigram recall instead of whole-query phrase matching."
---
# Session search uses literal terms and CJK tokens

English | [中文](guide.zh.md)

## Change

SQLite session search previously quoted the entire query as one phrase. It now requires every whitespace-separated literal term, without requiring term adjacency. CJK runs contribute character and bigram tokens, so Chinese and Japanese substrings can match within longer text. Multiple bigrams need not be adjacent; search results are candidates for exact substring checks. FTS operators remain inert data. Non-CJK tokens still require whole-token matches.

## Migration

1. Use `ctx.sessionQuery.filterEvents()` with a `text` clause when the caller requires exact whitespace-flexible substring matching. Review callers that relied on whole-query phrase adjacency.
2. Let the SQLite provider rebuild its disposable derived index when its schema differs. Keep Session logs unchanged. CJK token expansion increases derived index size.
3. Confirm that searching `内存` finds a Session containing `内存溢出`, and that `AI` still does not match `BRAID`. See the [backend search behavior](../../../../packages/session-query/session-query-sqlite/README.md).
