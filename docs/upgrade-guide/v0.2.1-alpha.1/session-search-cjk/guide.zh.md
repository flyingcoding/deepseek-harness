---
kind: upgrade-guide
description: "SQLite 会话搜索对字面关键词执行 AND 匹配，并恢复 CJK 单字与双字召回，替代整条查询的短语匹配。"
---
# 会话搜索使用字面关键词与 CJK token

[English](guide.md) | 中文

## 变更

SQLite 会话搜索此前把整条查询引成一个短语。现在要求每个空白分隔的字面关键词都出现，但不要求关键词相邻。CJK 连续文本贡献单字与双字 token，因此中文和日文子串可匹配更长文本。多个双字 token 不要求相邻；搜索结果是精确子串验证的候选。FTS 操作符仍视为惰性数据。非 CJK token 仍要求完整词匹配。

## 迁移

1. 调用方要求精确且空白灵活的子串匹配时，使用带 `text` 子句的 `ctx.sessionQuery.filterEvents()`。检查依赖整条查询短语相邻性的调用方。
2. SQLite provider 在 schema 不同时重建可丢弃的派生索引。保持 Session 日志不变。CJK token 扩展会增加派生索引体积。
3. 确认搜索 `内存` 能找到包含 `内存溢出` 的 Session，而 `AI` 仍不会匹配 `BRAID`。参见[后端搜索行为](../../../../packages/session-query/session-query-sqlite/README.zh.md)。
