/** One-shot session-lineage and event-relationship tracing helpers. */

import { foldSurface, isSurfaceEvent, snapshotSessionEvent } from '@deepseek-ai/dsh-session'
import type { SessionEvent, SessionId, SurfaceEvent, SurfaceEventType } from '@deepseek-ai/dsh-session'
import { SessionQueryError } from './config.ts'
import type {
  SessionEventRecord,
  SessionEventTrace,
  SessionLineageNode,
  SessionLineageTrace,
  SessionRecord,
} from './types.ts'

interface EventLogMaps {
  current: Set<number>
  replacedBy: Map<number, number>
  replacedEventSeqs: Map<number, number[]>
  currentSeqs: number[]
}

interface EventLogAnalysis extends EventLogMaps {
  records: SessionEventRecord[]
}

/**
 * Classify a raw event log with one canonical surface fold.
 * @param sessionId - owner of the event log.
 * @param events - detached raw event log.
 * @returns lightweight records in ascending log order.
 */
export function eventRecords(
  sessionId: SessionId,
  events: readonly SessionEvent[],
): SessionEventRecord[] {
  return analyzeEventLog(sessionId, events).records
}

/**
 * Fold and return the current model surface after validating the whole log.
 * @param sessionId - owner used in query diagnostics.
 * @param events - detached raw event log from one corpus observation.
 * @returns detached current surface events in folded order.
 */
export function currentSurfaceEvents(
  sessionId: SessionId,
  events: readonly SessionEvent[],
): SurfaceEvent[] {
  const analysis = analyzeEventLogMaps(events)
  return analysis.currentSeqs.map((seq) => {
    const event = events[seq]
    /* v8 ignore next 6 -- analyzeEventLog validated contiguous seqs and foldSurface returned only surface-event seqs. */
    if (event === undefined || event.seq !== seq || !isSurfaceEvent(event)) {
      throw new SessionQueryError(
        `invalid session surface: current node ${seq} of session "${sessionId}" is not a surface event`,
        'SESSION_QUERY_INVALID_SURFACE',
      )
    }
    return snapshotSessionEvent(event)
  })
}

/**
 * Trace one target after one canonical surface fold and whole-log validation.
 * @param sessionId - owner of the event log.
 * @param events - detached raw event log.
 * @param seq - target event seq.
 * @returns direct surface replacements and relationships to cited source events.
 */
export function traceEvent(
  sessionId: SessionId,
  events: readonly SessionEvent[],
  seq: number,
): SessionEventTrace {
  const target = events[seq]
  if (target === undefined || target.seq !== seq) {
    throw new SessionQueryError(
      `session "${sessionId}" has no event at seq ${seq}`,
      'SESSION_QUERY_EVENT_NOT_FOUND',
    )
  }

  // The lean fold avoids materializing one record object per event: the
  // target record is the only record this trace retains.
  const analysis = analyzeEventLogMaps(events)

  const replacementChain: number[] = []
  let replacement = analysis.replacedBy.get(seq)
  while (replacement !== undefined) {
    replacementChain.push(replacement)
    replacement = analysis.replacedBy.get(replacement)
  }

  const derivedEventSeqs: number[] = []
  for (const event of events) {
    if (event.seq <= seq) continue
    if (eventSources(event).includes(seq)) derivedEventSeqs.push(event.seq)
  }

  const targetRecord: SessionEventRecord = {
    sessionId,
    seq: target.seq,
    type: target.type,
    time: target.time,
    surface: analysis.current.has(target.seq)
      ? 'current'
      : analysis.replacedBy.has(target.seq) ? 'shadowed' : 'log-only',
  }
  const replacedBy = analysis.replacedBy.get(seq)
  return {
    target: targetRecord,
    ...replacedBy === undefined ? {} : { replacedBy },
    replacementChain,
    replacedEventSeqs: analysis.replacedEventSeqs.get(seq) ?? [],
    sourceEventSeqs: [...eventSources(target)],
    derivedEventSeqs,
  }
}

/**
 * Trace one target's known ancestry and recursively known descendants.
 * @param records - complete logical corpus from one observation.
 * @param sessionId - target session id.
 * @returns complete or explicitly partial lineage.
 */
export function traceSession(
  records: readonly SessionRecord[],
  sessionId: SessionId,
): SessionLineageTrace {
  const byId = new Map(records.map(record => [record.header.id, record]))
  const target = byId.get(sessionId)
  if (target === undefined) {
    throw new SessionQueryError(
      `session "${sessionId}" not found`,
      'SESSION_QUERY_SESSION_NOT_FOUND',
    )
  }

  const ancestors: SessionRecord[] = []
  const ancestrySeen = new Set<SessionId>([sessionId])
  let unresolvedParentId: SessionId | undefined
  let parentId = target.header.parentSession
  while (parentId !== undefined) {
    if (ancestrySeen.has(parentId)) {
      throw new SessionQueryError(
        `session lineage contains a cycle at "${parentId}"`,
        'SESSION_QUERY_INVALID_LINEAGE',
      )
    }
    ancestrySeen.add(parentId)
    const parent = byId.get(parentId)
    if (parent === undefined) {
      unresolvedParentId = parentId
      break
    }
    ancestors.push(parent)
    parentId = parent.header.parentSession
  }

  const childrenByParent = new Map<SessionId, SessionRecord[]>()
  for (const record of records) {
    const parent = record.header.parentSession
    if (parent === undefined) continue
    const children = childrenByParent.get(parent) ?? []
    children.push(record)
    childrenByParent.set(parent, children)
  }
  for (const children of childrenByParent.values()) {
    children.sort((a, b) => a.header.createdAt - b.header.createdAt || a.header.id.localeCompare(b.header.id))
  }

  const descendants = buildDescendants(childrenByParent, sessionId)
  const common = {
    target: cloneRecord(target),
    ancestors: ancestors.map(cloneRecord),
    descendants,
  }
  if (unresolvedParentId !== undefined) {
    return { ...common, complete: false, unresolvedParentId }
  }
  return {
    ...common,
    complete: true,
    root: cloneRecord(ancestors.at(-1) ?? target),
  }
}

function analyzeEventLog(
  sessionId: SessionId,
  events: readonly SessionEvent[],
): EventLogAnalysis {
  const maps = analyzeEventLogMaps(events)
  return {
    records: events.map(event => recordFor(event, sessionId, maps)),
    ...maps,
  }
}

/** Classify one event's folded surface against precomputed maps. */
function recordFor(
  event: SessionEvent,
  sessionId: SessionId,
  maps: EventLogMaps,
): SessionEventRecord {
  return {
    sessionId,
    seq: event.seq,
    type: event.type,
    time: event.time,
    surface: maps.current.has(event.seq)
      ? 'current'
      : maps.replacedBy.has(event.seq) ? 'shadowed' : 'log-only',
  }
}

/**
 * Fold one log into its relationship maps without a per-event record array,
 * so traces and surface reads stay linear in allocation-free light state.
 */
function analyzeEventLogMaps(events: readonly SessionEvent[]): EventLogMaps {
  let folded: ReturnType<typeof foldSurface>
  try {
    folded = foldSurface(events)
  } catch (error: unknown) {
    throw new SessionQueryError(
      /* v8 ignore next -- foldSurface throws Error instances */
      `invalid session surface: ${error instanceof Error ? error.message : 'unknown error'}`,
      'SESSION_QUERY_INVALID_SURFACE',
      { cause: error },
    )
  }
  const current = new Set(folded.nodes)
  const replacedBy = new Map<number, number>()
  const replacedEventSeqs = new Map<number, number[]>()
  for (const replacement of folded.replacements) {
    const removed = replacement.shadowedSeqs
    replacedEventSeqs.set(replacement.seq, removed)
    for (const removedSeq of removed) {
      replacedBy.set(removedSeq, replacement.seq)
    }
  }
  return {
    current,
    replacedBy,
    replacedEventSeqs,
    currentSeqs: [...folded.nodes],
  }
}

function eventSources(event: SessionEvent): readonly number[] {
  return (event as SessionEvent<SurfaceEventType>).sourceEventSeqs ?? []
}

function buildDescendants(
  childrenByParent: ReadonlyMap<SessionId, readonly SessionRecord[]>,
  sessionId: SessionId,
): SessionLineageNode[] {
  const descendants: SessionLineageNode[] = []
  const stack = [{ sessionId, descendants }]
  while (stack.length > 0) {
    // The length guard proves a frame exists.
    // oxlint-disable-next-line typescript/no-non-null-assertion
    const frame = stack.pop()!
    const nodes: SessionLineageNode[] = []
    for (const child of childrenByParent.get(frame.sessionId) ?? []) {
      const node = { session: cloneRecord(child), descendants: [] }
      nodes.push(node)
      frame.descendants.push(node)
    }
    for (let index = nodes.length - 1; index >= 0; index -= 1) {
      // The loop bounds prove this indexed node exists.
      // oxlint-disable-next-line typescript/no-non-null-assertion
      const node = nodes[index]!
      stack.push({ sessionId: node.session.header.id, descendants: node.descendants })
    }
  }
  return descendants
}

function cloneRecord(record: SessionRecord): SessionRecord {
  return { ...record, header: structuredClone(record.header) }
}
