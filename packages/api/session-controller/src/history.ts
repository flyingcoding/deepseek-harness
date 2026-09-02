/** Cold Session history pagination and live-event source. */

import type { Context } from '@deepseek-ai/cordis'
import { Deque } from '@deepseek-ai/dsh-deque'
import {
  isAppendSurfaceEvent,
  SessionLogOffset,
  SessionSeq,
} from '@deepseek-ai/dsh-session'
import { isChunkRow, packChunkRuns, type ChunkRow } from '@deepseek-ai/dsh-session/chunk-rows'
import type {
  SessionEvent,
  SessionHeader,
  SessionId,
  SessionLogOffset as SessionLogOffsetType,
  SessionSeqCursor,
} from '@deepseek-ai/dsh-session'
import { SessionQueryError, type SessionObservation } from '@deepseek-ai/dsh-session-query'
import type {} from '@deepseek-ai/dsh-session-projection-cache'
import type {} from '@deepseek-ai/dsh-subagent'
import { RemoteError } from '@deepseek-ai/dsh-typert-protocol'
import type {
  SessionAddress,
  SessionChunkRun,
  SessionEventEntry,
  SessionFollowRequest,
  SessionFollowFrame,
  SessionHistoryRecord,
  SessionPage,
  SessionPageRequest,
  SessionProjectionBaseline,
  SessionProjectionValues,
  SessionWireHeader,
  SessionWireEvent,
} from './types.ts'

const DEFAULT_MAX_MESSAGES = 50
/** Default logical-event ceiling for one history page or follow opening. */
export const DEFAULT_HISTORY_PAGE_MAX_EVENTS = 20_000
const MESSAGE_TYPES = new Set(['user/message', 'assistant/message'])

interface ColdHistoryWindow {
  readonly header: SessionHeader
  readonly inheritedEventCount: SessionLogOffsetType
  readonly events: readonly SessionEvent[]
  readonly cursor: SessionSeqCursor
  readonly hasMore: boolean
  readonly projections?: NonNullable<SessionObservation['projections']>
}

/** Implements cold-safe history operations delegated by the Session Controller. */
export class SessionHistoryController {
  private readonly closeFollowers = new Set<() => void>()
  private readonly maxPageEvents: number

  /**
   * @param ctx - Host context carrying Session query and projection services.
   * @param maxPageEvents - maximum logical events retained in one history response.
   */
  constructor(
    private readonly ctx: Context,
    maxPageEventsOrLegacyPromote: number | ((observation: SessionObservation) => void) = DEFAULT_HISTORY_PAGE_MAX_EVENTS,
  ) {
    const maxPageEvents = typeof maxPageEventsOrLegacyPromote === 'number'
      ? maxPageEventsOrLegacyPromote
      : DEFAULT_HISTORY_PAGE_MAX_EVENTS
    if (!Number.isSafeInteger(maxPageEvents) || maxPageEvents < 1) {
      throw new TypeError('maxPageEvents must be a positive safe integer')
    }
    this.maxPageEvents = maxPageEvents
    ctx.effect(() => () => {
      for (const close of this.closeFollowers) close()
      this.closeFollowers.clear()
    }, 'session-controller.history')
  }

  /**
   * Read one message-aligned history page without activating an Agent.
   * @param request - durable address and backwards-page cursor.
   * @param signal - caller cancellation for persistence reads.
   * @returns a contiguous event page.
   */
  async page(request: SessionPageRequest, signal: AbortSignal): Promise<SessionPage> {
    validatePageRequest(request)
    const throughSeq: SessionSeqCursor = request.throughSeq === -1
      ? -1
      : SessionSeq(request.throughSeq)
    const beforeSeq = request.beforeSeq === undefined
      ? undefined
      : SessionLogOffset(request.beforeSeq)
    const window = await this.coldWindow(request.address, beforeSeq ?? SessionLogOffset(throughSeq + 1), signal, false)
    if (window !== undefined) {
      if (throughSeq > window.cursor) {
        throw new RemoteError(
          'gateway/bad-request',
          `session page through seq ${String(throughSeq)} is past cursor ${String(window.cursor)}`,
          {},
        )
      }
      const page = paginate(
        window.events,
        undefined,
        request.maxMessages ?? DEFAULT_MAX_MESSAGES,
        throughSeq,
        this.maxPageEvents,
      )
      return { records: pageRecords(page.events), hasMore: window.hasMore || page.hasMore }
    }
    using source = await this.sourceFor(request.address, signal, false)
    signal.throwIfAborted()
    const sourceLog = source.events
    const sourceCursor: SessionSeqCursor = sourceLog.at(-1)?.seq ?? -1
    if (throughSeq > sourceCursor) {
      throw new RemoteError(
        'gateway/bad-request',
        `session page through seq ${String(throughSeq)} is past cursor ${String(sourceCursor)}`,
        {},
      )
    }
    /* v8 ignore next -- Session and persistence validation guarantee a dense zero-based event prefix. */
    if (throughSeq >= 0 && sourceLog[throughSeq]?.seq !== throughSeq) {
      throw new RemoteError('gateway/internal', `session log does not contain through seq ${String(throughSeq)}`, {})
    }
    const page = paginate(
      sourceLog,
      beforeSeq,
      request.maxMessages ?? DEFAULT_MAX_MESSAGES,
      throughSeq,
      this.maxPageEvents,
    )
    const records = pageRecords(page.events)
    return {
      records,
      hasMore: page.hasMore,
    }
  }

  /**
   * Follow events appended after an initial cursor on one durable address.
   * @param request - durable address and last committed sequence already held by the caller.
   * @param signal - stream cancellation owned by the Remote carrier.
   * @returns a complete opening snapshot followed by gap-free event frames.
   */
  async *follow(request: SessionFollowRequest, signal: AbortSignal): AsyncIterable<SessionFollowFrame> {
    validateFollowRequest(request)
    const { address } = request
    const target = addressId(address)
    const buffered = new Deque<SessionEvent>()
    let snapshotCursor: SessionSeqCursor | undefined
    let wake: (() => void) | undefined
    const notify = (): void => {
      const resume = wake
      wake = undefined
      resume?.()
    }
    const follower = { closed: false }
    const close = (): void => {
      follower.closed = true
      notify()
    }
    this.closeFollowers.add(close)
    const disposeEvent = this.ctx.on('session/event', (session, event) => {
      if (session.id !== target) return
      buffered.pushBack(event)
      notify()
    }, { global: true })
    const disposeCreated = this.ctx.on('session/created', (session) => {
      if (session.id !== target) return
      // Constructor seed events have no session/event notification. Normally
      // only the end-seed suffix is new; if persistence advanced after the
      // opening observation, replay everything beyond that snapshot cursor.
      const suffix = session.snapshotEvents(snapshotCursor === undefined
        ? session.firstLiveSeq
        : SessionLogOffset(snapshotCursor + 1))
      for (let index = suffix.length - 1; index >= 0; index -= 1) {
        buffered.pushFront(suffix[index] as SessionEvent)
      }
      notify()
    }, { global: true })
    const onAbort = (): void => { notify() }
    signal.addEventListener('abort', onAbort, { once: true })
    try {
      const opening = await this.openingSnapshot(request, signal)
      snapshotCursor = opening.cursor === -1 ? -1 : SessionSeq(opening.cursor)
      yield opening
      let nextOffset = SessionLogOffset(opening.cursor + 1)
      while (!follower.closed && !signal.aborted) {
        const item = buffered.popFront()
        if (item === undefined) {
          await new Promise<void>((resolve) => { wake = resolve })
          continue
        }
        const expectedSeq = SessionSeq(nextOffset)
        if (item.seq < expectedSeq) continue
        if (item.seq !== expectedSeq) {
          throw new RemoteError('gateway/internal', `session event stream skipped seq ${String(expectedSeq)}`, {})
        }
        nextOffset = SessionLogOffset(nextOffset + 1)
        yield entryFor(item)
      }
    } finally {
      this.closeFollowers.delete(close)
      signal.removeEventListener('abort', onAbort)
      disposeCreated()
      disposeEvent()
    }
  }

  /** Build a bounded opening frame before the long-lived follower waits. */
  private async openingSnapshot(
    request: SessionFollowRequest,
    signal: AbortSignal,
  ): Promise<Extract<SessionFollowFrame, { type: 'snapshot' }>> {
    const window = await this.coldWindow(request.address, undefined, signal, true)
    if (window !== undefined) {
      const page = paginate(
        window.events,
        undefined,
        request.maxMessages ?? DEFAULT_MAX_MESSAGES,
        window.cursor,
        this.maxPageEvents,
      )
      return {
        type: 'snapshot',
        header: wireHeader(window.header, window.inheritedEventCount),
        cursor: window.cursor,
        records: pageRecords(page.events),
        hasMore: window.hasMore || page.hasMore,
        projections: window.projections === undefined
          ? { asOfSeq: -1, values: {} }
          : projectionBlock(window.projections),
      }
    }
    using source = await this.sourceFor(request.address, signal, true)
    signal.throwIfAborted()
    const page = paginate(
      source.events,
      undefined,
      request.maxMessages ?? DEFAULT_MAX_MESSAGES,
      source.cursor,
      this.maxPageEvents,
    )
    return {
      type: 'snapshot',
      header: wireHeader(source.header, source.inheritedEventCount),
      cursor: source.cursor,
      records: pageRecords(page.events),
      hasMore: page.hasMore,
      projections: source.projections === undefined
        ? { asOfSeq: source.cursor, values: {} }
        : projectionBlock(source.projections),
    }
  }

  /** Read a cold bounded window directly from persistence when authorization permits it. */
  private async coldWindow(
    address: SessionAddress,
    beforeSeq: SessionLogOffsetType | undefined,
    signal: AbortSignal,
    withProjections: boolean,
  ): Promise<ColdHistoryWindow | undefined> {
    const sessionId = addressId(address)
    if (this.ctx.sessions.get(sessionId) !== undefined) return undefined
    const persistence = this.ctx.get('sessionPersistence')
    if (persistence === undefined || typeof persistence.readWindow !== 'function') return undefined
    try {
      const window = await persistence.readWindow(sessionId, beforeSeq, this.maxPageEvents, signal)
      signal.throwIfAborted()
      if (this.ctx.sessions.get(sessionId) !== undefined) return undefined
      if (beforeSeq === undefined && window.cursor >= 0 && window.events.at(-1)?.type !== 'turn/end') return undefined
      const projections = withProjections || address.kind === 'subagent'
        ? this.ctx.get('sessionProjectionCache')?.cachedSnapshot(window.meta, window.inheritedEventCount)
        : undefined
      if (window.meta.cwd === undefined) rejectNotFound(address)
      if (address.kind === 'subagent' && projections === undefined) return undefined
      validateAddress(address, window.meta, window.inheritedEventCount, projections)
      return {
        header: window.meta,
        inheritedEventCount: window.inheritedEventCount,
        events: window.events,
        cursor: window.cursor,
        hasMore: window.hasMore,
        ...projections === undefined ? {} : { projections },
      }
    } catch (error: unknown) {
      if ((error as { name?: unknown } | null)?.name === 'SessionPersistenceNotFoundError') rejectNotFound(address)
      throw error
    }
  }

  private async sourceFor(
    address: SessionAddress,
    signal: AbortSignal,
    withProjections: boolean,
  ): Promise<SessionObservation> {
    const sessionId = addressId(address)
    try {
      const observation = await this.ctx.sessionQuery.observeSession(sessionId, {
        signal,
        projectionMode: withProjections || address.kind === 'subagent' ? 'all' : 'none',
      })
      if (observation.header.cwd === undefined) {
        observation[Symbol.dispose]()
        rejectNotFound(address)
      }
      try {
        validateAddress(
          address,
          observation.header,
          observation.inheritedEventCount,
          observation.projections,
        )
      } catch (error: unknown) {
        observation[Symbol.dispose]()
        throw error
      }
      return observation
    } catch (error: unknown) {
      if (error instanceof SessionQueryError
        && error.code === 'SESSION_QUERY_SESSION_NOT_FOUND') rejectNotFound(address)
      throw error
    }
  }

}

function projectionBlock(
  snapshot: NonNullable<SessionObservation['projections']>,
): SessionProjectionBaseline {
  return {
    asOfSeq: snapshot.asOfSeq,
    // Projection definitions validate whole JSON values before snapshot publication.
    values: snapshot.values as SessionProjectionValues,
  }
}

function validatePageRequest(request: SessionPageRequest): void {
  if (!Number.isSafeInteger(request.throughSeq)
    || request.throughSeq < -1
    || Object.is(request.throughSeq, -0)) {
    throw new RemoteError('gateway/bad-request', 'throughSeq must be an integer greater than or equal to -1', {})
  }
  if (request.beforeSeq !== undefined
    && (!Number.isSafeInteger(request.beforeSeq)
      || request.beforeSeq < 0
      || Object.is(request.beforeSeq, -0))) {
    throw new RemoteError('gateway/bad-request', 'beforeSeq must be a non-negative safe integer', {})
  }
  if (request.maxMessages !== undefined
    && (!Number.isSafeInteger(request.maxMessages) || request.maxMessages <= 0)) {
    throw new RemoteError('gateway/bad-request', 'maxMessages must be a positive safe integer', {})
  }
}

function validateFollowRequest(request: SessionFollowRequest): void {
  if (request.maxMessages !== undefined
    && (!Number.isSafeInteger(request.maxMessages) || request.maxMessages <= 0)) {
    throw new RemoteError('gateway/bad-request', 'maxMessages must be a positive safe integer', {})
  }
}

function addressId(address: SessionAddress): SessionId {
  return address.kind === 'session' ? address.sessionId : address.childSessionId
}

function validateAddress(
  address: SessionAddress,
  header: SessionHeader,
  inheritedEventCount: SessionLogOffsetType,
  projections: SessionObservation['projections'],
): void {
  if (address.kind === 'session') {
    if (header.origin === 'subagent') {
      throw new RemoteError('session/agent-busy', 'subagent Sessions require their durable parent address', {
        reason: 'use subagent delivery for this child session',
      })
    }
    return
  }
  if (header.origin !== 'subagent' || header.parentSession !== address.parentSessionId) {
    throw new RemoteError('subagent/unauthorized', 'subagent does not belong to the supplied parent', {
      childSessionId: address.childSessionId,
    })
  }
  const identity = projections?.values.subagent
  if (identity === null) {
    throw new RemoteError('subagent/catalog-diagnostic', 'subagent descriptor is corrupt', {
      parentSessionId: address.parentSessionId,
      childSessionId: address.childSessionId,
      reason: 'corrupt',
    })
  }
  if (identity === undefined || identity.seq < inheritedEventCount) {
    throw new RemoteError('subagent/catalog-diagnostic', 'subagent descriptor is unavailable', {
      parentSessionId: address.parentSessionId,
      childSessionId: address.childSessionId,
      reason: 'unsupported',
    })
  }
  if (identity.mode !== address.mode) {
    throw new RemoteError('subagent/unauthorized', 'subagent mode does not match the supplied address', {
      childSessionId: address.childSessionId,
    })
  }
}

function rejectNotFound(address: SessionAddress): never {
  if (address.kind === 'session') {
    throw new RemoteError('session/not-found', `session "${address.sessionId}" not found`, { sessionId: address.sessionId })
  }
  throw new RemoteError('subagent/not-found', 'subagent is unavailable', {
    parentSessionId: address.parentSessionId,
    childSessionId: address.childSessionId,
  })
}

function paginate(
  events: readonly SessionEvent[],
  beforeSeq: SessionLogOffsetType | undefined,
  maxMessages: number,
  throughSeq: SessionSeqCursor = events.at(-1)?.seq ?? -1,
  maxEvents = DEFAULT_HISTORY_PAGE_MAX_EVENTS,
): { readonly events: SessionEvent[]; readonly hasMore: boolean } {
  const firstSeq = events[0]?.seq ?? SessionSeq(0)
  const endSeq = Math.min(throughSeq + 1, beforeSeq ?? throughSeq + 1)
  const end = Math.max(0, Math.min(events.length, endSeq - firstSeq))
  let count = 0
  let cut = 0
  for (let index = end - 1; index >= 0; index--) {
    const event = events[index] as SessionEvent
    if (!MESSAGE_TYPES.has(event.type) || !isAppendSurfaceEvent(event)) continue
    count++
    const sources = event.sourceEventSeqs
    let groupStart = event.seq
    if (sources !== undefined) {
      for (const source of sources) {
        if (source < groupStart) groupStart = source
      }
    }
    if (count >= maxMessages) {
      cut = Math.max(0, groupStart - firstSeq)
      break
    }
  }
  cut = Math.max(cut, end - maxEvents)
  return { events: events.slice(cut, end), hasMore: firstSeq + cut > 0 }
}

/** Translate logical Session metadata to the unchanged v0 browser wire. */
function wireHeader(
  header: SessionHeader,
  inheritedEventCount: SessionLogOffsetType,
): SessionWireHeader {
  const { isSeeded, ...wire } = header
  return {
    ...wire,
    ...isSeeded ? { seedLength: inheritedEventCount } : {},
  }
}

function entryFor(event: SessionEvent): SessionEventEntry {
  return {
    type: 'event',
    // Session.append validates and freezes event data as JSON before publication.
    event: event as unknown as SessionWireEvent,
  }
}

function chunkEntryFor(row: ChunkRow): SessionChunkRun {
  switch (row.type) {
    case 'text-chunks':
      return {
        type: 'chunks',
        event: { type: 'chunkrow/text-chunks', seq: row.seq0, time: row.time0, data: row.data },
      }
    case 'reasoning-chunks':
      return {
        type: 'chunks',
        event: { type: 'chunkrow/reasoning-chunks', seq: row.seq0, time: row.time0, data: row.data },
      }
    case 'tool-call-chunks':
      return {
        type: 'chunks',
        event: { type: 'chunkrow/tool-call-chunks', seq: row.seq0, time: row.time0, data: row.data },
      }
  }
}

/** Encode one bounded logical page without changing its pagination cut. */
function pageRecords(events: readonly SessionEvent[]): SessionHistoryRecord[] {
  return packChunkRuns(events).map(record => isChunkRow(record)
    ? chunkEntryFor(record)
    : entryFor(record))
}
