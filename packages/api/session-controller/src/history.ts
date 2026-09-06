/** Cold Session history pagination and live-event source. */

import type { Context } from '@deepseek-ai/cordis'
import { Deque } from '@deepseek-ai/dsh-deque'
import type { AssistantStreamFrame } from '@deepseek-ai/dsh-agent'
import {
  isAppendSurfaceEvent,
  SessionLogOffset,
  SessionSeq,
} from '@deepseek-ai/dsh-session'
import type {
  SessionEvent,
  SessionHeader,
  SessionId,
  SessionLogOffset as SessionLogOffsetType,
  SessionSeqCursor,
} from '@deepseek-ai/dsh-session'
import { SessionQueryError, type SessionObservation } from '@deepseek-ai/dsh-session-query'
import type { SessionPersistenceWindow } from '@deepseek-ai/dsh-session-persistence'
import type { ProjectionSnapshot } from '@deepseek-ai/dsh-session-projection'
import type {} from '@deepseek-ai/dsh-session-projection-cache'
import type {} from '@deepseek-ai/dsh-subagent'
import { RemoteError } from '@deepseek-ai/dsh-typert-protocol'
import type { JsonValue } from '@deepseek-ai/dsh-util-values'
import type {
  SessionAddress,
  SessionAssistantStreamFrame,
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
import { SessionAssistantStreamAccumulator } from './assistant-stream.ts'

const DEFAULT_MAX_MESSAGES = 50
/** Default logical-event ceiling for one history page or follow opening. */
export const DEFAULT_HISTORY_PAGE_MAX_EVENTS = 20_000
const MESSAGE_TYPES = new Set(['user/message', 'assistant/message'])

interface ColdHistoryWindow extends Disposable {
  readonly source: 'window'
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
  private readonly assistantStreams = new Map<SessionId, SessionAssistantStreamAccumulator>()

  /**
   * @param ctx - Host context carrying Session query and projection services.
   * @param promote - starts ordinary Session activation after snapshot delivery.
   * @param maxPageEvents - maximum logical events retained by a history response.
   */
  constructor(
    private readonly ctx: Context,
    private readonly promote: (observation: SessionObservation) => void,
    private readonly maxPageEvents: number = DEFAULT_HISTORY_PAGE_MAX_EVENTS,
  ) {
    if (!Number.isSafeInteger(maxPageEvents) || maxPageEvents < 1) {
      throw new TypeError('maxPageEvents must be a positive safe integer')
    }
    ctx.on('agent/assistant-stream', ({ agent, frame }) => {
      let stream = this.assistantStreams.get(agent.session.id)
      if (stream === undefined) {
        stream = new SessionAssistantStreamAccumulator()
        this.assistantStreams.set(agent.session.id, stream)
      }
      stream.accept(frame, cursorBeforeNext(agent.session.seq))
    }, { global: true })
    ctx.on('agent/disposed', ({ agent }) => {
      this.assistantStreams.delete(agent.session.id)
    }, { global: true })
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
    const window = await this.coldWindow(
      request.address,
      SessionLogOffset(Math.min(beforeSeq ?? throughSeq + 1, throughSeq + 1)),
      signal,
      false,
    )
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
   * @returns a complete opening snapshot followed by gap-free durable events and opted-in assistant frames.
   */
  async *follow(request: SessionFollowRequest, signal: AbortSignal): AsyncIterable<SessionFollowFrame> {
    validateFollowRequest(request)
    const { address } = request
    const target = addressId(address)
    const buffered = new Deque<
      | { readonly type: 'event'; readonly event: SessionEvent }
      | {
        readonly type: 'assistant-stream'
        readonly frame: SessionAssistantStreamFrame
        readonly ordinal: number
      }
    >()
    let snapshotCursor: SessionSeqCursor | undefined
    let assistantStreamOrdinal = 0
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
      buffered.pushBack({ type: 'event', event })
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
        buffered.pushFront({ type: 'event', event: suffix[index] as SessionEvent })
      }
      notify()
    }, { global: true })
    const disposeAssistantStream = request.assistantStream !== true
      ? undefined
      : this.ctx.on('agent/assistant-stream', ({ agent, frame }) => {
        if (agent.session.id !== target) return
        buffered.pushBack({
          type: 'assistant-stream',
          frame: wireAssistantStreamFrame(frame, cursorBeforeNext(agent.session.seq)),
          ordinal: ++assistantStreamOrdinal,
        })
        notify()
      }, { global: true })
    const onAbort = (): void => { notify() }
    signal.addEventListener('abort', onAbort, { once: true })
    try {
      using source = await this.coldWindow(address, undefined, signal, true)
        ?? await this.sourceFor(address, signal, true)
      const events = source.events
      signal.throwIfAborted()
      const cursor = source.cursor
      snapshotCursor = cursor
      const page = paginate(events, undefined, request.maxMessages ?? DEFAULT_MAX_MESSAGES, cursor, this.maxPageEvents)
      const assistantStream = request.assistantStream === true
        ? this.assistantStreams.get(target)?.snapshot() ?? { revision: 0 }
        : undefined
      // The accumulator snapshot and this watermark are synchronous. Frames
      // through the cut are represented or superseded by that baseline,
      // including larger revisions from a retired Agent; later revision
      // resets reach Client continuity validation.
      const assistantStreamOrdinalCut = assistantStreamOrdinal
      yield {
        type: 'snapshot',
        header: wireHeader(source.header),
        cursor,
        records: pageRecords(page.events),
        hasMore: page.hasMore || source.source === 'window' && source.hasMore,
        projections: source.projections === undefined
          ? { asOfSeq: source.source === 'window' ? -1 : cursor, values: {} }
          : projectionBlock(source.projections),
        ...assistantStream === undefined ? {} : { assistantStream },
      }
      if (address.kind === 'session' && source.source === 'prepared') {
        const promotion = source.retain()
        try {
          this.promote(promotion)
        } catch (error: unknown) {
          promotion[Symbol.dispose]()
          throw error
        }
      }
      let nextOffset = SessionLogOffset(cursor + 1)
      while (!follower.closed && !signal.aborted) {
        const item = buffered.popFront()
        if (item === undefined) {
          await new Promise<void>((resolve) => { wake = resolve })
          continue
        }
        if (item.type === 'assistant-stream') {
          if (item.ordinal > assistantStreamOrdinalCut) {
            yield { type: 'assistant-stream', frame: item.frame }
          }
          continue
        }
        const expectedSeq = SessionSeq(nextOffset)
        if (item.event.seq < expectedSeq) continue
        if (item.event.seq !== expectedSeq) {
          throw new RemoteError('gateway/internal', `session event stream skipped seq ${String(expectedSeq)}`, {})
        }
        nextOffset = SessionLogOffset(nextOffset + 1)
        yield entryFor(item.event)
      }
    } finally {
      this.closeFollowers.delete(close)
      signal.removeEventListener('abort', onAbort)
      disposeCreated()
      disposeEvent()
      disposeAssistantStream?.()
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
    if (persistence === undefined) return undefined
    try {
      let projectionReader: WindowProjectionReader | undefined
      const registry = this.ctx.get('sessionProjections')
      const window = await persistence.readWindow(sessionId, beforeSeq, this.maxPageEvents, signal, (window) => {
        if (registry === undefined || !(withProjections || address.kind === 'subagent')) return undefined
        if (withProjections && !window.hasMore) return undefined
        const reader = createWindowProjectionReader(registry, window, this.maxPageEvents)
        projectionReader = reader
        return (event) => { reader.accept(event) }
      })
      signal.throwIfAborted()
      if (this.ctx.sessions.get(sessionId) !== undefined) return undefined
      if (withProjections && !window.hasMore) return undefined
      if (beforeSeq === undefined && window.cursor >= 0 && window.events.at(-1)?.type !== 'turn/end') return undefined
      const projections = projectionReader?.snapshot() ?? (withProjections || address.kind === 'subagent'
        ? this.ctx.get('sessionProjectionCache')?.cachedSnapshot(window.meta, window.inheritedEventCount)
        : undefined)
      if (window.meta.cwd === undefined) rejectNotFound(address)
      if (address.kind === 'subagent' && projections === undefined) return undefined
      validateAddress(address, window.meta, window.inheritedEventCount, projections)
      return {
        source: 'window',
        [Symbol.dispose]: () => {},
        header: window.meta,
        inheritedEventCount: window.inheritedEventCount,
        events: window.events,
        cursor: window.cursor,
        hasMore: window.hasMore,
        ...projections === undefined ? {} : { projections },
      }
    } catch (error: unknown) {
      signal.throwIfAborted()
      if ((error as { name?: unknown } | null)?.name === 'SessionPersistenceNotFoundError') rejectNotFound(address)
      if (error instanceof SessionQueryError && error.code === 'SESSION_QUERY_SESSION_NOT_FOUND') rejectNotFound(address)
      if (error instanceof SessionQueryError || error instanceof RemoteError) throw error
      const corrupt = error instanceof Error
        && (error.name === 'SessionPersistenceCorruptionError' || error.name === 'SessionFormatUnsupportedError')
      throw new SessionQueryError(
        `failed to read session "${sessionId}": ${String(error)}`,
        corrupt ? 'SESSION_QUERY_CORRUPT_SESSION' : 'SESSION_QUERY_PERSISTENCE_FAILED',
        { cause: error },
      )
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

interface WindowProjectionReader {
  accept(event: SessionEvent): void
  snapshot(): ProjectionSnapshot
}

/** Fold the complete stored prefix in bounded batches without constructing a Session. */
function createWindowProjectionReader(
  registry: Context['sessionProjections'],
  window: SessionPersistenceWindow,
  maxEvents: number,
): WindowProjectionReader {
  let restored = registry.restore({}, [], SessionLogOffset(0), window.meta, window.inheritedEventCount)
  let pending: SessionEvent[] = []
  /** Include the pending contiguous batch before exposing its projection cut. */
  const snapshot = (): ProjectionSnapshot => {
    if (pending.length > 0) {
      restored = registry.restore(
        restored.checkpoint,
        pending,
        SessionLogOffset((pending[0] as SessionEvent).seq),
        window.meta,
        window.inheritedEventCount,
      )
      pending = []
    }
    return restored.snapshot
  }
  return {
    /** Bound retained input independently from each projection's accumulated state. */
    accept(event) {
      pending.push(event)
      if (pending.length >= maxEvents) snapshot()
    },
    snapshot,
  }
}

function cursorBeforeNext(nextSeq: SessionLogOffsetType): SessionSeqCursor {
  return nextSeq === 0 ? -1 : SessionSeq(nextSeq - 1)
}

function wireAssistantStreamFrame(
  frame: AssistantStreamFrame,
  durableCursor: SessionSeqCursor,
): SessionAssistantStreamFrame {
  if (frame.type === 'start') return { ...frame, startedAfterSeq: durableCursor }
  if (frame.type === 'end') return frame
  return {
    ...frame,
    chunk: frame.chunk as JsonValue,
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

/** Translate current logical Session metadata to the browser wire. */
function wireHeader(header: SessionHeader): SessionWireHeader {
  return { ...header }
}

function entryFor(event: SessionEvent): SessionEventEntry {
  return {
    type: 'event',
    // Session.append validates and freezes event data as JSON before publication.
    event: event as unknown as SessionWireEvent,
  }
}

/** Encode one bounded logical page without changing its pagination cut. */
function pageRecords(events: readonly SessionEvent[]): SessionHistoryRecord[] {
  return events.map(entryFor)
}
