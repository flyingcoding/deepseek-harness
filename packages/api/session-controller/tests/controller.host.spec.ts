import { Context } from '@deepseek-ai/cordis'
import AgentRegistry from '@deepseek-ai/dsh-agent'
import type { Agent } from '@deepseek-ai/dsh-agent'
import { createUserMessage } from '@deepseek-ai/dsh-llm'
import SessionStore, { SessionId } from '@deepseek-ai/dsh-session'
import type { SessionEvent, SessionHeader } from '@deepseek-ai/dsh-session'
import { describe, expect, it, vi } from 'vitest'
import SessionController from '../src/index.ts'
import type { ApiSessionAgentController } from '../src/agent.ts'
import { createSessionTestController, testSessionPersistence } from './test-remote.ts'

const defaults = {
  defaultModelSelection: () => ({ provider: 'fixture', model: 'fixture-model' }),
  cwd: '/tmp',
}

describe('SessionController facade', () => {
  it('does not require the Tools service', () => {
    expect(SessionController.inject).not.toContain('tools')
  })

  it('owns Host service methods and publishes Agent lifecycle projections', async () => {
    const ctx = new Context()
    await ctx.plugin(SessionStore)
    await ctx.plugin(AgentRegistry)
    const sessionId = SessionId('controller-session')
    const header: SessionHeader = {
      version: 0,
      id: sessionId,
      createdAt: 1,
      cwd: '/workspace',
    }
    const events: SessionEvent[] = []
    const inspect = vi.fn(() => Promise.resolve({ meta: header, events }))
    ctx.provide('sessionPersistence', testSessionPersistence(ctx, {
      list: () => Promise.resolve([header]),
      inspect,
    }) as never)
    const controller = createSessionTestController(ctx, defaults)
    const status = vi.fn()
    const failure = vi.fn()
    const activity = vi.fn()
    ctx.on('api-session/status', status)
    ctx.on('api-session/error', failure)
    ctx.on('api-session/activity', activity)

    await expect(controller.inspect(sessionId)).resolves.toEqual({ meta: header, events })
    expect(inspect).toHaveBeenCalledOnce()

    const session = ctx.sessions.create(sessionId, { meta: header })
    const agent = {
      id: sessionId,
      session,
      status: 'idle',
      ctx,
    } as Agent
    ctx.agents.register(agent)
    const consumeSelection = vi.spyOn(
      (controller as unknown as { agents: ApiSessionAgentController }).agents,
      'consumeSelection',
    )

    await expect(controller.resolveAgent(sessionId)).resolves.toEqual({ agent })
    await expect(controller.inspect(sessionId)).resolves.toEqual({ meta: header, events })
    expect(inspect).toHaveBeenCalledOnce()
    ctx.emit('agent/status', { agent, status: 'running' })
    ctx.emit('agent/error', { agent, turn: 1, step: 0, error: new Error('fixture failure') })
    session.append('user/message', createUserMessage({
      content: [{ type: 'text', text: 'hello' }],
      source: { kind: 'user' },
    }), { surfaceOp: 'append' })
    expect(status).toHaveBeenCalledWith(sessionId, true)
    expect(failure).toHaveBeenCalledWith(sessionId, expect.stringContaining('fixture failure'))
    expect(activity).toHaveBeenCalledWith(sessionId, expect.any(Number))
    session.append('request/header', {
      header: { config: { provider: 'fixture', model: 'fixture-model' } },
      reason: 'initial',
    })
    expect(consumeSelection).toHaveBeenCalledWith(
      agent, 'fixture', 'fixture-model', undefined,
    )
    const unowned = ctx.sessions.create(SessionId('controller-unowned'), {
      meta: { cwd: '/workspace' },
    })
    unowned.append('request/header', {
      header: { config: { provider: 'fixture', model: 'other-model' } },
      reason: 'initial',
    })
    expect(consumeSelection).toHaveBeenCalledTimes(1)

    const abort = new AbortController()
    const iterator = controller.follow({
      address: { kind: 'session', sessionId },
    }, abort.signal)[Symbol.asyncIterator]()
    await expect(iterator.next()).resolves.toMatchObject({
      done: false,
      value: { type: 'snapshot', cursor: 1 },
    })
    abort.abort()
    await expect(iterator.next()).resolves.toEqual({ done: true, value: undefined })
  })

  it('keeps a cold history follower inspection-only until an explicit Agent operation', async () => {
    const ctx = new Context()
    await ctx.plugin(SessionStore)
    await ctx.plugin(AgentRegistry)
    const sessionId = SessionId('cold-follow')
    const header: SessionHeader = {
      version: 0, id: sessionId, createdAt: 1, cwd: '/workspace',
    }
    ctx.provide('sessionPersistence', testSessionPersistence(ctx, {
      list: () => Promise.resolve([header]),
      inspect: () => Promise.resolve({ meta: header, events: [] }),
    }) as never)
    const controller = createSessionTestController(ctx, defaults)
    const agents = (controller as unknown as { agents: ApiSessionAgentController }).agents
    const resolveObserved = vi.spyOn(agents, 'resolveObservedAgent')
    const resolve = vi.spyOn(agents, 'resolveAgent')
    const abort = new AbortController()
    const iterator = controller.follow({
      address: { kind: 'session', sessionId },
    }, abort.signal)[Symbol.asyncIterator]()

    await expect(iterator.next()).resolves.toMatchObject({ value: { type: 'snapshot' } })
    const waiting = iterator.next()
    await Promise.resolve()
    expect(resolveObserved).not.toHaveBeenCalled()
    expect(resolve).not.toHaveBeenCalled()
    abort.abort()
    await expect(waiting).resolves.toMatchObject({ done: true })
    await ctx.fiber.dispose()
  })
})
