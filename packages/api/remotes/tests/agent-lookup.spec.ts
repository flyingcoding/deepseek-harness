import { describe, expect, it, vi } from 'vitest'
import { Context } from '@deepseek-ai/cordis'
import AgentRegistry from '@deepseek-ai/dsh-agent'
import type { Agent, AgentHandle, ResumeAgentOptions } from '@deepseek-ai/dsh-agent'
import SessionStore from '@deepseek-ai/dsh-session'
import type { Session, SessionEvent, SessionHeader, SessionId } from '@deepseek-ai/dsh-session'
import { createApiRemoteAgentResolver } from '@deepseek-ai/dsh-api-remotes'
import { TypertLookupFailure } from '@deepseek-ai/dsh-typert-protocol'
import TypertRegistry from '@deepseek-ai/dsh-typert-registry'

const sid = (value: string): SessionId => value as SessionId

function header(id: SessionId): SessionHeader {
  return { version: 0, id, createdAt: 1, cwd: '/proj' }
}

async function createContext(): Promise<Context> {
  const ctx = new Context()
  await ctx.plugin(TypertRegistry)
  await ctx.plugin(SessionStore)
  await ctx.plugin(AgentRegistry)
  return ctx
}

function provideSession(
  ctx: Context,
  meta: SessionHeader,
  inspect: () => Promise<{ meta: SessionHeader; events: SessionEvent[] }> = () => Promise.resolve({ meta, events: [] }),
): void {
  ctx.provide('sessionPersistence', {
    list: () => Promise.resolve([meta]),
    inspect,
    locate: () => undefined,
  } as never)
}

function stubAgent(ctx: Context, session: Session): Agent {
  return { id: session.id, session, status: 'idle', ctx } as Agent
}

describe('API Remote Agent resolver races', () => {
  it('maps a listed session without a cwd to session-not-found', async () => {
    const ctx = await createContext()
    const sessionId = sid('missing-after-inspect')
    const meta = { ...header(sessionId), cwd: undefined } as unknown as SessionHeader
    const inspect = vi.fn(() => Promise.resolve({ meta, events: [] }))
    provideSession(ctx, meta, inspect)

    const result = await createApiRemoteAgentResolver(ctx, {})(sessionId)

    expect(result).toMatchObject({ error: { code: 'session-not-found', details: { sessionId } } })
    expect(inspect).not.toHaveBeenCalled()
    await ctx.fiber.dispose()
  })

  it('resumes through a concurrently attached ordinary Session without optional defaults', async () => {
    const ctx = await createContext()
    const sessionId = sid('ordinary-attach-race')
    const meta = header(sessionId)
    provideSession(ctx, meta)
    const resume = vi.spyOn(ctx.agents, 'resume').mockImplementation(async () => {
      const published = ctx.sessions.create(sessionId, { meta: { cwd: '/proj' } })
      return { agent: stubAgent(ctx, published), dispose: () => Promise.resolve() }
    })

    const result = await createApiRemoteAgentResolver(ctx, {})(sessionId)

    expect(result).toMatchObject({ agent: { id: sessionId } })
    expect(resume).toHaveBeenCalledOnce()
    expect(resume.mock.calls[0]?.[0].resumeSessionId).toBe(sessionId)
    expect(typeof resume.mock.calls[0]?.[0].setup).toBe('function')
    await ctx.fiber.dispose()
  })

  it('rejects a listed subagent Session before resume', async () => {
    const ctx = await createContext()
    const sessionId = sid('owned-attach-race')
    const meta = { ...header(sessionId), origin: 'subagent' as const }
    provideSession(ctx, meta)
    const resume = vi.spyOn(ctx.agents, 'resume')

    const result = await createApiRemoteAgentResolver(ctx, {})(sessionId)

    expect(result).toMatchObject({ error: { code: 'agent-busy' } })
    expect(resume).not.toHaveBeenCalled()
    await ctx.fiber.dispose()
  })

  it('reclassifies failed resumes after a live or attached subagent wins publication', async () => {
    for (const winner of ['agent', 'session'] as const) {
      const ctx = await createContext()
      const sessionId = sid(`owned-${winner}-resume-race`)
      const meta = header(sessionId)
      provideSession(ctx, meta)
      vi.spyOn(ctx.agents, 'resume').mockImplementationOnce(async () => {
        const session = ctx.sessions.create(sessionId, { meta: { cwd: '/proj', origin: 'subagent' } })
        if (winner === 'agent') ctx.agents.register(stubAgent(ctx, session))
        throw new Error('session id already published')
      })

      const result = await createApiRemoteAgentResolver(ctx, {})(sessionId)

      expect(result).toMatchObject({ error: { code: 'agent-busy' } })
      await ctx.fiber.dispose()
    }
  })

  it('uses the shared cold-resume policy for the Agent Host Context', async () => {
    const ctx = await createContext()
    const sessionId = sid('context-cold-resume')
    const meta = header(sessionId)
    provideSession(ctx, meta)
    const agentCtx = ctx.extend()
    vi.spyOn(ctx.agents, 'resume').mockImplementation(async () => {
      const published = ctx.sessions.create(sessionId, { meta: { cwd: '/proj' } })
      return { agent: stubAgent(agentCtx, published), dispose: () => Promise.resolve() }
    })
    const defaultProvider = ctx.typert.contexts.getHost('agent')
    createApiRemoteAgentResolver(ctx, {})
    await vi.waitFor(() => { expect(ctx.typert.contexts.getHost('agent')).not.toBe(defaultProvider) })
    const provider = ctx.typert.contexts.getHost('agent')
    if (provider === undefined) throw new Error('Agent Host Context provider was not mounted')

    await expect(provider.resolve(sessionId)).resolves.toBe(agentCtx)
    await ctx.fiber.dispose()
  })

  it('builds setup from the resumed Session without a prior inspection', async () => {
    const ctx = await createContext()
    const sessionId = sid('single-materialization-setup')
    const meta = header(sessionId)
    const inspect = vi.fn(() => Promise.resolve({ meta, events: [] }))
    provideSession(ctx, meta, inspect)
    const seen: number[] = []
    vi.spyOn(ctx.agents, 'resume').mockImplementation(async (
      options: ResumeAgentOptions,
    ): Promise<AgentHandle> => {
      const session = ctx.sessions.create(sessionId, { meta: { cwd: '/proj' } })
      session.append('session/end-seed', {})
      const agent = stubAgent(ctx.extend(), session)
      Object.defineProperty(agent.ctx, 'agent', { value: agent, configurable: true })
      await options.setup?.(agent.ctx)
      return { agent, dispose: () => Promise.resolve() }
    })
    const resolve = createApiRemoteAgentResolver(ctx, {
      setup: ({ events }) => (agentCtx) => {
        expect(agentCtx.agent?.id).toBe(sessionId)
        seen.push(events.length)
      },
    })

    await expect(resolve(sessionId)).resolves.toMatchObject({ agent: { id: sessionId } })
    expect(inspect).not.toHaveBeenCalled()
    expect(seen).toEqual([1])
    await ctx.fiber.dispose()
  })

  it('applies the subagent ownership fence to the Agent Host Context', async () => {
    const ctx = await createContext()
    const sessionId = sid('context-owned-subagent')
    const session = ctx.sessions.create(sessionId, { meta: { cwd: '/proj', origin: 'subagent' } })
    ctx.agents.register(stubAgent(ctx.extend(), session))
    const defaultProvider = ctx.typert.contexts.getHost('agent')
    createApiRemoteAgentResolver(ctx, {})
    await vi.waitFor(() => { expect(ctx.typert.contexts.getHost('agent')).not.toBe(defaultProvider) })
    const provider = ctx.typert.contexts.getHost('agent')
    if (provider === undefined) throw new Error('Agent Host Context provider was not mounted')

    const resolution = provider.resolve(sessionId)
    await expect(resolution).rejects.toBeInstanceOf(TypertLookupFailure)
    await expect(resolution).rejects.toMatchObject({ failure: { code: 'agent-busy' } })
    await ctx.fiber.dispose()
  })
})
