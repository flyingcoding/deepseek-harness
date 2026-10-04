/** Fixed-time live Session evidence for the CJK search snapshot. */
import { SessionId, SessionSeq } from '@deepseek-ai/dsh-session'
import { createUserMessage } from '@deepseek-ai/dsh-llm'

export const name = 'cjk-evidence'
export const inject = ['sessions']

/**
 * Seed deterministic search evidence owned by this scenario's plugin fiber.
 * @param {import('@deepseek-ai/cordis').Context} ctx - composition context.
 */
export function apply(ctx) {
  ctx.on('agent/created', ({ agent }) => {
    ctx.sessions.create(SessionId('cjk-evidence'), {
      meta: { createdAt: 0, cwd: agent.session.header.cwd },
      seed: [{
        type: 'user/message', seq: SessionSeq(0), time: 0,
        data: createUserMessage({ content: [{ type: 'text', text: '今天遇到内存溢出的问题' }],
          source: { kind: 'user' } }),
        surfaceOp: 'append',
      }],
    })
  })
}
