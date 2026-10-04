/** Typed entry for the fixed-time CJK search evidence plugin. */
import type { Context } from '@deepseek-ai/cordis'
export declare const name = 'cjk-evidence'
export declare const inject: readonly ['sessions']
/** Seed fixed-time Session evidence in the scenario composition. */
export declare function apply(ctx: Context): void
