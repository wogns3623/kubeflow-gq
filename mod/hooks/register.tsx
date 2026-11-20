import { atom, read, update } from 'claude-code'
import type { EngineInterface, Register } from 'claude-code'

import type { Job } from '../types'

// What `gq ps` reports for this session's project, refreshed every minute.
const jobs = atom({ plugin: 'gq-status', key: 'jobs' } as const, [])

const POLL_MS = 60_000
const MAX_ROWS = 5
const TERMINAL = ['rejected', 'expired', 'succeeded', 'failed', 'timeout', 'lost', 'killed']
// gq lives in the notebook's own install; a session started without the shell rc may lack it on PATH.
const GQ = 'command -v gq >/dev/null 2>&1 || . "${GQ_ROOT:-$HOME/gq}/env.sh" 2>/dev/null; exec gq "$@"'

export function parseJobs(stdout: string): Job[] {
  const raw = JSON.parse(stdout) as Record<string, any>[]
  return raw.map(s => ({
    id: s.id,
    name: s.name || s.id,
    state: s.state,
    reason: s.reason ?? null,
    gpus: typeof s.gpus === 'number' ? s.gpus : (s.gpus_requested?.[1] ?? 0),
    nodes: s.nodes || 1,
    started: s.started ?? null,
    queuePosition: s.queue_position ?? null,
    exitCode: s.exit_code ?? null,
  }))
}

export function isActive(j: Job): boolean {
  return !TERMINAL.includes(j.state)
}

export function duration(ms: number): string {
  const m = Math.max(0, Math.floor(ms / 60_000))
  return m < 60 ? `${m}m` : `${Math.floor(m / 60)}h${String(m % 60).padStart(2, '0')}m`
}

export function row(j: Job, now: number): string {
  const gpu = j.gpus ? ` · ${j.gpus} GPU${j.nodes > 1 ? ` × ${j.nodes} nodes` : ''}` : ''
  if (j.state === 'running' && j.started) {
    return `${j.name}: running ${duration(now - Date.parse(j.started))}${gpu}`
  }
  const why = j.reason ? ` (${j.reason}${j.queuePosition ? ` #${j.queuePosition}` : ''})` : ''
  return `${j.name}: ${j.state}${why}${gpu}`
}

export function statusLine(cap: Record<string, any> | null, mine: Job[]): string | undefined {
  const active = mine.filter(isActive)
  const running = active.filter(j => j.state === 'running').length
  const parts: string[] = []
  if (cap) parts.push(`gq ${cap.gq_gpus_in_use ?? '?'}/${cap.gq_gpus_cap ?? '?'} GPU`)
  if (active.length) parts.push(`here ${running} running, ${active.length - running} waiting`)
  return parts.length ? parts.join(' · ') : undefined
}

// Jobs that were active at the last poll and have ended since: what is worth a toast.
export function ended(before: Job[], after: Job[]): Job[] {
  const was = new Set(before.filter(isActive).map(j => j.id))
  return after.filter(j => was.has(j.id) && !isActive(j))
}

async function gq($: EngineInterface, args: string[]): Promise<string | null> {
  const r = await $.process.run(['sh', '-c', GQ, 'gq', ...args], { timeoutMs: 20_000 })
  return r.exitCode === 0 ? r.stdout : null
}

async function poll($: EngineInterface, cwd: string): Promise<void> {
  const out = await gq($, ['ps', '--json', '--under', cwd])
  if (out === null) return // no gq here (not the notebook), or gq failed: leave the last view
  const next = parseJobs(out)
  const capText = await gq($, ['capacity', '--json'])
  const before = await read($, jobs)
  for (const j of ended(before, next)) {
    $.ui.toast(`gq: ${j.name} ${j.state}${j.exitCode !== null ? ` (exit ${j.exitCode})` : ''}`)
  }
  await update($, jobs, () => next)
  $.ui.status(statusLine(capText ? JSON.parse(capText) : null, next))
}

export const register: Register = on => {
  on('session.start', async ($, e, next) => {
    const started = await next(e)
    // Not awaited: session.start holds the first prompt, and gq on a slow volume can take seconds.
    // A failed poll (gq hung past the timeout, bad output) only skips that minute.
    $.clock.after(0, () => poll($, e.cwd).catch(() => undefined))
    $.clock.every(POLL_MS, () => poll($, e.cwd).catch(() => undefined))
    return started
  })

  on('ui.render', { component: 'AbovePrompt' }, async ($, e, next) => {
    const active = (await read($, jobs)).filter(isActive)
    if (e.props.hasSurvey || active.length === 0) {
      return next(e)
    }
    const { Box, Text } = $.ui.resolve(e)
    const now = await $.clock.now()
    const shown = active.slice(0, MAX_ROWS)
    return (
      <Box flexDirection="column">
        {shown.map(j => (
          <Text dimColor>gq {row(j, now)}</Text>
        ))}
        {active.length > MAX_ROWS ? <Text dimColor>gq +{active.length - MAX_ROWS} more (gq ps)</Text> : null}
      </Box>
    )
  })
}
