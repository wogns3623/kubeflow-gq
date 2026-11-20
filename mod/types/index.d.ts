export type Job = {
  id: string
  name: string
  state: string
  reason: string | null
  gpus: number
  nodes: number
  started: string | null
  queuePosition: number | null
  exitCode: number | null
}

declare module 'claude-code' {
  interface PluginState {
    'gq-status': { jobs: Job[] }
  }
}
