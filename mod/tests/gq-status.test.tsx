import { expect, mock, test } from 'claude-code/testing'

const BAND = {
  plugin: 'gq-status',
  component: 'AbovePrompt',
  props: { hasSurvey: false, isCollapsed: false, isWorking: false, maxRows: 20, bodyColumns: 80,
           scroll: { offset: 0, bodyRows: 20 }, view: {} },
} as const

const T0 = Date.parse('2026-11-20T10:00:00Z')

function job(id: string, state: string, extra: Record<string, unknown> = {}) {
  return { id, name: id, state, reason: null, gpus: null, gpus_requested: [1, 1], nodes: 1, started: null,
           queue_position: null, exit_code: null, ...extra }
}

test('the band lists this project, the status line counts, a finished job toasts', async ($, on) => {
  const clock = mock.clock(on, { now: T0 })
  let ps = [
    job('train', 'running', { gpus: 4, started: '2026-11-20T08:30:00Z' }),
    job('eval', 'queued', { reason: 'Cap', queue_position: 2 }),
  ]
  const seen: { status?: string; toasts: string[]; under?: string } = { toasts: [] }
  on('process.run', (_, e) => {
    const argv = e.argv
    if (argv.includes('ps')) seen.under = argv[argv.indexOf('--under') + 1]
    const stdout = JSON.stringify(argv.includes('ps') ? ps : { gq_gpus_in_use: 4, gq_gpus_cap: 8 })
    return { value: { exitCode: 0, stdout, stderr: '', isStdoutTruncated: false, isStderrTruncated: false } }
  })
  on('ui.status', (_, e) => ((seen.status = e.text), { value: undefined }))
  on('ui.toast', (_, e) => (seen.toasts.push(e.text), { value: undefined }))
  on('session.start', (_, e) => ({ cwd: e.cwd }))
  on('ui.render', ($, e) => {
    const { Text } = $.ui.resolve(e) // stands for the engine's own (empty) band
    return <Text>engine</Text>
  })

  await $.session.start({ cwd: '/home1/me/proj', surface: 'terminal', isInteractive: true })
  await clock.settle()
  expect(seen.under).toBe('/home1/me/proj')
  expect(seen.status).toBe('gq 4/8 GPU · here 1 running, 1 waiting')

  for (const surface of ['terminal', 'desktop', 'vscode', 'mobile'] as const) {
    const ui = await $.ui.mount({ ...BAND, surface })
    expect((await ui.find({ type: 'Text', text: /train/ }))?.text).toBe('gq train: running 1h30m · 4 GPU')
    expect((await ui.find({ type: 'Text', text: /eval/ }))?.text).toBe('gq eval: queued (Cap #2) · 1 GPU')
    await ui.unmount()
  }

  ps = [job('train', 'succeeded', { gpus: 4, exit_code: 0 }), ps[1]!]
  await clock.advance(60_000)
  expect(seen.toasts).toEqual(['gq: train succeeded (exit 0)'])
  expect(seen.status).toBe('gq 4/8 GPU · here 0 running, 1 waiting')

  ps = []
  await clock.advance(60_000)
  const ui = await $.ui.mount({ ...BAND, surface: 'terminal' })
  expect(await ui.find({ type: 'Text', text: /gq/ })).toBeUndefined()
  await ui.unmount()
})

test('without gq (not the notebook) it shows nothing', async ($, on) => {
  const clock = mock.clock(on)
  let status: string | undefined = 'untouched'
  on('process.run', () => ({ value: { exitCode: 127, stdout: '', stderr: 'gq: not found', isStdoutTruncated: false, isStderrTruncated: false } }))
  on('ui.status', (_, e) => ((status = e.text), { value: undefined }))
  on('session.start', (_, e) => ({ cwd: e.cwd }))
  await $.session.start({ cwd: '/Users/me', surface: 'desktop', isInteractive: true })
  await clock.advance(120_000)
  expect(status).toBe('untouched')
})
