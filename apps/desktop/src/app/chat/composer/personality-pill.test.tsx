// Local patch test (unwritten-ops hermes-patch-persistence/composer-personality-picker).
// Guards the picker contract: pill renders LEFT of the model pill, options come from
// configured `agent.personalities` (+ `none`), never the renderer-wide
// `$availablePersonalities`, and selecting fires live `config.set personality`.
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import type { ChatBarState } from '@/app/chat/composer/types'
import type * as HermesModule from '@/hermes'
import { I18nProvider } from '@/i18n'
import { $activeSessionId, $currentPersonality } from '@/store/session'

import { ComposerControls } from './controls'
import { PersonalityPill } from './personality-pill'

const requestGateway = vi.fn(async (..._args: unknown[]) => ({}))

vi.mock('./model-pill', () => ({ ModelPill: () => <span data-testid="model-pill" /> }))
vi.mock('./reasoning-pill', () => ({ ReasoningPill: () => null }))
vi.mock('@/app/gateway/hooks/use-gateway-request', () => ({ useGatewayRequest: () => ({ requestGateway }) }))
vi.mock('@/hermes', async importOriginal => ({
  ...(await importOriginal<typeof HermesModule>()),
  getHermesConfig: vi.fn(async () => ({
    agent: { personalities: { 'ana-tender': { system_prompt: 'x' }, 'ana-sharp': { system_prompt: 'y' } } }
  }))
}))

const state: ChatBarState = {
  model: { canSwitch: false, model: '', provider: '' },
  tools: { enabled: false, label: '' },
  voice: { active: false, enabled: false }
}

function renderControls() {
  return render(
    <I18nProvider configClient={null} initialLocale="en">
      <ComposerControls
        autoSpeak={false}
        busy={false}
        busyAction="stop"
        canSubmit={true}
        conversation={{
          active: false,
          level: 0,
          muted: false,
          onEnd: vi.fn(),
          onStart: vi.fn(),
          onStopTurn: vi.fn(),
          onToggleMute: vi.fn(),
          status: 'idle'
        }}
        disabled={false}
        hasComposerPayload={true}
        onDictate={vi.fn()}
        onQueue={vi.fn()}
        onToggleAutoSpeak={vi.fn()}
        state={state}
        voiceStatus="idle"
      />
    </I18nProvider>
  )
}

beforeEach(() => {
  requestGateway.mockClear()
  $currentPersonality.set('ana-tender')
  $activeSessionId.set('sess-1')
})

afterEach(() => {
  cleanup()
  $currentPersonality.set('')
  $activeSessionId.set(null)
})

describe('PersonalityPill wiring', () => {
  it('renders the personality pill immediately left of the model pill', () => {
    renderControls()

    const pill = screen.getByLabelText('Personality: ana-tender')
    const model = screen.getByTestId('model-pill')

    expect(pill.compareDocumentPosition(model) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
  })

  it('offers only configured personalities plus none, and switches live via config.set', async () => {
    render(<PersonalityPill disabled={false} />)

    const trigger = screen.getByLabelText('Personality: ana-tender')
    fireEvent.pointerDown(trigger, { button: 0, ctrlKey: false, pointerType: 'mouse' })

    await waitFor(() => expect(screen.getByText('ana-sharp')).toBeTruthy())
    const items = screen.getAllByRole('menuitem').map(el => el.textContent)
    expect(items).toEqual(['none', 'ana-tender', 'ana-sharp'])

    fireEvent.click(screen.getByText('ana-sharp'))

    await waitFor(() =>
      expect(requestGateway).toHaveBeenCalledWith('config.set', {
        key: 'personality',
        session_id: 'sess-1',
        value: 'ana-sharp'
      })
    )
    expect($currentPersonality.get()).toBe('ana-sharp')
  })
})
