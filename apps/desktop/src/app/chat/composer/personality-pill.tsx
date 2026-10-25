import { useStore } from '@nanostores/react'
import { useEffect, useState } from 'react'

import { useGatewayRequest } from '@/app/gateway/hooks/use-gateway-request'
import { Button } from '@/components/ui/button'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger
} from '@/components/ui/dropdown-menu'
import { getHermesConfig } from '@/hermes'
import { normalizePersonalityValue, personalityNamesFromConfig } from '@/lib/chat-runtime'
import { Check, ChevronDown } from '@/lib/icons'
import { cn } from '@/lib/utils'
import { notifyError } from '@/store/notifications'
import { $activeSessionId, $currentPersonality, setCurrentPersonality } from '@/store/session'

const PILL = cn(
  'h-(--composer-control-size) max-w-40 shrink-0 gap-1 rounded-md px-2 text-xs font-normal',
  'text-(--ui-text-tertiary) hover:bg-(--chrome-action-hover) hover:text-foreground'
)

/**
 * Composer personality picker — mirrors the model pill. Shows the active
 * overlay (`display.personality`) and opens a dropdown of the configured
 * personalities. Selecting one drives the gateway's `config.set personality`,
 * the same path `/personality <name>` takes (writes config + applies live to
 * the session). Options come straight from `agent.personalities` so the menu
 * only ever offers names the gateway will accept — the shared
 * `$availablePersonalities` list folds in builtins that aren't configured.
 */
export function PersonalityPill({ disabled }: { disabled: boolean }) {
  const personality = useStore($currentPersonality)
  const activeSessionId = useStore($activeSessionId)
  const { requestGateway } = useGatewayRequest()
  const [names, setNames] = useState<string[]>([])
  const [open, setOpen] = useState(false)
  const [busy, setBusy] = useState(false)

  useEffect(() => {
    let live = true
    // Defer into the promise chain: getHermesConfig throws synchronously when
    // the desktop bridge is absent, and that must not escape the effect.
    void Promise.resolve()
      .then(() => getHermesConfig())
      .then(config => {
        if (live) {setNames(personalityNamesFromConfig(config))}
      })
      .catch(() => {})

    return () => {
      live = false
    }
  }, [])

  const current = normalizePersonalityValue(personality)
  const options = ['none', ...names]

  async function select(value: string) {
    const next = normalizePersonalityValue(value)
    setOpen(false)

    if (next === current) {return}

    const prev = personality
    setCurrentPersonality(next)
    setBusy(true)

    try {
      await requestGateway('config.set', {
        key: 'personality',
        session_id: activeSessionId ?? '',
        value: value || 'none'
      })
    } catch (err) {
      setCurrentPersonality(prev)
      notifyError(err, 'Failed to switch personality')
    } finally {
      setBusy(false)
    }
  }

  const title = `Personality: ${current || 'none'}`

  return (
    <DropdownMenu onOpenChange={setOpen} open={open}>
      <DropdownMenuTrigger asChild>
        <Button
          aria-label={title}
          className={PILL}
          disabled={disabled || busy}
          title={title}
          type="button"
          variant="ghost"
        >
          <span className="truncate">{current || 'personality'}</span>
          <ChevronDown className="size-2.5 shrink-0 opacity-50" />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="max-h-80 w-56 overflow-y-auto" side="top" sideOffset={8}>
        {options.map(name => {
          const norm = normalizePersonalityValue(name)
          const selected = norm === current

          return (
            <DropdownMenuItem
              className="gap-2 text-foreground focus:bg-accent [&_svg]:size-4"
              key={name || 'none'}
              onSelect={() => void select(name)}
            >
              <Check className={cn('shrink-0', selected ? 'opacity-100' : 'opacity-0')} />
              <span className="truncate">{norm || 'none'}</span>
            </DropdownMenuItem>
          )
        })}
      </DropdownMenuContent>
    </DropdownMenu>
  )
}
