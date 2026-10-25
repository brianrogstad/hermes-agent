import { useStore } from '@nanostores/react'

import { $backdrop } from '@/store/backdrop'

const assetPath = (path: string) => `${import.meta.env.BASE_URL}${path.replace(/^\/+/, '')}`

export function Backdrop() {
  const on = useStore($backdrop)

  if (!on) {
    return null
  }

  return (
    <div aria-hidden className="pointer-events-none absolute inset-0 z-2 opacity-[0.15] mix-blend-normal">
      <img
        alt=""
        className="h-[160dvh] w-auto min-w-dvw object-cover object-left-top -translate-y-[14dvh]"
        fetchPriority="low"
        src={assetPath('ds-assets/filler-bg0.jpg')}
      />
    </div>
  )
}
