// Local patch (unwritten-ops hermes-patch-persistence/pinned-file-tree-jump).
export const PINNED_FILE_FOLDERS = [
  { label: 'bugs/specs', path: '/Users/brian/Developer/unwritten-ops/projects' }
] as const

export type PinnedFileFolder = (typeof PINNED_FILE_FOLDERS)[number]

function cleanPath(path: string): string {
  return path.replace(/[\\/]+$/, '')
}

export function isPathInside(root: string, path: string): boolean {
  const cleanRoot = cleanPath(root)
  const cleanTarget = cleanPath(path)

  if (!cleanRoot) {
    return false
  }

  return cleanTarget === cleanRoot || cleanTarget.startsWith(`${cleanRoot}/`)
}

export function visiblePinnedFolders(cwd: string): PinnedFileFolder[] {
  return PINNED_FILE_FOLDERS.filter(pin => isPathInside(cwd, pin.path))
}
