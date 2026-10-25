import { describe, expect, it } from 'vitest'

import { isPathInside, PINNED_FILE_FOLDERS, visiblePinnedFolders } from './pinned-folders'

describe('pinned file-tree folders', () => {
  it('keeps the bugs/specs jump targeting the ops projects tree', () => {
    expect(PINNED_FILE_FOLDERS).toEqual([
      { label: 'bugs/specs', path: '/Users/brian/Developer/unwritten-ops/projects' }
    ])
  })

  it('shows the pin when the workspace root contains the target', () => {
    for (const cwd of [
      '/Users/brian/Developer/unwritten-ops',
      '/Users/brian/Developer/unwritten-ops/',
      '/Users/brian/Developer',
      '/Users/brian/Developer/unwritten-ops/projects'
    ]) {
      expect(visiblePinnedFolders(cwd).map(pin => pin.label)).toEqual(['bugs/specs'])
    }
  })

  it('hides the pin when the target is outside the workspace root', () => {
    for (const cwd of [
      '/Users/brian/Developer/unwritten-ops/projects/unwritten-ops',
      '/Users/brian/Developer/unwritten-ops-other',
      '/tmp',
      ''
    ]) {
      expect(visiblePinnedFolders(cwd)).toEqual([])
    }
  })

  it('does not treat a sibling prefix as containment', () => {
    expect(isPathInside('/a/b', '/a/bc')).toBe(false)
    expect(isPathInside('/a/b', '/a/b/c')).toBe(true)
  })
})
