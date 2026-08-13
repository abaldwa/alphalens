/**
 * scripts/check-layers.mjs
 *
 * Enforces the layering inside src/features/backtest-report so the UI stays
 * separable. The point is a future UI refactor: `ui/` should be replaceable
 * wholesale without touching a single rule about returns, and that only stays
 * true if the dependency direction is checked rather than merely intended.
 *
 *   core/  pure domain logic. No React, no router, no query client, no
 *          Tailwind class strings, no DOM. Depends on nothing but core.
 *   data/  fetching and URL/session state. May use react-query and the
 *          router; may import core. Must not import ui.
 *   ui/    React components and Tailwind. May import anything.
 *
 * Run: npm run check:layers  (also part of npm run verify)
 */

import { readdirSync, readFileSync, statSync } from 'node:fs'
import { join, relative } from 'node:path'

const ROOT = 'src/features/backtest-report'

/** What each layer is forbidden from importing. */
const FORBIDDEN_IMPORTS = {
  core: [
    [/^react$|^react\//, 'React'],
    [/^react-dom/, 'react-dom'],
    [/react-router/, 'the router'],
    [/@tanstack\/react-query/, 'react-query'],
    [/@tanstack\/react-table/, 'react-table'],
    [/@\/lib\/ui/, 'the component library'],
    [/\.\.\/ui\/|\.\/ui\//, 'the ui layer'],
    [/\.\.\/data\/|\.\/data\//, 'the data layer'],
  ],
  data: [
    [/@\/lib\/ui/, 'the component library'],
    [/\.\.\/ui\/|\.\/ui\//, 'the ui layer'],
  ],
  ui: [],
}

/** core/ must also stay free of styling, which imports alone do not catch. */
const STYLE_HINTS = [
  [/className/, 'className'],
  [/\b(?:bg|text|border|px|py|flex|grid)-\[?[a-z0-9-]/, 'a Tailwind class'],
]

function walk(dir) {
  const out = []
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry)
    if (statSync(full).isDirectory()) out.push(...walk(full))
    else if (/\.tsx?$/.test(full)) out.push(full)
  }
  return out
}

function importsOf(source) {
  const specs = []
  const re = /(?:from|import)\s+['"]([^'"]+)['"]/g
  let m
  while ((m = re.exec(source)) !== null) specs.push(m[1])
  return specs
}

const violations = []

for (const file of walk(ROOT)) {
  const rel = relative(ROOT, file)
  const layer = rel.split('/')[0]
  if (!(layer in FORBIDDEN_IMPORTS)) continue // selfcheck.ts and friends
  const source = readFileSync(file, 'utf8')

  for (const spec of importsOf(source)) {
    for (const [pattern, label] of FORBIDDEN_IMPORTS[layer]) {
      if (pattern.test(spec)) {
        violations.push(`${file}: ${layer}/ must not import ${label} (${spec})`)
      }
    }
  }

  if (layer === 'core') {
    if (file.endsWith('.tsx')) {
      violations.push(`${file}: core/ must not contain JSX — move the component to ui/`)
    }
    for (const [pattern, label] of STYLE_HINTS) {
      if (pattern.test(source)) {
        violations.push(
          `${file}: core/ must not contain ${label} — styling belongs in ui/`,
        )
      }
    }
  }
}

if (violations.length > 0) {
  console.error('Layer boundary violations:\n')
  for (const v of violations) console.error(`  ${v}`)
  console.error(
    '\nThe UI layer is meant to be replaceable on its own. Keeping rules about\n' +
      'returns out of ui/ and styling out of core/ is what makes that possible.',
  )
  process.exit(1)
}

console.log('Layer boundaries OK: core/ is pure, data/ is UI-free.')
