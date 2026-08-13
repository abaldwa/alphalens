/**
 * features/backtest-report/ui/ragTheme.ts
 *
 * Tailwind classes for the RAG bands. These live in the UI layer, not beside
 * the classifier in core/matrix.ts: `classifyRag` is a rule about returns and
 * survives any restyling, whereas these strings are this design system's and
 * would be thrown away by a UI refactor.
 *
 * Both maps are literals rather than interpolated. Tailwind extracts class
 * names statically, so a `text-${band}` template compiles to nothing at all
 * and the element silently loses its colour.
 */

import type { RagBand } from '../core/matrix'

/** Cell shading: background wash plus readable foreground. */
export const RAG_CLASSES: Record<RagBand, string> = {
  red: 'bg-red/15 text-red',
  amber: 'bg-amber/15 text-amber',
  green: 'bg-green/15 text-green',
}

/** Text-only variants, for the RAG count column headers. */
export const RAG_TEXT_CLASSES: Record<RagBand, string> = {
  red: 'text-red',
  amber: 'text-amber',
  green: 'text-green',
}
