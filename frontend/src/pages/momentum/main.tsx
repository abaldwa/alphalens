import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { QueryClientProvider } from '@tanstack/react-query'

import '@/index.css'
import { queryClient } from '@/shared/query'
// The bare /momentum.html "Overview" entry point renders the same
// Universe screen as /momentum-universe.html — the old dashboard's
// momentum app landed on the Universe screen by default (shell.js maps
// "universe" -> index.html as the first/default screen), and
// /api/v1/momentum/universe requires a strategy_id, so a generic
// unparameterized list stub here would be broken. Reuse the real page
// rather than keeping two competing "universe" implementations.
import { MomentumUniversePage } from './universe'

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <MomentumUniversePage />
    </QueryClientProvider>
  </StrictMode>,
)
