import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { QueryClientProvider } from '@tanstack/react-query'

import '@/index.css'
import { queryClient } from '@/shared/query'
import { AccuracyPage } from './accuracy'

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <AccuracyPage />
    </QueryClientProvider>
  </StrictMode>,
)
