import { QueryClient } from '@tanstack/react-query'

// Shared TanStack Query client — one instance reused across every Vite
// entry (each page's main.tsx imports this rather than constructing its
// own), so caching semantics stay consistent app-wide.
export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 60_000,
      retry: 1,
      refetchOnWindowFocus: false,
    },
  },
})
