import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import path from 'node:path'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  build: {
    rolldownOptions: {
      output: {
        // [2026-08-13, FE2] Route-level lazy loading in app/router.tsx splits
        // the 65 pages apart; this splits the framework away from OUR code.
        //
        // The point is cache lifetime, not total bytes. React, TanStack and
        // Radix change when a dependency is upgraded — a few times a year —
        // while app code changes daily. Bundled together, every app edit
        // invalidates ~450 kB of unchanged framework for every user. Split,
        // that chunk keeps its hash across deploys and stays in cache.
        //
        // Deliberately coarse: one vendor chunk rather than one per library.
        // Finer splitting trades a smaller invalidation surface for more
        // requests, and these libraries are upgraded together anyway.
        advancedChunks: {
          groups: [
            {
              name: 'vendor-react',
              test: /node_modules[\\/](react|react-dom|scheduler|react-router|react-router-dom)[\\/]/,
            },
            {
              name: 'vendor-data',
              test: /node_modules[\\/]@tanstack[\\/]/,
            },
            {
              name: 'vendor-ui',
              test: /node_modules[\\/](@radix-ui|lucide-react|class-variance-authority|tailwind-merge|clsx)[\\/]/,
            },
          ],
        },
      },
    },
  },
})
