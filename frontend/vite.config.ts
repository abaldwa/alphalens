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
    rollupOptions: {
      output: {
        // Per-route chunks (see src/app/router.tsx's React.lazy() imports)
        // already keep any one page's JS small; this splits the large
        // third-party libraries that used to all land in one shared `ui`
        // chunk (see FeatureBacklog.md FE2) into their own vendor chunks
        // instead, so a page that doesn't use e.g. TradingView's charting
        // library never has to download it, and the vendor chunks that
        // are shared across pages get cached independently of app code.
        manualChunks(id) {
          if (!id.includes('node_modules')) return undefined
          if (id.includes('lightweight-charts')) return 'vendor-lightweight-charts'
          if (id.includes('recharts') || id.includes('d3-')) return 'vendor-recharts'
          if (id.includes('@radix-ui')) return 'vendor-radix'
          if (id.includes('react-router')) return 'vendor-react-router'
          if (id.includes('@tanstack')) return 'vendor-tanstack'
          if (id.includes('/react/') || id.includes('/react-dom/') || id.includes('/scheduler/')) {
            return 'vendor-react'
          }
          return 'vendor'
        },
      },
    },
  },
})
