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
      input: {
        // Multi-page app: one HTML entry per dashboard section. Each entry
        // shares the AppShell/UI library in src/lib/ui. Phase 1 only wires
        // up `main` (placeholder home) and `technical` (reference section);
        // later phases add one input per remaining old dashboard/static/
        // section (fundamental, valuation, forensic, ml, momentum,
        // big_investors, ops).
        main: path.resolve(__dirname, 'index.html'),
        'technical-screener': path.resolve(__dirname, 'technical-screener.html'),
        'technical-watchlist': path.resolve(__dirname, 'technical-watchlist.html'),
        'technical-overview': path.resolve(__dirname, 'technical-overview.html'),
        'technical-alerts': path.resolve(__dirname, 'technical-alerts.html'),
        'technical-compare': path.resolve(__dirname, 'technical-compare.html'),
        'technical-chart': path.resolve(__dirname, 'technical-chart.html'),
        'technical-deep_dive': path.resolve(__dirname, 'technical-deep_dive.html'),

        fundamental: path.resolve(__dirname, 'fundamental.html'),
        'fundamental-screener': path.resolve(__dirname, 'fundamental-screener.html'),
        'fundamental-peers': path.resolve(__dirname, 'fundamental-peers.html'),
        'fundamental-sector': path.resolve(__dirname, 'fundamental-sector.html'),
        'fundamental-management': path.resolve(__dirname, 'fundamental-management.html'),
        'fundamental-thesis': path.resolve(__dirname, 'fundamental-thesis.html'),

        valuation: path.resolve(__dirname, 'valuation.html'),
        'valuation-dcf': path.resolve(__dirname, 'valuation-dcf.html'),
        'valuation-relative': path.resolve(__dirname, 'valuation-relative.html'),
        'valuation-batch': path.resolve(__dirname, 'valuation-batch.html'),
        'valuation-accuracy': path.resolve(__dirname, 'valuation-accuracy.html'),

        forensic: path.resolve(__dirname, 'forensic.html'),
        'forensic-benford': path.resolve(__dirname, 'forensic-benford.html'),
        'forensic-cashflow': path.resolve(__dirname, 'forensic-cashflow.html'),
        'forensic-heatmap': path.resolve(__dirname, 'forensic-heatmap.html'),
        'forensic-redflag': path.resolve(__dirname, 'forensic-redflag.html'),
        'forensic-report': path.resolve(__dirname, 'forensic-report.html'),
        'forensic-universe': path.resolve(__dirname, 'forensic-universe.html'),

        ml: path.resolve(__dirname, 'ml.html'),
        'ml-signal': path.resolve(__dirname, 'ml-signal.html'),
        'ml-backtest': path.resolve(__dirname, 'ml-backtest.html'),
        'ml-holdings': path.resolve(__dirname, 'ml-holdings.html'),
        'ml-positions': path.resolve(__dirname, 'ml-positions.html'),
        'ml-multibagger': path.resolve(__dirname, 'ml-multibagger.html'),
        'ml-sector_rotation': path.resolve(__dirname, 'ml-sector_rotation.html'),
        'ml-exit_urgency': path.resolve(__dirname, 'ml-exit_urgency.html'),
        'ml-universe': path.resolve(__dirname, 'ml-universe.html'),
        'ml-watchlist': path.resolve(__dirname, 'ml-watchlist.html'),
        'ml-tools': path.resolve(__dirname, 'ml-tools.html'),

        momentum: path.resolve(__dirname, 'momentum.html'),
        'momentum-portfolio': path.resolve(__dirname, 'momentum-portfolio.html'),
        'momentum-rebalance': path.resolve(__dirname, 'momentum-rebalance.html'),
        'momentum-universe': path.resolve(__dirname, 'momentum-universe.html'),

        big_investors: path.resolve(__dirname, 'big_investors.html'),
        'big_investors-announcements': path.resolve(__dirname, 'big_investors-announcements.html'),
        'big_investors-mf_holdings': path.resolve(__dirname, 'big_investors-mf_holdings.html'),

        ops: path.resolve(__dirname, 'ops.html'),

        macro: path.resolve(__dirname, 'macro.html'),
      },
    },
  },
})
